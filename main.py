# main.py - Python 기반 프록시 서버 (FastAPI + websockets)
import asyncio
import logging
import time
from typing import Dict, Optional, List
import requests
from fastapi import Depends, FastAPI, Header, WebSocket, WebSocketDisconnect, HTTPException, status
from fastapi.responses import PlainTextResponse
import os
import websockets
from websockets import exceptions as ws_exceptions
import re
import json

# --- 설정 ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("proxy")

app = FastAPI()
# infra-launcher wnth
INFRA_LAUNCHER_URL = os.getenv("INFRA_LAUNCHER_URL", "http://10.0.0.4:8000")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_ANON_KEY = os.getenv("SUPABASE_ANON_KEY")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY")
SESSION_TIMEOUT_SECONDS = 3600  # 1시간

# --- 세션 저장소 (메모리 기반) ---
# 세션 정보에 vmIp와 마지막 활동 시간 저장
# {"session-id": {"vmIp": "10.0.2.5", "last_activity": 1678886400.0}}
SESSIONS: Dict[str, Dict] = {}

# --- 헬퍼 함수 ---


class AuthenticationError(Exception):
    """Raised when Supabase authentication fails."""


def strip_ansi(text: str) -> str:
    """ANSI escape sequences 제거"""
    ansi_escape = re.compile(r'\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])')
    return ansi_escape.sub('', text)


def parse_kubectl_get_pods(output: str) -> Optional[List[Dict]]:
    """kubectl get pods 출력을 파싱하여 구조화된 데이터로 변환"""
    try:
        clean_output = strip_ansi(output)
        lines = [line.strip() for line in clean_output.split('\n')]
        
        # 헤더 찾기
        header_index = -1
        for i, line in enumerate(lines):
            if 'NAME' in line and 'READY' in line and 'STATUS' in line:
                header_index = i
                break
        
        if header_index == -1:
            return None
        
        pods = []
        for i in range(header_index + 1, len(lines)):
            line = lines[i]
            if not line or line.startswith('%') or '$' in line or '~ %' in line:
                break
            
            parts = line.split()
            if len(parts) >= 3:
                pods.append({
                    'name': parts[0] if len(parts) > 0 else '',
                    'ready': parts[1] if len(parts) > 1 else '',
                    'status': parts[2] if len(parts) > 2 else '',
                    'restarts': parts[3] if len(parts) > 3 else '',
                    'age': parts[4] if len(parts) > 4 else '',
                })
        
        return pods if pods else None
    except Exception as e:
        logger.warning("Failed to parse kubectl get pods output: %s", e)
        return None


def parse_kubectl_get_nodes(output: str) -> Optional[List[Dict]]:
    """kubectl get nodes 출력을 파싱하여 구조화된 데이터로 변환"""
    try:
        clean_output = strip_ansi(output)
        lines = [line.strip() for line in clean_output.split('\n')]
        
        # 헤더 찾기
        header_index = -1
        for i, line in enumerate(lines):
            if 'NAME' in line and 'STATUS' in line:
                header_index = i
                break
        
        if header_index == -1:
            return None
        
        nodes = []
        for i in range(header_index + 1, len(lines)):
            line = lines[i]
            if not line or line.startswith('%') or '$' in line or '~ %' in line:
                break
            
            parts = line.split()
            if len(parts) >= 3:
                nodes.append({
                    'name': parts[0] if len(parts) > 0 else '',
                    'status': parts[1] if len(parts) > 1 else '',
                    'roles': parts[2] if len(parts) > 2 else '',
                    'age': parts[3] if len(parts) > 3 else '',
                    'version': parts[4] if len(parts) > 4 else '',
                })
        
        return nodes if nodes else None
    except Exception as e:
        logger.warning("Failed to parse kubectl get nodes output: %s", e)
        return None


async def _call_infra_launcher(method: str, endpoint: str, **kwargs) -> requests.Response:
    url = f"{INFRA_LAUNCHER_URL.rstrip('/')}{endpoint}"
    return await asyncio.to_thread(requests.request, method, url, timeout=10, **kwargs)


def _supabase_rest_headers() -> Dict:
    if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
        return {}

    return {
        "apikey": SUPABASE_SERVICE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
        "Content-Type": "application/json",
    }


def _supabase_rest_url(path: str) -> str:
    return f"{SUPABASE_URL.rstrip('/')}/rest/v1/{path}"


async def _upsert_user_session(user_id: str, session_id: str, status: str = "active"):
    headers = _supabase_rest_headers()
    if not headers:
        logger.debug("Supabase service key not configured; skipping session persistence")
        return

    payload = {
        "uid": user_id,
        "session_id": session_id,
        "status": status,
    }
    headers_with_prefer = {**headers, "Prefer": "resolution=merge-duplicates"}
    params = {"on_conflict": "uid"}

    try:
        await asyncio.to_thread(
            requests.post,
            _supabase_rest_url("proxy_sessions"),
            json=payload,
            params=params,
            headers=headers_with_prefer,
            timeout=10,
        )
    except requests.exceptions.RequestException as exc:
        logger.warning("Failed to upsert Supabase session for %s: %s", user_id, exc)


async def _mark_user_session_inactive(user_id: str):
    headers = _supabase_rest_headers()
    if not headers:
        return

    payload = {
        "session_id": None,
        "status": "inactive",
    }
    headers_with_prefer = {**headers, "Prefer": "return=minimal"}
    params = {"uid": f"eq.{user_id}"}

    try:
        await asyncio.to_thread(
            requests.patch,
            _supabase_rest_url("proxy_sessions"),
            json=payload,
            params=params,
            headers=headers_with_prefer,
            timeout=10,
        )
    except requests.exceptions.RequestException as exc:
        logger.warning("Failed to mark Supabase session inactive for %s: %s", user_id, exc)


async def _fetch_supabase_user(access_token: str) -> Dict:
    if not SUPABASE_URL or not SUPABASE_ANON_KEY:
        raise AuthenticationError("Supabase environment variables are not configured")
    if not access_token:
        raise AuthenticationError("Missing access token")

    headers = {
        "Authorization": f"Bearer {access_token}",
        "apikey": SUPABASE_ANON_KEY,
    }
    url = f"{SUPABASE_URL.rstrip('/')}/auth/v1/user"
    try:
        response = await asyncio.to_thread(requests.get, url, headers=headers, timeout=10)
    except requests.exceptions.RequestException as exc:
        raise AuthenticationError("Failed to reach Supabase") from exc

    if response.status_code != 200:
        raise AuthenticationError("Invalid or expired Supabase token")

    return response.json()


async def _fetch_session_record(session_id: str) -> Optional[Dict]:
    headers = _supabase_rest_headers()
    if not headers or not session_id:
        return None

    params = {"session_id": f"eq.{session_id}"}
    try:
        response = await asyncio.to_thread(
            requests.get,
            _supabase_rest_url("proxy_sessions"),
            headers=headers,
            params=params,
            timeout=10,
        )
        if response.status_code == 200:
            rows = response.json()
            if rows:
                return rows[0]
    except requests.exceptions.RequestException as exc:
        logger.warning("Supabase lookup failed for session %s: %s", session_id, exc)
    return None


async def get_current_user(authorization: str = Header(default=None)) -> Dict:
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing Authorization header")

    token = authorization.split(" ", 1)[1].strip()
    try:
        return await _fetch_supabase_user(token)
    except AuthenticationError as exc:
        logger.warning("Supabase auth failed: %s", exc)
        raise HTTPException(status_code=401, detail="Invalid token") from exc


async def delete_vm(session_id: str):
    """infra-launcher API를 호출하여 VM을 삭제하고 세션 저장소에서 제거합니다."""
    logger.info("Deleting VM for session %s", session_id)
    try:
        response = await _call_infra_launcher("DELETE", f"/api/vm/{session_id}")
        if response.status_code == 200:
            logger.info("VM deletion request successful for session %s", session_id)
        else:
            logger.warning(
                "Failed to delete VM for session %s. Status: %s, Details: %s",
                session_id,
                response.status_code,
                response.text,
            )
    except requests.exceptions.RequestException as e:
        logger.error("Error calling infra-launcher for session %s: %s", session_id, e)
    finally:
        # API 호출 성공 여부와 관계없이 세션 저장소에서 제거
        removed = SESSIONS.pop(session_id, None)
        if removed and removed.get("uid"):
            asyncio.create_task(_mark_user_session_inactive(removed["uid"]))

# --- 백그라운드 작업 ---
async def session_cleanup_task():
    """주기적으로 오래된 세션을 확인하고 삭제합니다."""
    while True:
        await asyncio.sleep(60)  # 1분마다 확인
        now = time.time()
        stale_sessions = [
            sid for sid, data in SESSIONS.items()
            if (now - data.get("last_activity", now)) > SESSION_TIMEOUT_SECONDS
        ]
        if stale_sessions:
            logger.info("Found stale sessions: %s", stale_sessions)
            for session_id in stale_sessions:
                await delete_vm(session_id)

@app.on_event("startup")
async def startup_event():
    """서버 시작 시 백그라운드 작업을 시작합니다."""
    logger.info("Starting session cleanup task in background.")
    asyncio.create_task(session_cleanup_task())

# --- API 엔드포인트 ---
@app.get("/health")
def health():
    return PlainTextResponse("Proxy OK")

@app.post("/register-session")
async def register_session(payload: Dict[str, str]):
    session_id = payload.get("sessionId")
    vm_ip = payload.get("vmIp")
    user_id = payload.get("userId")

    if not session_id or not vm_ip:
        raise HTTPException(status_code=400, detail="Missing sessionId or vmIp")

    existing = SESSIONS.get(session_id, {})
    resolved_user_id = user_id or existing.get("uid")
    if not resolved_user_id:
        record = await _fetch_session_record(session_id)
        if record:
            resolved_user_id = record.get("uid")
    SESSIONS[session_id] = {
        "vmIp": vm_ip,
        "last_activity": time.time(),
        "uid": resolved_user_id,
    }
    if resolved_user_id:
        # user_id가 payload에 존재하면 초기 등록 단계로 간주하고 inactive 상태 유지
        status = "inactive" if user_id else "active"
        asyncio.create_task(
            _upsert_user_session(resolved_user_id, session_id, status=status)
        )
    else:
        logger.warning("No user mapping found for session %s; status unchanged", session_id)
    logger.info(
        "Session registered: %s -> %s (ready=%s)",
        session_id,
        vm_ip,
        "false" if user_id else "true",
    )
    return {"message": "Session registered successfully"}


@app.post("/launch", status_code=status.HTTP_201_CREATED)
async def launch_session(current_user: Dict = Depends(get_current_user)):
    """Launch a new user VM via infra-launcher and return session info."""
    user_id = current_user.get("id")
    request_payload: Optional[Dict[str, str]] = None
    if user_id:
        request_payload = {"userId": user_id}

    try:
        kwargs = {"json": request_payload} if request_payload else {}
        response = await _call_infra_launcher("POST", "/api/launch-vm", **kwargs)
    except requests.exceptions.RequestException as exc:
        logger.error("Failed to reach infra-launcher for launch request: %s", exc)
        raise HTTPException(status_code=502, detail="Infra-launcher is unreachable") from exc

    if response.status_code != 200:
        logger.warning(
            "Infra-launcher launch failed. Status: %s, Details: %s",
            response.status_code,
            response.text,
        )
        try:
            payload = response.json()
        except ValueError:
            payload = {"error": response.text}
        raise HTTPException(status_code=502, detail=payload)

    try:
        data = response.json()
    except ValueError as exc:
        logger.error("Infra-launcher returned non-JSON payload: %s", response.text)
        raise HTTPException(status_code=502, detail="Invalid response from infra-launcher") from exc

    session_id = data.get("session_id")
    vm_ip = data.get("vm_ip")
    if session_id and vm_ip:
        user_id = current_user.get("id")
        SESSIONS[session_id] = {
            "vmIp": vm_ip,
            "last_activity": time.time(),
            "uid": user_id,
        }
        asyncio.create_task(_upsert_user_session(user_id, session_id, status="inactive"))
        logger.info("Session %s launched with VM %s", session_id, vm_ip)
    else:
        logger.warning("Infra-launcher response missing expected fields: %s", data)

    return data


@app.delete("/session/{session_id}")
async def terminate_session(session_id: str, current_user: Dict = Depends(get_current_user)):
    """Delete a user VM via infra-launcher and remove session state."""
    session = SESSIONS.get(session_id)
    if session and session.get("uid") and session["uid"] != current_user.get("id"):
        raise HTTPException(status_code=403, detail="Not authorised for this session")

    try:
        response = await _call_infra_launcher("DELETE", f"/api/vm/{session_id}")
    except requests.exceptions.RequestException as exc:
        logger.error("Failed to reach infra-launcher for delete request: %s", exc)
        raise HTTPException(status_code=502, detail="Infra-launcher is unreachable") from exc

    if response.status_code != 200:
        logger.warning(
            "Infra-launcher delete failed for session %s. Status: %s, Details: %s",
            session_id,
            response.status_code,
            response.text,
        )
        raise HTTPException(status_code=502, detail="Infra-launcher failed to delete the VM")

    removed = SESSIONS.pop(session_id, None)
    if removed and removed.get("uid"):
        asyncio.create_task(_mark_user_session_inactive(removed["uid"]))
    try:
        payload = response.json()
    except ValueError:
        payload = {"status": "success", "message": "Deletion requested"}

    return payload

@app.websocket("/session/{session_id}")
async def session_proxy(websocket: WebSocket, session_id: str):
    await websocket.accept()

    token = websocket.query_params.get("token")
    try:
        supabase_user = await _fetch_supabase_user(token)
    except AuthenticationError as exc:
        logger.warning("Websocket auth failed for session %s: %s", session_id, exc)
        await websocket.close(code=4401)
        return

    session = SESSIONS.get(session_id)
    if not session:
        headers = _supabase_rest_headers()
        if headers:
            params = {"session_id": f"eq.{session_id}"}
            try:
                response = await asyncio.to_thread(
                    requests.get,
                    _supabase_rest_url("proxy_sessions"),
                    headers=headers,
                    params=params,
                    timeout=10,
                )
                if response.status_code == 200:
                    rows = response.json()
                    if rows:
                        row = rows[0]
                        uid = row.get("uid")
                        vm_ip = row.get("vm_ip") or row.get("vmIp")
                        if uid and vm_ip:
                            session = SESSIONS[session_id] = {
                                "vmIp": vm_ip,
                                "last_activity": time.time(),
                                "uid": uid,
                            }
            except requests.exceptions.RequestException as exc:
                logger.warning("Supabase lookup failed for session %s: %s", session_id, exc)

    if not session:
        logger.warning("Session not found: %s", session_id)
        await websocket.close(code=1008)
        return

    session = SESSIONS[session_id]
    owner = session.get("uid")
    if owner and owner != supabase_user.get("id"):
        logger.warning("Websocket access denied for session %s", session_id)
        await websocket.close(code=4403)
        return

    # 활동 시간 갱신
    session["last_activity"] = time.time()
    
    target_ip = session["vmIp"]
    target_uri = f"ws://{target_ip}:8889"
    
    target_ws = None
    try:
        # 대상 VM의 웹소켓 서버에 연결
        target_ws = await websockets.connect(
            target_uri,
            open_timeout=10,
            ping_interval=30,
            ping_timeout=30,
        )
        logger.info("Proxying connection: %s -> %s", session_id, target_uri)
        
        # 연결 성공 후 초기 리소스 정보 자동 가져오기
        async def fetch_initial_resources():
            """연결 후 자동으로 kubectl 명령어를 실행하여 초기 리소스 정보 가져오기"""
            try:
                # 터미널이 준비될 시간 대기
                await asyncio.sleep(1.5)
                
                # kubectl get pods 명령어 전송
                await target_ws.send("kubectl get pods\n")
                logger.info("Sent initial 'kubectl get pods' command for session %s", session_id)
                
                # 명령어 실행 시간 대기
                await asyncio.sleep(2.0)
                
                # kubectl get nodes 명령어 전송
                await target_ws.send("kubectl get nodes\n")
                logger.info("Sent initial 'kubectl get nodes' command for session %s", session_id)
            except Exception as e:
                logger.warning("Failed to send initial kubectl commands for session %s: %s", session_id, e)
        
        # 백그라운드에서 초기 리소스 가져오기 실행
        asyncio.create_task(fetch_initial_resources())

        async def client_to_target():
            """클라이언트 -> 타겟 VM으로 메시지 전달"""
            try:
                while True:
                    msg = await websocket.receive()
                    msg_type = msg.get("type")
                    if msg_type == "websocket.disconnect":
                        break
                    if msg_type != "websocket.receive":
                        continue

                    # 활동 시간 갱신
                    if session_id in SESSIONS:
                        SESSIONS[session_id]["last_activity"] = time.time()

                    if msg.get("bytes") is not None:
                        await target_ws.send(msg["bytes"])
                    elif msg.get("text") is not None:
                        await target_ws.send(msg["text"])
            except WebSocketDisconnect:
                logger.info("Client websocket disconnected for session %s", session_id)

        async def target_to_client():
            """타겟 VM -> 클라이언트로 메시지 전달"""
            buffer = ""  # kubectl 출력 버퍼링을 위한 변수
            last_parse_time = {}  # 각 리소스 타입별 마지막 파싱 시간
            parse_cooldown = 2.0  # 파싱 쿨다운 시간 (초)
            
            try:
                async for msg in target_ws:
                    # 메시지를 문자열로 변환
                    if isinstance(msg, (bytes, bytearray)):
                        msg_text = msg.decode('utf-8', errors='ignore')
                    else:
                        msg_text = str(msg)
                    
                    # 버퍼에 추가
                    buffer += msg_text
                    
                    # 클라이언트에 원본 메시지 전송
                    if isinstance(msg, (bytes, bytearray)):
                        await websocket.send_bytes(msg)
                    else:
                        await websocket.send_text(msg)
                    
                    # kubectl 명령어 출력 감지 및 파싱
                    # 버퍼가 너무 커지면 초기화 (메모리 관리)
                    if len(buffer) > 20000:
                        buffer = buffer[-10000:]  # 최근 10000자만 유지
                    
                    current_time = time.time()
                    
                    # kubectl get pods 출력 감지 및 파싱
                    if ('kubectl get pods' in buffer.lower() or 'kubectl get pod' in buffer.lower()) and 'NAME' in buffer and 'READY' in buffer:
                        # 쿨다운 체크 (너무 자주 파싱하지 않도록)
                        if current_time - last_parse_time.get('pods', 0) > parse_cooldown:
                            # 출력이 완료되었는지 확인 (프롬프트가 나타나거나 일정 시간 경과)
                            # 프롬프트 패턴 감지
                            prompt_patterns = [r'\n\$', r'\n~ %', r'\n%', r'\n\[.*@.*\].*\$']
                            has_prompt = any(re.search(pattern, buffer[-200:]) for pattern in prompt_patterns)
                            
                            # 또는 출력 끝에 빈 줄이 여러 개 있으면 완료로 간주
                            trailing_newlines = len(buffer) - len(buffer.rstrip('\n\r'))
                            output_complete = has_prompt or trailing_newlines >= 2
                            
                            if output_complete:
                                parsed_pods = parse_kubectl_get_pods(buffer)
                                if parsed_pods:
                                    metadata = {
                                        'type': 'k8s-resource',
                                        'resource': 'pods',
                                        'data': parsed_pods
                                    }
                                    try:
                                        # HTML 주석 형태로 메타데이터 전송 (터미널에 표시되지 않음)
                                        await websocket.send_text(f"\n<!--K8S_METADATA:{json.dumps(metadata)}-->\n")
                                        logger.info("Sent parsed pods data for session %s: %d pods", session_id, len(parsed_pods))
                                        last_parse_time['pods'] = current_time
                                        # 파싱 성공 후 버퍼의 해당 부분 제거 (선택적)
                                        # buffer = ""
                                    except Exception as e:
                                        logger.warning("Failed to send parsed pods data: %s", e)
                    
                    # kubectl get nodes 출력 감지 및 파싱
                    elif ('kubectl get nodes' in buffer.lower() or 'kubectl get node' in buffer.lower()) and 'NAME' in buffer and 'STATUS' in buffer:
                        if current_time - last_parse_time.get('nodes', 0) > parse_cooldown:
                            prompt_patterns = [r'\n\$', r'\n~ %', r'\n%', r'\n\[.*@.*\].*\$']
                            has_prompt = any(re.search(pattern, buffer[-200:]) for pattern in prompt_patterns)
                            trailing_newlines = len(buffer) - len(buffer.rstrip('\n\r'))
                            output_complete = has_prompt or trailing_newlines >= 2
                            
                            if output_complete:
                                parsed_nodes = parse_kubectl_get_nodes(buffer)
                                if parsed_nodes:
                                    metadata = {
                                        'type': 'k8s-resource',
                                        'resource': 'nodes',
                                        'data': parsed_nodes
                                    }
                                    try:
                                        await websocket.send_text(f"\n<!--K8S_METADATA:{json.dumps(metadata)}-->\n")
                                        logger.info("Sent parsed nodes data for session %s: %d nodes", session_id, len(parsed_nodes))
                                        last_parse_time['nodes'] = current_time
                                    except Exception as e:
                                        logger.warning("Failed to send parsed nodes data: %s", e)
                    
            except ws_exceptions.ConnectionClosed:
                logger.info("Target websocket closed for session %s", session_id)

        # 두 작업을 동시에 실행
        await asyncio.gather(client_to_target(), target_to_client())

    except (ws_exceptions.InvalidURI, ws_exceptions.InvalidHandshake, OSError) as e:
        logger.error("Websocket setup failed for session %s: %s", session_id, e)
    except Exception as e:
        logger.exception("Connection error for session %s", session_id)
    # finally:
    #     # 연결이 어떤 이유로든 종료되면 항상 실행
    #     logger.info("Closing connection for session: %s", session_id)
    #     if target_ws:
    #         await target_ws.close()
    #     if websocket.client_state != 3: # CLOSED
    #         await websocket.close()
        
    #     # VM 삭제 함수 호출
    #     await delete_vm(session_id)
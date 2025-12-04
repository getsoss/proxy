# main.py - Python 기반 프록시 서버 (FaessiotAPI + websockets)

import asyncio

import logging

import time

from typing import Dict, Optional

import requests

from fastapi import Depends, FastAPI, Header, WebSocket, WebSocketDisconnect, HTTPException, status, Request, Response

from fastapi.responses import PlainTextResponse

import os

import websockets

from websockets import exceptions as ws_exceptions

# --- 설정 ---

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

logger = logging.getLogger("proxy")

app = FastAPI()

# infra-launcher wnth

INFRA_LAUNCHER_URL = os.getenv("INFRA_LAUNCHER_URL", "http://10.0.0.4:8000")

SUPABASE_URL = os.getenv("SUPABASE_URL")

SUPABASE_ANON_KEY = os.getenv("SUPABASE_ANON_KEY")

SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY")

ionION_TIMEOUT_SECONDS = 3600  # 1시간

# --- 세션 저장소 (메모리 기반) ---

# 세션 정보에 vmIp와 마지막 활동 시간 저장

# {" = -id": {"vmIp": "10.0.2.5", "last_activity": 1678886400.0}}

SESSIONS: Dict[str, Dict] = {}

# --- 헬퍼 함수 ---

class AuthenticationError(Exception):

    """Raised when Supabase authentication fails."""

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

     SS SESSIONS.get(session_id)

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

@app.api_route("/session/{session_id}/k8s-api/{path:path}", methods=["GET", "POST", "OPTIONS"])

async def k8s_api_proxy(session_id: str, path: str, request: Request):

    """k8s-api-server로 HTTP 요청을 프록시합니다."""

    # 세션 확인

    session = SESSIONS.get(session_id)

    if not session:

        # Supabase에서 세션 조회 시도

        record = await _fetch_session_record(session_id)

        if record:

            vm_ip = record.get("vm_ip") or record.get("vmIp")

            uid = record.get("uid")

            if vm_ip:

                session = SESSIONS[session_id] = {

                    "vmIp": vm_ip,

                    "last_activity": time.time(),

                    "uid": uid,

                }

    if not session:

        raise HTTPException(status_code=404, detail="Session not found")

    vm_ip = session["vmIp"]

    target_url = f"http://{vm_ip}:8890/{path}"

    # 쿼리 파라미터 포함

    if request.url.query:

        target_url += f"?{request.url.query}"

    # 요청 본문 처리

    body = None

    if request.method == "POST":

        body = await request.body()

    # VM의 k8s-api-server로 프록시

    try:

        response = await asyncio.to_thread(

            requests.request,

            method=request.method,

            url=target_url,

            headers=dict(request.headers),

            data=body,

            timeout=30

        )

        # CORS 헤더 추가

        headers = dict(response.headers)

        headers["Access-Control-Allow-Origin"] = "*"

        headers["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS"

        headers["Access-Control-Allow-Headers"] = "Content-Type"

        return Response(

            content=response.content,

            status_code=response.status_code,

            headers=headers,

            media_type=response.headers.get("Content-Type", "application/json")

        )

    except requests.exceptions.RequestException as e:

        logger.error(f"Failed to proxy k8s-api request: {e}")

        raise HTTPException(status_code=502, detail="Failed to reach k8s-api-server")

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

    
    
    
    IONS.get(session_id)

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

            try:

                async for msg in target_ws:

                    if isinstance(msg, (bytes, bytearray)):

                        await websocket.send_bytes(msg)

                    else:

                        await websocket.send_text(msg)

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

"""Minimal NiFi 2.x REST client (standard library only), used by deploy.py and the integration tests."""
from __future__ import annotations

import json
import ssl
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid

DEFAULT_URL = "https://localhost:8443/nifi-api"


class NiFi:
    def __init__(self, url: str = DEFAULT_URL, username: str = "admin",
                 password: str = "travelradar123", timeout: float = 60):
        self.url = url.rstrip("/")
        self.timeout = timeout
        self.ctx = ssl.create_default_context()
        self.ctx.check_hostname = False          # local single-user NiFi uses a self-signed cert
        self.ctx.verify_mode = ssl.CERT_NONE
        self.client_id = str(uuid.uuid4())
        self.token = self._login(username, password)

    # ---- transport -------------------------------------------------------------------------------
    def _login(self, username: str, password: str, wait_s: float = 180) -> str:
        data = urllib.parse.urlencode({"username": username, "password": password}).encode()
        deadline = time.time() + wait_s
        while True:
            try:
                req = urllib.request.Request(f"{self.url}/access/token", data=data, method="POST")
                with urllib.request.urlopen(req, context=self.ctx, timeout=self.timeout) as r:
                    return r.read().decode()
            except (urllib.error.URLError, ConnectionError) as exc:
                if time.time() > deadline:
                    raise RuntimeError(f"NiFi not reachable at {self.url}: {exc}") from exc
                time.sleep(5)

    def call(self, method: str, path: str, body=None, raw: bytes | None = None,
             content_type: str = "application/json"):
        data = raw if raw is not None else (json.dumps(body).encode() if body is not None else None)
        req = urllib.request.Request(f"{self.url}{path}", data=data, method=method)
        req.add_header("Authorization", f"Bearer {self.token}")
        if data is not None:
            req.add_header("Content-Type", content_type)
        try:
            with urllib.request.urlopen(req, context=self.ctx, timeout=self.timeout) as r:
                text = r.read().decode()
        except urllib.error.HTTPError as exc:
            raise RuntimeError(f"{method} {path} -> {exc.code}: {exc.read().decode()[:600]}") from exc
        if not text:
            return None
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            return text

    def get(self, path: str):
        return self.call("GET", path)

    # ---- flow ------------------------------------------------------------------------------------
    def root_id(self) -> str:
        return self.get("/flow/process-groups/root")["processGroupFlow"]["id"]

    def groups(self, parent: str | None = None) -> dict[str, dict]:
        parent = parent or self.root_id()
        flow = self.get(f"/flow/process-groups/{parent}")["processGroupFlow"]["flow"]
        return {g["component"]["name"]: g for g in flow["processGroups"]}

    def processors(self, group_id: str) -> list[dict]:
        return self.get(f"/process-groups/{group_id}/processors")["processors"]

    def controller_services(self, group_id: str) -> list[dict]:
        return self.get(f"/flow/process-groups/{group_id}/controller-services"
                        "?includeAncestorGroups=false&includeDescendantGroups=false")["controllerServices"]

    def upload_group(self, parent: str, name: str, flow_json: bytes, x: float, y: float) -> dict:
        boundary = uuid.uuid4().hex
        fields = {"groupName": name, "positionX": str(x), "positionY": str(y),
                  "clientId": self.client_id}
        parts = [f"--{boundary}\r\nContent-Disposition: form-data; name=\"{k}\"\r\n\r\n{v}\r\n".encode()
                 for k, v in fields.items()]
        parts.append(f"--{boundary}\r\nContent-Disposition: form-data; name=\"file\"; "
                     f"filename=\"{name}.json\"\r\nContent-Type: application/json\r\n\r\n".encode()
                     + flow_json + b"\r\n")
        parts.append(f"--{boundary}--\r\n".encode())
        return self.call("POST", f"/process-groups/{parent}/process-groups/upload", raw=b"".join(parts),
                         content_type=f"multipart/form-data; boundary={boundary}")

    def download_group(self, group_id: str) -> dict:
        return self.get(f"/process-groups/{group_id}/download?includeReferencedServices=true")

    def delete_group(self, group: dict) -> None:
        gid = group["id"]
        self.set_group_state(gid, "STOPPED")
        time.sleep(3)                                    # let running tasks finish
        self.set_services_state(gid, "DISABLED")
        drop = self.call("POST", f"/process-groups/{gid}/empty-all-connections-requests")["dropRequest"]
        while not drop["finished"]:
            time.sleep(1)
            drop = self.get(f"/process-groups/{gid}/empty-all-connections-requests/{drop['id']}")["dropRequest"]
        self.call("DELETE", f"/process-groups/{gid}/empty-all-connections-requests/{drop['id']}")
        version = self.get(f"/process-groups/{gid}")["revision"]["version"]
        self.call("DELETE", f"/process-groups/{gid}?version={version}&clientId={self.client_id}")

    def set_services_state(self, group_id: str, state: str) -> None:
        self.call("PUT", f"/flow/process-groups/{group_id}/controller-services",
                  {"id": group_id, "state": state})

    def set_group_state(self, group_id: str, state: str) -> None:
        self.call("PUT", f"/flow/process-groups/{group_id}", {"id": group_id, "state": state})

    def bulletins(self, group_id: str | None = None) -> list[dict]:
        q = f"?groupId={group_id}" if group_id else ""
        return self.get(f"/flow/bulletin-board{q}")["bulletinBoard"]["bulletins"]

    def group_status(self, group_id: str) -> dict:
        return self.get(f"/flow/process-groups/{group_id}/status?recursive=true")[
            "processGroupStatus"]["aggregateSnapshot"]

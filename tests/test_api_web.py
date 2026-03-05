from __future__ import annotations

import time

from fastapi.testclient import TestClient
import pytest

import rslogic.api.server as server


@pytest.fixture
def web_client(monkeypatch: pytest.MonkeyPatch) -> TestClient:
    monkeypatch.setattr(server, "_consume_results", lambda: None)
    with TestClient(server.app) as client:
        yield client


def test_ui_index_serves_web_shell(web_client: TestClient) -> None:
    response = web_client.get("/ui")

    assert response.status_code == 200
    assert "RSLOGIC" in response.text
    assert "JOB BUILDER" in response.text


def test_job_builder_import_accepts_workflow_path(web_client: TestClient, tmp_path) -> None:
    workflow_path = tmp_path / "workflow.json"
    workflow_path.write_text(
        '{"steps":[{"kind":"sdk","action":"sdk_project_status","params":{}}]}',
        encoding="utf-8",
    )

    response = web_client.post("/ui/api/job-builder/import", json={"source": str(workflow_path)})

    assert response.status_code == 200
    assert response.json() == {
        "step_count": 1,
        "steps": [{"kind": "sdk", "action": "sdk_project_status", "params": {}}],
    }


def test_upload_directories_lists_child_directories(web_client: TestClient, tmp_path) -> None:
    root = tmp_path / "captures"
    root.mkdir()
    (root / "set-a").mkdir()
    child = root / "set-b"
    child.mkdir()
    (child / "nested").mkdir()

    response = web_client.get("/ui/api/upload/directories", params={"path": str(root)})

    assert response.status_code == 200
    payload = response.json()
    assert payload["path"] == str(root.resolve())
    assert payload["directories"] == [
        {"name": "set-a", "path": str((root / "set-a").resolve()), "has_children": False},
        {"name": "set-b", "path": str(child.resolve()), "has_children": True},
    ]


def test_client_routes_use_bus_payloads(web_client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    class FakeBus:
        def list_active_clients(self) -> list[str]:
            return ["client-a"]

        def get_client_heartbeat(self, client_id: str) -> dict[str, object]:
            assert client_id == "client-a"
            return {"ts": time.time() - 4, "status": "alive", "active_job_id": "job-1"}

        def command_queue_depth(self, client_id: str) -> int:
            assert client_id == "client-a"
            return 2

        def clear_client_queues(self, client_id: str) -> int:
            assert client_id == "client-a"
            return 3

    monkeypatch.setattr(server, "_bus", FakeBus())

    list_response = web_client.get("/ui/api/clients")
    detail_response = web_client.get("/ui/api/clients/client-a")
    clear_response = web_client.post("/ui/api/clients/client-a/clear-queues")

    assert list_response.status_code == 200
    assert detail_response.status_code == 200
    assert clear_response.status_code == 200
    assert list_response.json()["clients"][0]["client_id"] == "client-a"
    assert list_response.json()["clients"][0]["queue_depth"] == 2
    assert list_response.json()["clients"][0]["heartbeat"]["active_job_id"] == "job-1"
    assert detail_response.json()["client_id"] == "client-a"
    assert clear_response.json() == {"client_id": "client-a", "deleted_keys": 3}

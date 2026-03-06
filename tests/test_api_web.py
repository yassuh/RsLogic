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
    assert 'id="app-root"' in response.text
    assert "/static/js/app.js" in response.text


def test_job_builder_metadata_exposes_sdk_action_catalog() -> None:
    payload = server.web_job_builder_metadata()

    assert "sdk_project_add_folder" in payload["actions"]["sdk_steps"]
    assert "sdk_node_status" in payload["actions"]["sdk_steps"]
    assert "file_copy_staging_to_session_imagery" not in payload["actions"]["file_steps"]
    assert payload["actions"]["file_steps"]["file_copy_staging_to_working"]["optional_params"] == ["relative_dir"]


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


def test_image_routes_use_db_payloads(monkeypatch: pytest.MonkeyPatch) -> None:
    class FakeDb:
        def list_image_assets(self, *, require_coordinates: bool = False) -> list[dict[str, object]]:
            assert require_coordinates is True
            return [{"id": "img-1", "latitude": 1.0, "longitude": 2.0, "group_ids": []}]

        def list_image_groups(self) -> list[dict[str, object]]:
            return [{"id": "group-1", "name": "group-1", "image_count": 1}]

        def get_image_group_detail(self, group_id: str) -> dict[str, object] | None:
            assert group_id == "group-1"
            return {"id": "group-1", "name": "group-1", "image_ids": ["img-1"], "image_count": 1}

        def create_image_group(self, *, name: str, description: str | None = None, image_ids: list[str] | None = None) -> dict[str, object]:
            assert name == "group-2"
            assert description == "desc"
            assert image_ids == ["img-1"]
            return {"id": "group-2", "name": name, "image_ids": image_ids or [], "image_count": 1}

        def update_image_group_membership(self, *, group_id: str, image_ids: list[str], mode: str) -> dict[str, object]:
            assert group_id == "group-1"
            assert image_ids == ["img-1"]
            assert mode == "add"
            return {"group_id": group_id, "mode": mode, "image_ids": image_ids, "image_count": 1}

        def delete_image_group(self, group_id: str) -> bool:
            assert group_id == "group-1"
            return True

    monkeypatch.setattr(server, "_db", FakeDb())

    assert server.web_image_assets() == {"assets": [{"id": "img-1", "latitude": 1.0, "longitude": 2.0, "group_ids": []}]}
    assert server.web_image_groups() == {"groups": [{"id": "group-1", "name": "group-1", "image_count": 1}]}
    assert server.web_image_group_detail("group-1")["image_ids"] == ["img-1"]
    assert server.web_create_image_group(server.ImageGroupCreateRequest(name="group-2", description="desc", image_ids=["img-1"]))["id"] == "group-2"
    assert server.web_update_image_group_membership(
        "group-1",
        server.ImageGroupMembershipRequest(mode="add", image_ids=["img-1"]),
    )["mode"] == "add"
    assert server.web_delete_image_group("group-1") == {"group_id": "group-1", "deleted": True}

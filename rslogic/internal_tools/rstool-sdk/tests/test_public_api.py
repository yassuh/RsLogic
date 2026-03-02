import sys
from pathlib import Path


def test_can_import_realityscan_client() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    sys.path.insert(0, str(repo_root / "src"))

    from realityscan_sdk import RealityScanClient  # noqa: F401


def test_client_exposes_project_resource() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    sys.path.insert(0, str(repo_root / "src"))

    from realityscan_sdk import RealityScanClient

    client = RealityScanClient(base_url="https://example.invalid", client_id="x", app_token="y")
    assert hasattr(client, "project")

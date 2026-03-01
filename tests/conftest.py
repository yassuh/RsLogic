from __future__ import annotations

from pathlib import Path
import tempfile
import sys

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from config import AppConfig, ApiConfig, ControlConfig, LabelDbConfig, LogConfig, QueueConfig, RsToolsConfig, S3Config


def build_test_app_config(repo_root: Path | None = None) -> AppConfig:
    """Build a lightweight config object for unit tests.

    This fixture avoids requiring external services and keeps tests isolated.
    """
    root = repo_root or Path(tempfile.gettempdir()) / "rslogic-test-artifacts"
    return AppConfig(
        app_name="RsLogic Test",
        default_group_name="default-group",
        log=LogConfig(level="INFO", format="%(levelname)s:%(message)s"),
        s3=S3Config(
            region="us-east-1",
            bucket_name="unit-test-bucket",
            processed_bucket_name="unit-test-processed",
            scratchpad_prefix="scratchpad",
            endpoint_url="https://example.com",
            multipart_part_size=1024 * 1024 * 16,
            multipart_concurrency=4,
            resume_uploads=True,
            manifest_dir=str(root / "manifests"),
        ),
        queue=QueueConfig(
            worker_count=1,
            poll_interval_seconds=1,
            backend="redis",
            redis_url="redis://127.0.0.1:6379/0",
            redis_queue_key="rslogic:test:queue",
            redis_block_timeout_seconds=1,
            start_local_workers=False,
        ),
        control=ControlConfig(
            command_queue_key="rslogic:test:control:commands",
            result_queue_key="rslogic:test:control:results",
            block_timeout_seconds=1,
            result_ttl_seconds=300,
            request_timeout_seconds=30,
        ),
        rstools=RsToolsConfig(
            executable_path=None,
            working_root=str(root / "jobs"),
            mode="stub",
            sdk_base_url="http://127.0.0.1:8000",
            sdk_client_id=None,
            sdk_app_token="app-token",
            sdk_auth_token="auth-token",
        ),
        label_db=LabelDbConfig(
            migration_root=str(root / "label-db"),
            alembic_ini=str(root / "label-db" / "alembic.ini"),
            database_url="postgresql://user:pass@localhost:5432/test",
        ),
        api=ApiConfig(base_url="http://127.0.0.1:8000"),
    )

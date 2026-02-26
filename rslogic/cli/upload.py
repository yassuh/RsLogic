"""CLI for uploading to the locked waiting S3 bucket used by RsLogic."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
import json
import logging
import sys
import threading
import warnings
from pathlib import Path
from typing import Any, Callable, List, Sequence

from botocore.exceptions import BotoCoreError, ClientError, EndpointConnectionError, NoCredentialsError
from sqlalchemy.exc import SAWarning, SQLAlchemyError

from config import AppConfig, load_config
from rslogic.metadata import PRIMARY_IMAGE_EXTENSIONS, SIDECAR_EXTENSIONS, DroneSidecarMetadataExtractor
from rslogic.services import S3MetadataIngestPreviewService
from rslogic.storage import StorageRepository
from rslogic.storage.s3 import S3ClientProvider
from rslogic.storage.uploader import S3MultipartUploader

logger = logging.getLogger("rslogic.cli.upload")


@dataclass(frozen=True)
class FileCollectionResult:
    media_files: List[Path]
    skipped_files: List[Path]


class UploadCliApp:
    """Bucket-oriented upload CLI with simple subcommands."""

    def __init__(self, config: AppConfig) -> None:
        self._config = config
        self._client = S3ClientProvider(config.s3).get_client()
        self._repository: StorageRepository | None = None
        self._sidecar_extractor = DroneSidecarMetadataExtractor()

    def build_parser(self) -> argparse.ArgumentParser:
        parser = argparse.ArgumentParser(description="RsLogic S3 upload CLI")
        parser.add_argument("--verbose", action="store_true", help="Enable debug logging")
        parser.add_argument(
            "--tui",
            action="store_true",
            help="Force Textual interactive UI (even if TTY detection fails)",
        )
        parser.add_argument(
            "--prompt",
            action="store_true",
            help="Force prompt mode (disable Textual UI)",
        )
        subparsers = parser.add_subparsers(dest="command", required=False)

        upload_parser = subparsers.add_parser("upload", help="Upload file(s) or folder(s) to S3")
        upload_parser.add_argument("paths", nargs="+", help="File or directory paths")
        upload_parser.add_argument(
            "--group",
            dest="group_name",
            default=None,
            help="Optional image group name to include in S3 metadata",
        )
        upload_parser.add_argument(
            "--override-existing",
            action="store_true",
            help="Re-upload and replace objects that already exist in S3",
        )

        groups_parser = subparsers.add_parser("groups", help="Manage image groups from DB")
        groups_subparsers = groups_parser.add_subparsers(dest="groups_command", required=True)
        groups_list_parser = groups_subparsers.add_parser("list", help="List image groups")
        groups_list_parser.add_argument("--limit", type=int, default=100, help="Max groups to return")
        groups_create_parser = groups_subparsers.add_parser("create", help="Create image group")
        groups_create_parser.add_argument("name", help="Group name")
        groups_create_parser.add_argument("--description", default=None, help="Optional description")

        ingest_parser = subparsers.add_parser(
            "ingest",
            help="Read waiting-bucket object metadata and return parsed metadata JSON",
        )
        ingest_parser.add_argument("--prefix", default=None, help="Optional prefix filter")
        ingest_parser.add_argument("--limit", type=int, default=100, help="Max objects to inspect")
        ingest_parser.add_argument(
            "--group",
            dest="group_name",
            default=None,
            help="Optional image group name to validate from DB",
        )

        interactive_parser = subparsers.add_parser("interactive", help="Run interactive upload wizard")
        interactive_parser.add_argument(
            "--tui",
            action="store_true",
            help="Force Textual interactive UI (even if TTY detection fails)",
        )
        interactive_parser.add_argument(
            "--prompt",
            action="store_true",
            help="Force prompt mode (disable Textual UI)",
        )
        interactive_parser.add_argument(
            "--group",
            dest="group_name",
            default=None,
            help="Optional image group name to prefill in interactive mode",
        )
        return parser

    def run(self, argv: Sequence[str] | None = None) -> None:
        parser = self.build_parser()
        args = parser.parse_args(argv)
        logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO)

        try:
            if args.command in {None, "interactive"}:
                self.run_interactive(
                    force_tui=args.tui,
                    force_prompt=args.prompt,
                    initial_group_name=getattr(args, "group_name", None),
                )
                return

            if args.command == "upload":
                self.upload(
                    paths=args.paths,
                    group_name=args.group_name,
                    override_existing=args.override_existing,
                )
                return

            if args.command == "groups":
                self.run_groups(args)
                return

            if args.command == "ingest":
                self.run_ingest_metadata(
                    prefix=args.prefix,
                    limit=args.limit,
                    group_name=args.group_name,
                )
                return

            parser.error(f"unsupported command: {args.command}")
        except FileNotFoundError as exc:
            raise SystemExit(str(exc)) from exc
        except ValueError as exc:
            raise SystemExit(str(exc)) from exc
        except SQLAlchemyError as exc:
            raise SystemExit(f"Database error: {exc}") from exc
        except NoCredentialsError as exc:
            raise SystemExit("Missing S3 credentials. Set S3_ACCESS_KEY and S3_SECRET_KEY.") from exc
        except EndpointConnectionError as exc:
            raise SystemExit(f"Unable to reach S3 endpoint. Check S3_ENDPOINT_URL. Details: {exc}") from exc
        except ClientError as exc:
            error = exc.response.get("Error", {}) if hasattr(exc, "response") else {}
            code = str(error.get("Code", "unknown"))
            message = str(error.get("Message", str(exc)))
            raise SystemExit(f"S3 API error [{code}]: {message}") from exc
        except BotoCoreError as exc:
            raise SystemExit(f"S3 client error: {exc}") from exc

    def _repo(self) -> StorageRepository:
        if self._repository is None:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", SAWarning)
                self._repository = StorageRepository()
        return self._repository

    def run_groups(self, args: argparse.Namespace) -> None:
        if args.groups_command == "list":
            limit = max(args.limit, 1)
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", SAWarning)
                groups = self._repo().list_image_groups(limit=limit)
            if not groups:
                print("No image groups found.")
                return
            for group in groups:
                created_at = group.created_at.isoformat() if getattr(group, "created_at", None) else "-"
                description = getattr(group, "description", None) or ""
                print(f"{group.id}\t{group.name}\t{created_at}\t{description}")
            return

        if args.groups_command == "create":
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", SAWarning)
                group = self._repo().create_image_group(
                    name=args.name,
                    description=args.description,
                )
            print(f"Group ready: id={group.id} name={group.name}")
            return

        raise SystemExit(f"unsupported groups command: {args.groups_command}")

    def run_ingest_metadata(
        self,
        *,
        prefix: str | None,
        limit: int,
        group_name: str | None = None,
    ) -> None:
        if limit < 1:
            raise ValueError("limit must be at least 1")

        group_payload: dict[str, Any] | None = None
        if group_name:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", SAWarning)
                group = self._repo().get_image_group_by_name(group_name)
            if group is None:
                raise SystemExit(f"image group not found: {group_name}")
            group_payload = {
                "id": group.id,
                "name": group.name,
                "description": group.description,
            }

        service = S3MetadataIngestPreviewService(s3_config=self._config.s3)
        items = service.list_metadata(prefix=prefix, limit=limit)
        payload = {
            "bucket": self._config.s3.bucket_name,
            "prefix": prefix,
            "limit": limit,
            "group": group_payload,
            "count": len(items),
            "items": [item.to_dict() for item in items],
        }
        print(json.dumps(payload, indent=2))

    def run_interactive(
        self,
        *,
        force_tui: bool = False,
        force_prompt: bool = False,
        initial_group_name: str | None = None,
    ) -> None:
        if force_tui and force_prompt:
            raise SystemExit("Choose only one interactive mode: --tui or --prompt")

        if force_prompt:
            logger.info("Prompt mode selected via --prompt.")
            self._run_prompt_interactive(initial_group_name=initial_group_name)
            return

        if force_tui or (sys.stdin.isatty() and sys.stdout.isatty()):
            try:
                from rslogic.cli.upload_tui import run_upload_wizard
            except Exception as exc:  # pragma: no cover - fallback path for env issues
                logger.warning("Interactive TUI unavailable, using prompt mode: %s", exc)
            else:
                previous_disable = logging.root.manager.disable
                logging.disable(logging.CRITICAL)
                try:
                    run_upload_wizard(
                        self._config,
                        lambda paths, group_name, override_existing, reporter, progress: self.upload(
                            paths=paths,
                            group_name=group_name,
                            override_existing=override_existing,
                            reporter=reporter,
                            progress=progress,
                        ),
                        initial_group_name=initial_group_name,
                    )
                finally:
                    logging.disable(previous_disable)
                return

        logger.info(
            "TTY/TUI unavailable, using prompt mode (stdin_tty=%s stdout_tty=%s). Use --tui to force Textual.",
            sys.stdin.isatty(),
            sys.stdout.isatty(),
        )
        self._run_prompt_interactive(initial_group_name=initial_group_name)

    def _run_prompt_interactive(self, *, initial_group_name: str | None = None) -> None:
        print("RsLogic Upload Wizard")
        print("Prompt mode (TUI unavailable)")
        print(f"Locked bucket: {self._config.s3.bucket_name}")
        print(f"Using config default prefix: {self._config.s3.scratchpad_prefix}")
        print(f"Using config default concurrency: {max(self._config.s3.multipart_concurrency, 1)}")
        print(f"Using config default part size MB: {max(self._config.s3.multipart_part_size // (1024 * 1024), 5)}")
        print(f"Using config default resume: {self._config.s3.resume_uploads}")
        print("Video files are unsupported and will be ignored.")

        paths = self._prompt_paths()
        group_name = self._prompt_group_name(initial_value=initial_group_name)

        print("\nUpload plan")
        print(f"- files/paths: {', '.join(paths)}")
        print(f"- bucket: {self._config.s3.bucket_name} (locked)")
        print(f"- prefix: {self._config.s3.scratchpad_prefix} (config)")
        print(f"- concurrency: {max(self._config.s3.multipart_concurrency, 1)} (config)")
        print(f"- part size MB: {max(self._config.s3.multipart_part_size // (1024 * 1024), 5)} (config)")
        print(f"- resume: {self._config.s3.resume_uploads} (config)")
        print(f"- group: {group_name or '(none)'}")
        print("- bucket must already exist: true")

        if not self._prompt_bool("Proceed", default=True):
            print("Cancelled.")
            return

        self.upload(paths=paths, group_name=group_name)

    def _prompt_bool(self, prompt: str, *, default: bool) -> bool:
        default_label = "Y/n" if default else "y/N"
        while True:
            try:
                raw = input(f"{prompt} ({default_label}): ").strip().lower()
            except EOFError:
                logger.info("Input stream closed while prompting '%s'; cancelling interactive flow.", prompt)
                return False
            if raw == "":
                return default
            if raw in {"y", "yes"}:
                return True
            if raw in {"n", "no"}:
                return False
            print("Please answer yes or no.")

    def _prompt_paths(self) -> List[str]:
        print("Enter one or more file/folder paths.")
        print("Use commas for multiple entries, or press Enter on empty line when done.")
        selected: List[str] = []
        while True:
            try:
                raw = input("Paths: ").strip()
            except EOFError as exc:
                raise SystemExit("Input stream closed while selecting upload paths") from exc
            if not raw:
                if selected:
                    return selected
                print("At least one path is required.")
                continue
            items = [item.strip() for item in raw.split(",") if item.strip()]
            if not items:
                print("At least one path is required.")
                continue
            selected.extend(items)
            if self._prompt_bool("Add more paths", default=False):
                continue
            return selected

    def _prompt_group_name(self, *, initial_value: str | None = None) -> str | None:
        seed = (initial_value or "").strip()
        prompt = "Image group name (optional)"
        if seed:
            prompt = f"{prompt} [{seed}]"

        try:
            raw = input(f"{prompt}: ").strip()
        except EOFError:
            logger.info("Input stream closed while prompting for optional group name.")
            return seed or None

        if not raw:
            return seed or None
        return raw

    def validate_bucket_exists(self, bucket_name: str) -> None:
        """Validate that the required bucket already exists and is reachable."""
        try:
            self._client.head_bucket(Bucket=bucket_name)
            logger.info("Bucket reachable: %s", bucket_name)
        except ClientError as exc:
            code = str(exc.response.get("Error", {}).get("Code", ""))
            if code in {"404", "NoSuchBucket", "NotFound"}:
                raise SystemExit(
                    f"Required bucket does not exist: {bucket_name}. "
                    "Create it outside RsLogic, then retry."
                ) from exc
            raise

    def collect_files(self, input_paths: Sequence[str]) -> FileCollectionResult:
        candidates: List[Path] = []
        for raw_path in input_paths:
            path = Path(raw_path).expanduser()
            if path.is_dir():
                candidates.extend(
                    sorted(
                        (candidate for candidate in path.rglob("*") if candidate.is_file()),
                        key=lambda item: str(item),
                    )
                )
            elif path.is_file():
                candidates.append(path)
            else:
                raise FileNotFoundError(f"Path is not a file or directory: {path}")

        media_files: List[Path] = []
        skipped_files: List[Path] = []
        seen: set[Path] = set()

        for candidate in candidates:
            resolved = candidate.resolve()
            if resolved in seen:
                continue
            seen.add(resolved)
            suffix = resolved.suffix.lower()
            if suffix in PRIMARY_IMAGE_EXTENSIONS:
                media_files.append(resolved)
            else:
                skipped_files.append(resolved)

        return FileCollectionResult(media_files=media_files, skipped_files=skipped_files)

    def inspect_sidecars(self, media_files: Sequence[Path]) -> tuple[dict[str, int], list[str]]:
        counts: dict[str, int] = {}
        warnings_list: list[str] = []

        for media_path in media_files:
            result = self._sidecar_extractor.extract_for_media(media_path, parse=False)
            for extension in result.present_sidecars:
                counts[extension] = counts.get(extension, 0) + 1
            if result.missing_expected:
                missing = ", ".join(extension.upper() for extension in result.missing_expected)
                warnings_list.append(f"{media_path.name}: missing expected sidecar(s): {missing}")

        return counts, warnings_list

    def upload(
        self,
        *,
        paths: Sequence[str],
        group_name: str | None = None,
        override_existing: bool = False,
        reporter: Callable[[str], None] | None = None,
        progress: Callable[[int, int, int, int], None] | None = None,
    ) -> None:
        emit = reporter or print
        selection = self.collect_files(paths)
        files = selection.media_files
        if not files:
            raise SystemExit("No files found to upload")

        if selection.skipped_files:
            sidecar_skipped = sum(
                1 for file_path in selection.skipped_files if file_path.suffix.lower() in SIDECAR_EXTENSIONS
            )
            unsupported_skipped = len(selection.skipped_files) - sidecar_skipped
            if sidecar_skipped:
                emit(f"Detected {sidecar_skipped} sidecar file(s); they are not uploaded.")
            if unsupported_skipped:
                emit(
                    f"Skipped {unsupported_skipped} unsupported file(s). "
                    "Video files are currently ignored."
                )

        sidecar_counts, sidecar_warnings = self.inspect_sidecars(files)
        if sidecar_counts:
            rendered_counts = ", ".join(
                f"{extension.upper()}={count}" for extension, count in sorted(sidecar_counts.items())
            )
            emit(f"Detected sidecars in selection: {rendered_counts}")
        emit("Sidecar parsing enabled: matching .XMP/.MRK sidecars for images will be parsed before upload.")
        if sidecar_warnings:
            emit(f"WARNING: {len(sidecar_warnings)} file(s) are missing expected sidecars.")
            preview_limit = 20
            for warning in sidecar_warnings[:preview_limit]:
                emit(f"WARNING: {warning}")
            if len(sidecar_warnings) > preview_limit:
                emit(f"WARNING: ... plus {len(sidecar_warnings) - preview_limit} more missing-sidecar warnings.")

        prefix = self._config.s3.scratchpad_prefix
        concurrency = max(self._config.s3.multipart_concurrency, 1)
        part_size_mb = max(self._config.s3.multipart_part_size // (1024 * 1024), 5)
        resume = self._config.s3.resume_uploads
        locked_bucket = self._config.s3.bucket_name
        normalized_group = (group_name or "").strip() or None
        extra_metadata: dict[str, str] | None = None
        if normalized_group:
            extra_metadata = {
                "group_name": normalized_group,
            }
        total_planned_bytes = 0
        for file_path in files:
            try:
                total_planned_bytes += max(file_path.stat().st_size, 0)
            except OSError:
                logger.debug("Unable to stat file size for progress planning path=%s", str(file_path))
        if progress is not None:
            progress(0, len(files), 0, total_planned_bytes)
        emit(f"Uploading {len(files)} file(s) to s3://{locked_bucket}/{prefix}")
        if normalized_group:
            emit(f"Using group metadata: {normalized_group}")
        emit(f"Override existing: {'ON' if override_existing else 'OFF'}")
        self.validate_bucket_exists(locked_bucket)
        emit(f"Bucket {locked_bucket} is reachable")

        uploader = S3MultipartUploader(part_size=part_size_mb * 1024 * 1024)
        uploaded_total = 0
        uploaded_count = 0
        uploaded_bytes = 0
        failed_count = 0
        progress_lock = threading.Lock()

        def _on_bytes_uploaded(delta: int) -> None:
            nonlocal uploaded_bytes
            if delta <= 0 or progress is None:
                return
            with progress_lock:
                uploaded_bytes += delta
                snapshot_count = uploaded_count + failed_count
                snapshot_bytes = uploaded_bytes
            progress(snapshot_count, len(files), snapshot_bytes, total_planned_bytes)

        def _on_uploaded(result) -> None:
            nonlocal uploaded_total, uploaded_count, uploaded_bytes
            with progress_lock:
                uploaded_count += 1
                uploaded_total += result.size
                if result.size > 0 and uploaded_bytes < uploaded_total:
                    uploaded_bytes = uploaded_total
                snapshot_count = uploaded_count + failed_count
                snapshot_bytes = uploaded_bytes
            if progress is not None:
                progress(snapshot_count, len(files), snapshot_bytes, total_planned_bytes)
            if result.skipped_existing:
                emit(
                    f"[{snapshot_count}/{len(files)}] Skipped existing: "
                    f"s3://{result.bucket}/{result.key}"
                )
                return
            emit(f"[{snapshot_count}/{len(files)}] Uploaded: s3://{result.bucket}/{result.key} ({result.size} bytes)")

        def _on_upload_error(path: Path, exc: Exception) -> None:
            nonlocal failed_count
            with progress_lock:
                failed_count += 1
                snapshot_count = uploaded_count + failed_count
                snapshot_bytes = uploaded_bytes
            if progress is not None:
                progress(snapshot_count, len(files), snapshot_bytes, total_planned_bytes)
            emit(f"[{snapshot_count}/{len(files)}] FAILED: {path} -> {exc}")

        results = uploader.upload_many(
            local_paths=files,
            bucket=locked_bucket,
            prefix=prefix,
            resume=resume,
            override_existing=override_existing,
            concurrency=concurrency,
            extra_metadata=extra_metadata,
            progress_callback=_on_uploaded,
            bytes_progress_callback=_on_bytes_uploaded,
            error_callback=_on_upload_error,
        )
        if progress is not None:
            with progress_lock:
                if uploaded_bytes < uploaded_total:
                    uploaded_bytes = uploaded_total
                final_uploaded_bytes = uploaded_bytes
                processed_count = uploaded_count + failed_count
            progress(processed_count, len(files), final_uploaded_bytes, total_planned_bytes)
        skipped_existing = sum(1 for result in results if result.skipped_existing)
        uploaded_count_total = len(results) - skipped_existing
        emit(
            f"Completed {len(results)} file(s): uploaded {uploaded_count_total}, "
            f"skipped existing {skipped_existing}, failed {failed_count}, bytes uploaded {uploaded_total}"
        )


def main(argv: List[str] | None = None) -> None:
    app = UploadCliApp(load_config())
    app.run(argv)


if __name__ == "__main__":
    main()

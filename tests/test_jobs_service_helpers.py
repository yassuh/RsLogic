from __future__ import annotations

from datetime import datetime

from rslogic.jobs.service import (
    _as_datetime,
    _as_optional_bool,
    _as_optional_float,
    _as_optional_int,
    _as_optional_str,
    _decode_filter_dict,
    _encode_filter_dict,
    ImageFilter,
)


def test_encode_filter_dict_roundtrip_includes_non_none_and_datetime_iso():
    start = datetime(2026, 2, 27, 21, 0, 0)
    filters = ImageFilter(
        group_name="group-a",
        drone_type="drone",
        start_time=start,
        max_images=12,
        sdk_include_subdirs=True,
        sdk_detector_sensitivity="Medium",
        stage_only=True,
    )

    encoded = _encode_filter_dict(filters)
    restored = _decode_filter_dict(encoded)

    assert encoded["group_name"] == "group-a"
    assert encoded["start_time"] == start.isoformat()
    assert encoded["max_images"] == 12
    assert restored.group_name == "group-a"
    assert restored.start_time == start
    assert restored.max_images == 12
    assert restored.sdk_include_subdirs is True
    assert restored.sdk_detector_sensitivity == "Medium"
    assert restored.stage_only is True


def test_decode_filter_dict_defaults_fallback_for_bad_values():
    decoded = _decode_filter_dict({
        "group_name": "",
        "start_time": "bad-time",
        "min_latitude": "abc",
        "sdk_run_align": "",
        "sdk_task_timeout_seconds": "not-an-int",
        "stage_only": "on",
    })

    assert decoded.group_name is None
    assert decoded.start_time is None
    assert decoded.min_latitude is None
    assert decoded.sdk_run_align is True
    assert decoded.stage_only is True
    assert decoded.sdk_task_timeout_seconds == 7200


def test_decode_filter_dict_with_none_payload_returns_defaults():
    decoded = _decode_filter_dict(None)
    assert decoded == ImageFilter()


def test_as_optional_casting_helpers():
    assert _as_optional_str(None) is None
    assert _as_optional_str("  ") is None
    assert _as_optional_str("value") == "value"

    assert _as_optional_float("1.5") == 1.5
    assert _as_optional_float("n/a") is None
    assert _as_optional_int("7") == 7
    assert _as_optional_int("n/a") is None

    assert _as_optional_bool(None, default=True) is True
    assert _as_optional_bool("false", default=True) is False
    assert _as_optional_bool("YES", default=False) is True


def test_as_datetime_parses_iso_and_returns_none_on_invalid():
    parsed = _as_datetime("2026-02-27T21:00:00Z")
    assert parsed is not None
    assert parsed.year == 2026
    assert _as_datetime("") is None
    assert _as_datetime("not-a-time") is None

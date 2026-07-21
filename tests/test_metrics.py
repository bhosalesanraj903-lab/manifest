from pipelines import metrics

SUMMARY = {
    "events_read": 899, "events_processed": 876, "duplicates": 12,
    "quarantined": {"total": 11, "unmapped_ref": 5, "unknown_status": 6, "unparseable_time": 0},
    "exceptions": 38, "exceptions_by_type": {"LATE_DEPARTURE": 20, "MISSED_MILESTONE": 18}, "runtime_s": 0.01,
}


def test_exposition_format():
    text = metrics.exposition(SUMMARY)
    assert "manifest_events_processed 876" in text
    assert 'manifest_quarantined{reason="unmapped_ref"} 5' in text
    assert text.endswith("\n")


def test_push_failure_is_swallowed(monkeypatch):
    def boom(*a, **k):
        raise metrics.requests.ConnectionError("no gateway")
    monkeypatch.setattr(metrics.requests, "put", boom)
    assert metrics.push_run_summary(SUMMARY) is False  # never raises

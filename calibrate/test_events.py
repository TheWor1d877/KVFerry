"""三类事件字段完整性的单测（D35）。"""

from __future__ import annotations

import json

import events


def _records(capsys):
    captured = capsys.readouterr()
    assert captured.err == ""
    return [json.loads(line) for line in captured.out.splitlines()]


def test_expire_fields(capsys):
    events.emit_expire("probe", "t1", "dram", 0.01, 0.02, -25.0)
    (record,) = _records(capsys)
    assert record["event"] == "expire"
    assert record["component"] == events.COMPONENT
    assert isinstance(record["ts_ms"], int)
    assert record["source"] == "probe"
    assert record["table"] == "t1"
    assert record["target"] == "dram"
    assert record["expected"] == 0.01
    assert record["observed"] == 0.02
    assert record["deviation_pct"] == 25.0


def test_recalibrate_phase_and_result(capsys):
    events.emit_recalibrate("passive", "t2", "4096", "start")
    events.emit_recalibrate("passive", "t2", "4096", "end", "success")
    events.emit_recalibrate("probe", "t1", "nvme-direct", "end", "fail")
    records = _records(capsys)
    assert [record["phase"] for record in records] == ["start", "end", "end"]
    assert "result" not in records[0]
    assert records[1]["result"] == "success"
    assert records[2]["result"] == "fail"
    for record in records:
        assert record["event"] == "recalibrate"
        assert record["component"] == events.COMPONENT
        assert isinstance(record["ts_ms"], int)
        assert record["source"] in ("probe", "passive")
        assert record["table"] in ("t1", "t2")


def test_degrade_phase(capsys):
    events.emit_degrade("probe", "t1", "dram", "start")
    events.emit_degrade("probe", "t1", "dram", "end")
    records = _records(capsys)
    assert [record["phase"] for record in records] == ["start", "end"]
    for record in records:
        assert record["event"] == "degrade"
        assert record["component"] == events.COMPONENT
        assert isinstance(record["ts_ms"], int)
        assert record["source"] == "probe"
        assert record["table"] == "t1"


def test_one_json_object_per_line(capsys):
    events.emit_expire("probe", "t1", "dram", 1.0, 2.0, 100.0)
    events.emit_recalibrate("probe", "t1", "dram", "start")
    events.emit_degrade("passive", "t2", "4096", "start")
    captured = capsys.readouterr()
    lines = captured.out.splitlines()
    assert len(lines) == 3
    for line in lines:
        payload = json.loads(line)
        assert isinstance(payload, dict)
    assert captured.err == ""

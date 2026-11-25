from pathlib import Path

from lakefreight import cli

ROOT = Path(__file__).resolve().parents[1]


def test_help_and_unknown_command(capsys):
    assert cli.main([]) == 0
    assert "plan" in capsys.readouterr().out
    assert cli.main(["bogus"]) == 2
    assert "unknown command" in capsys.readouterr().err


def test_plan_writes_work_items(tmp_path):
    code = cli.main(
        [
            "plan",
            "--metadata",
            str(ROOT / "examples" / "metadata_sample.csv"),
            "--out",
            str(tmp_path),
        ]
    )
    assert code == 0
    assert (tmp_path / "work_items.csv").exists()

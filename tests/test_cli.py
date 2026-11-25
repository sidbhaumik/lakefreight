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


def test_config_uses_local_file_then_packaged_defaults(tmp_path, monkeypatch, capsys):
    from lakefreight.config import load_settings

    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("LAKEFREIGHT_CONFIG", raising=False)
    assert cli.main(["config"]) == 0
    assert "built-in defaults" in capsys.readouterr().out
    assert load_settings().full_threshold_bytes == 107374182400

    assert cli.main(["config", "--write"]) == 0
    local = tmp_path / "lakefreight.toml"
    local.write_text(local.read_text().replace("107374182400", "5"))
    assert load_settings().full_threshold_bytes == 5
    assert cli.main(["config", "--write"]) == 1  # never overwrites

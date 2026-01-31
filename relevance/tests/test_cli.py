from pathlib import Path

from typer.testing import CliRunner

from relevance.cli.main import app


def test_ingest_requires_agency_when_not_all(tmp_path):
    runner = CliRunner()
    db_path = tmp_path / "test.sqlite"
    result = runner.invoke(
        app,
        ["ingest", "--all", "false"],
        env={"RELEVANCE_DATABASE_URL": f"sqlite:///{db_path}"},
    )
    assert result.exit_code != 0
    assert "agency is required" in result.stdout

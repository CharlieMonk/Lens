from pathlib import Path

from typer.testing import CliRunner

from relevance.cli_main import app


def test_ingest_requires_agency_when_not_all(tmp_path):
    runner = CliRunner()
    db_path = tmp_path / "test.sqlite"
    result = runner.invoke(
        app,
        ["ingest", "--no-all"],
        env={"RELEVANCE_DATABASE_URL": f"sqlite:///{db_path}"},
    )
    assert result.exit_code != 0
    output = result.stdout + result.stderr
    assert "agency is required" in output

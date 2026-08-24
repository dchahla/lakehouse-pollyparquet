"""Run the dbt ELT (snapshot + run) via dbt's Python API.

The pip-installed `dbt` console script isn't on PATH, but the package imports fine, so we drive it
through dbtRunner instead of the CLI. Same two steps as `cd dbt && dbt snapshot && dbt run`: build
the SCD2 snapshot into silver, then the gold marts. --project-dir / --profiles-dir make it runnable
from the repo root (profiles.yml lives in dbt/).
"""
from pathlib import Path

from dbt.cli.main import dbtRunner

PROJECT = Path(__file__).resolve().parent.parent / "dbt"


def main() -> None:
    runner = dbtRunner()
    for command in (["snapshot"], ["run"]):
        result = runner.invoke([*command,
                                "--project-dir", str(PROJECT),
                                "--profiles-dir", str(PROJECT)])
        if not result.success:                       # non-zero exit so `make` stops on failure
            raise SystemExit(f"dbt {command[0]} failed: {result.exception}")


if __name__ == "__main__":
    main()

# ==================================================================================================
# Glossary
#   dbtRunner            dbt-core's programmatic entry point; .invoke(args) runs a command in-process.
#   dbtRunnerResult      What invoke() returns; .success is the pass/fail flag, .exception the error.
#   --project-dir        Folder holding dbt_project.yml (the dbt project to run).
#   --profiles-dir       Folder holding profiles.yml (the Trino connection); kept in-repo under dbt/.
#   snapshot vs run      snapshot builds/maintains SCD2 history; run materializes the models (marts).
#   raise SystemExit     Exit non-zero on failure so the Makefile target reports an error.
#   Why not the CLI?     pip3 installed dbt's script outside PATH; importing the package sidesteps that.
# ==================================================================================================

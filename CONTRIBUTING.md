# Contributing to lakefreight

Thanks for your interest. Bug reports, documentation fixes and new features are all welcome.

## Set up

```bash
git clone https://github.com/sidbhaumik/lakefreight.git
```

```bash
cd lakefreight && python3 -m venv .venv && .venv/bin/pip install -e ".[dev]"
```

You need Python 3.11 or later. No Snowflake or AWS account is needed to develop or test the planning and SQL generation code.

## Before you open a pull request

Run the same checks as CI:

```bash
.venv/bin/ruff check src tests
```

```bash
.venv/bin/ruff format --check src tests
```

```bash
.venv/bin/python -m pytest -q
```

- Add or update tests for any change in behaviour. The SQL renderer and chunk planner are the core of the project; changes there need tests that show the generated SQL.
- Add a line under `[Unreleased]` in `CHANGELOG.md`.
- Keep pull requests to one change each.

## Design rules

These are deliberate. Please don't change them without opening an issue first; [docs/design-decisions.md](docs/design-decisions.md) explains each one.

- Sensitivity routing fails closed: missing classification is treated as sensitive.
- Chunk predicates must cover every value of the chunk column, including NULL.
- Joins that replace values use `LEFT JOIN`, and reconciliation counts come from the un-joined source.
- A manifest is written only after reconciliation passes.

## Releasing

Maintainers only. Releases are published to PyPI by GitHub Actions using trusted publishing.

1. Move the `[Unreleased]` entries in `CHANGELOG.md` under a new version heading with today's date, and update the comparison links at the bottom.
2. Set the same version in `pyproject.toml`.
3. Merge to `main` and wait for CI to pass.
4. Optional: run the Release workflow by hand to publish to TestPyPI and check the result.
5. Create a GitHub release with the tag `v<version>`. Publishing the release uploads the package to PyPI. The workflow refuses a tag that doesn't match the version in `pyproject.toml`.

## Reporting a bug

Open an issue with the command you ran, a minimal metadata CSV that reproduces the problem, what you expected, and what happened. Remove any real table names or data first.

## Security

Do not report security problems in a public issue. Use GitHub's private vulnerability reporting on this repository instead.

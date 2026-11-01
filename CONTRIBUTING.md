# Contributing to pgdispatch

Thank you for your interest in improving pgdispatch! Bug reports, documentation
fixes and pull requests are welcome.

## Development setup

You need [uv](https://docs.astral.sh/uv/), Docker and Python 3.10+.

```bash
git clone https://github.com/AmirSherov/pgdispatch && cd pgdispatch
uv sync                                   # creates .venv with all development dependencies
docker run -d --name pgdispatch-test -e POSTGRES_PASSWORD=postgres -p 55432:5432 postgres:17
export PGDISPATCH_TEST_DSN=postgresql://postgres:postgres@localhost:55432/postgres
```

## Checks

Each pull request must pass the same checks as CI:

```bash
uv run ruff check .
uv run ruff format --check .
uv run mypy
uv run pytest                     # unit, integration, Django and chaos tests
```

- `tests/unit`: pure logic. No database needed.
- `tests/integration`: real PostgreSQL and a local HTTP receiver. Each test runs in its own schema.
- `tests/django`: the Django app and its facade.
- `tests/chaos`: real worker processes killed with `SIGKILL`. Marked `slow`; skip them with `-m "not slow"`.

Integration tests are skipped when `PGDISPATCH_TEST_DSN` is not set.

## Guidelines

- **Tests prove behaviour.** Every fix comes with a test that fails without it.
  Changes that touch delivery guarantees need an integration or chaos test.
- **Keep the core small.** New runtime dependencies need a strong reason.
  Framework-specific code goes into extras (`pgdispatch.django`, `pgdispatch.fastapi`, …).
- **Write SQL once.** Database operations are generators in `pgdispatch/core`
  that run on every backend. Bind every parameter with `CAST(:name AS type)`.
- **Schema changes** go into a new numbered file in `src/pgdispatch/migrations`.
  Never edit a migration that has been released. Add a matching Django
  migration that calls `migrate_schema(..., target=N)`.
- Keep the public API typed, documented and backwards compatible within a
  minor version. Note user-facing changes in `CHANGELOG.md`.

## Releasing (maintainers)

1. Update `src/pgdispatch/_version.py` and move the `Unreleased` section of `CHANGELOG.md` under the new version.
2. Commit, then tag `vX.Y.Z` and push the tag. The release workflow publishes to PyPI through Trusted Publishing and creates the GitHub release.

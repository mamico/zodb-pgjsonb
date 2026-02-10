# Contributing to zodb-pgjsonb

## Development Setup

Requires Python 3.12+ and [uv](https://docs.astral.sh/uv/).

```bash
# Clone and install
git clone https://github.com/bluedynamics/zodb-pgjsonb.git
cd zodb-pgjsonb
uv sync --all-extras

# Start a test database
docker run -d --name zodb-pgjsonb-dev \
  -e POSTGRES_USER=zodb -e POSTGRES_PASSWORD=zodb -e POSTGRES_DB=zodb \
  -p 5433:5432 postgres:17
```

## Running Tests

```bash
pytest
```

Tests expect PostgreSQL on `localhost:5433` (user/password/db: `zodb`/`zodb`/`zodb_test`).

## Code Quality

```bash
ruff check .
ruff format .
```

Pre-commit hooks are configured — install them with:

```bash
pre-commit install
```

## Submitting Changes

1. Fork the repository and create a feature branch.
2. Write tests for new functionality.
3. Ensure all tests pass and code quality checks are clean.
4. Open a pull request against `main`.

## License

By contributing, you agree that your contributions will be licensed under the
[Zope Public License 2.1](LICENSE.txt).

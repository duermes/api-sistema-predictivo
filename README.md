### Setup just

MacOS:

```shell
brew install just
```

Debian/Ubuntu:

```shell
apt install just
```

### Setup poetry

```shell
pip install poetry
```

### Setup Postgres (16.3)

```shell
just up
```

### Copy the environment file and install dependencies

1. `cp .env.example .env`
2. `poetry install`

### Run the uvicorn server

With default settings:

```shell
just run
```

With extra configs (e.g. logging file)

```shell
just run --log-config logging.ini
```

### Linters

Format the code with `ruff --fix` and `ruff format`

```shell
just lint
```

### Migrations

- Create an automatic migration from changes in `src/database.py`

```shell
just mm *migration_name*
```

- Run migrations

```shell
just migrate
```

- Downgrade migrations

```shell
just downgrade downgrade -1  # or -2 or base or hash of the migration
```

## Deployment

Example of running the app with docker compose:

```shell
docker compose -f docker-compose.prod.yml up -d --build
```

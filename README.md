# PROJECT-5b4a9576

## Adjustments

- Add `FromRow` to generated models for `sqlx` compatibility.

## Commands

### Workspaces

```bash
cargo new crates/libs/lib-core --lib --vcs none
cargo new crates/libs/lib-data --lib --vcs none
cargo new crates/libs/lib-progress --lib --vcs none
```

### Setup

```bash
sea-orm-cli generate entity -o src/database

sea-orm-cli generate entity -o crates/libs/lib-data/src/database

sea-orm-cli generate entity -s sales -o crates/libs/lib-data/src/database-sales
```

### Docker

```bash
docker-compose exec database psql -U postgres -d MyDatabase
docker-compose exec database psql -U postgres -d salesdb
```

## Note

- 4:58:23

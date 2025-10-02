# PROJECT-5b4a9576

## Adjustments

- Add `FromRow` to generated models for `sqlx` compatibility.

## Commands

### Workspaces

```bash
cargo new crates/lib/lib-core --lib
cargo new crates/lib/lib-heavy --lib
```

### Setup

```bash
sea-orm-cli generate entity -o src/database
```

### Docker

```bash
docker-compose exec database psql -U postgres -d MyDatabase
```

## Note

- Last timestamp: 43:38

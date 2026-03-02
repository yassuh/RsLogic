# label-db

## Quick setup
1. Install project dependencies:
```powershell
pip install -e .\studio-db
```
2. Start Postgres/PostGIS:
```powershell
cd studio-db
docker compose up -d postgis
```
3. Set DB env vars in either `studio-db/.env` or repo root `.env`.

Required minimum:
```env
POSTGRES_PASSWORD=your_password
```
Optional (defaults shown):

```env
POSTGRES_HOST=/var/run/postgresql
POSTGRES_PORT=5432
POSTGRES_DB=studio_db
POSTGRES_USER=studio
```
```env
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=studio_db
POSTGRES_USER=studio
```

## Apply migrations
From `studio-db/`:
```powershell
# Apply all pending migrations to the latest revision (changes DB schema).
alembic -c alembic.ini upgrade head

# Show the revision currently applied in this database (read-only).
alembic -c alembic.ini current
```

## Edit models and create migrations
1. Update `studio-db/models.py`.
2. Generate a revision:
```powershell
alembic -c alembic.ini revision --autogenerate -m "describe change"
```
3. Review the new file in `studio-db/migrations/versions/` and adjust if needed.
4. Apply it:
```powershell
alembic -c alembic.ini upgrade head
```

## Useful checks
```powershell
alembic -c alembic.ini heads
alembic -c alembic.ini history --verbose
```

## Local-only: temporarily bypass FK/trigger constraints
Use only for local debugging/seeding, and always restore it in the same session.
```sql
SET session_replication_role = replica;
-- run local data fixes/seeds here
SET session_replication_role = origin;
```
Notes:
1. Requires a superuser connection.
2. This does not disable all constraint types (for example, `NOT NULL` still applies).

# Internal Tooling

This repository keeps internal company tooling under `internal_tools/`.

Current layout:

- `internal_tools/label-db/studio-db`: Alembic migrations and shared SQLAlchemy models.
- `internal_tools/rstool-sdk`: local RealityScan SDK checkout.

If you want to refresh these packages from origin, you can clone with:

```bash
rm -rf internal_tools/label-db internal_tools/rstool-sdk
git clone -b 2.0 https://github.com/yassuh/label-db.git internal_tools/label-db
git clone https://github.com/bossdown123/RsTool.git internal_tools/rstool-sdk
```

The project resolves label-db through the installed package so runtime does not depend
on a checked-in path override. Migrations are loaded from the installed `studio-db`
package location, and the SDK module still loads from `internal_tools/rstool-sdk/src`.

If you prefer submodules, replace these directories with git submodules to source updates directly.

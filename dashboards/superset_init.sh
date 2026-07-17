#!/bin/bash
# superset_init.sh
# ─────────────────
# One-shot initialization script for the Superset container.
# This is referenced in the superset-init service command in docker-compose.
# It creates the admin user and registers the finpulse Postgres connection.
#
# Usage: runs automatically inside the superset-init container.
#        Safe to re-run — the 'create-admin' and add-database commands are
#        idempotent (they skip if already configured).

set -e

echo "==> Upgrading Superset metadata DB..."
superset db upgrade

echo "==> Creating admin user..."
superset fab create-admin \
  --username admin \
  --firstname FinPulse \
  --lastname Admin \
  --email admin@finpulse.io \
  --password admin 2>/dev/null || true

echo "==> Initializing Superset roles and permissions..."
superset init

echo "==> Registering FinPulse Postgres connection..."
python - <<'PYEOF'
from superset import create_app
from superset.models.core import Database
from superset.extensions import db

app = create_app()
with app.app_context():
    existing = db.session.query(Database).filter_by(
        database_name="FinPulse Postgres"
    ).first()

    if not existing:
        new_db = Database(
            database_name="FinPulse Postgres",
            sqlalchemy_uri="postgresql+psycopg2://airflow:airflow@postgres/finpulse",
            expose_in_sqllab=True,
        )
        db.session.add(new_db)
        db.session.commit()
        print("Database connection 'FinPulse Postgres' registered.")
    else:
        print("Database connection already exists — skipping.")
PYEOF

echo "==> Superset initialization complete!"
echo "    Open http://localhost:8088 and login with admin / admin"

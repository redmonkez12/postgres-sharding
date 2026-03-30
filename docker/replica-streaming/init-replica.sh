#!/bin/bash
set -e

PGDATA="/var/lib/postgresql/data"

# Ensure data directory exists with correct ownership
mkdir -p "$PGDATA"
chown postgres:postgres "$PGDATA"
chmod 0700 "$PGDATA"

if [ ! -s "$PGDATA/PG_VERSION" ]; then
    echo "Initializing replica via pg_basebackup..."

    until pg_isready -h postgres -p 5432; do
        echo "Waiting for primary to be ready..."
        sleep 2
    done

    gosu postgres env PGPASSWORD=replicator pg_basebackup \
        -h postgres -p 5432 \
        -U replicator \
        -D "$PGDATA" \
        -Fp -Xs -P -R \
        -S replica_streaming_slot

    echo "Replica initialized successfully."
fi

exec gosu postgres postgres -c config_file=/etc/postgresql/postgresql.conf

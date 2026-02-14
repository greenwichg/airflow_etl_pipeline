-- =============================================================================
-- PostgreSQL CDC Prerequisites
-- =============================================================================
-- Run this on EACH regional PostgreSQL database to enable logical replication
-- for Debezium CDC connectors.
--
-- Prerequisites:
--   1. PostgreSQL 10+ with wal_level = 'logical' in postgresql.conf
--   2. max_replication_slots >= 4 (one per region connector)
--   3. max_wal_senders >= 4
-- =============================================================================

-- 1. Verify WAL level is set to 'logical'
SHOW wal_level;
-- Expected: 'logical'
-- If not, update postgresql.conf:  wal_level = logical  (requires restart)

-- 2. Create a dedicated replication user for Debezium
CREATE ROLE debezium_replication WITH
    LOGIN
    REPLICATION
    PASSWORD 'CHANGE_ME_IN_PRODUCTION';

-- 3. Grant read access on the source schema
GRANT USAGE ON SCHEMA sales TO debezium_replication;
GRANT SELECT ON ALL TABLES IN SCHEMA sales TO debezium_replication;
ALTER DEFAULT PRIVILEGES IN SCHEMA sales
    GRANT SELECT ON TABLES TO debezium_replication;

-- 4. Create a publication for the transactions table
-- Debezium uses this to subscribe to row-level changes via pgoutput.
-- Repeat this on each regional database with the appropriate publication name:
--   us_east  -> dbz_publication_us_east
--   us_west  -> dbz_publication_us_west
--   europe   -> dbz_publication_europe
--   asia_pacific -> dbz_publication_asia_pacific

CREATE PUBLICATION dbz_publication_us_east
    FOR TABLE sales.transactions;

-- 5. Ensure the transactions table has REPLICA IDENTITY FULL
-- This ensures UPDATE and DELETE events include all column values,
-- not just the primary key (needed for accurate CDC).
ALTER TABLE sales.transactions REPLICA IDENTITY FULL;

-- 6. Add an index on updated_at for efficient incremental queries
-- (used by the Airflow timestamp-based extraction as a fallback)
CREATE INDEX IF NOT EXISTS idx_transactions_updated_at
    ON sales.transactions (updated_at);

-- 7. Verify the replication slot will be auto-created by Debezium
-- (Debezium creates the slot on first connect; verify with:)
-- SELECT * FROM pg_replication_slots;

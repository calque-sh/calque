-- The role `calque dev` connects as.
--
-- It cannot be the image's bootstrap role. That role is a superuser, and
-- enrollment refuses a superuser outright with SUPERUSER_NOT_LEAST_PRIVILEGE
-- (source-control validates least privilege before anything is enrolled), so a
-- dev fixture that only offered the bootstrap role could never start the server.
--
-- REPLICATION is required for the logical slot. CREATE on the database is
-- required because the server installs its `calque_control` schema there,
-- and CREATE on `public` because this role owns the demo tables it seeds --
-- CREATE PUBLICATION FOR TABLE requires ownership of every named table.
--
-- These credentials are for a disposable local container that binds to
-- loopback only. Nothing here is a deployment pattern.
CREATE ROLE calque_sync LOGIN PASSWORD 'calque_sync' REPLICATION;
GRANT CREATE ON DATABASE calque_dev TO calque_sync;
GRANT CREATE, USAGE ON SCHEMA public TO calque_sync;

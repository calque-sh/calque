-- Run by `scripts/live-postgres-suite.sh bootstrap` as the image's superuser,
-- connected to the suite's database, with psql's `role` variable naming the
-- suite's role. Rerunning it replaces the functions in place.
--
-- Only a superuser may create a `FOR TABLES IN SCHEMA` publication, and the
-- suite's role is not one. These two functions, owned by the superuser, create
-- such a publication over a schema whose tables only the calling role can own,
-- and drop one they created for that role, so no test connects as a superuser.

CREATE SCHEMA IF NOT EXISTS calque_live;

CREATE TABLE IF NOT EXISTS calque_live.schema_publication (
  publication oid PRIMARY KEY,
  creator name NOT NULL
);
REVOKE ALL ON calque_live.schema_publication FROM PUBLIC;

CREATE OR REPLACE FUNCTION calque_live.create_schema_publication(publication name, target name)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $function$
DECLARE
  caller oid := (SELECT oid FROM pg_roles WHERE rolname = session_user);
  schema_oid oid := (SELECT oid FROM pg_namespace WHERE nspname = target AND nspowner = caller);
BEGIN
  -- A schema publication carries every table in the schema, now and later, so
  -- the caller must own each one and no other role may create one there.
  IF schema_oid IS NULL
     OR EXISTS (SELECT 1 FROM pg_class
                 WHERE relnamespace = schema_oid AND relkind IN ('r', 'p') AND relowner <> caller)
     OR EXISTS (SELECT 1 FROM pg_namespace, aclexplode(nspacl) AS item
                 WHERE pg_namespace.oid = schema_oid
                   AND item.privilege_type = 'CREATE' AND item.grantee <> caller) THEN
    RAISE EXCEPTION 'schema % and every table in it must be owned, and creatable in, by % alone',
      target, session_user;
  END IF;
  DELETE FROM calque_live.schema_publication
   WHERE schema_publication.publication NOT IN (SELECT oid FROM pg_publication);
  EXECUTE format('CREATE PUBLICATION %I FOR TABLES IN SCHEMA %I', publication, target);
  INSERT INTO calque_live.schema_publication
  SELECT oid, session_user FROM pg_publication WHERE pubname = publication;
END
$function$;

CREATE OR REPLACE FUNCTION calque_live.drop_schema_publication(publication name)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $function$
DECLARE
  created oid := (
    SELECT candidate.oid FROM pg_publication AS candidate
      JOIN calque_live.schema_publication AS made ON made.publication = candidate.oid
     WHERE candidate.pubname = drop_schema_publication.publication
       AND made.creator = session_user);
BEGIN
  IF created IS NULL THEN
    RAISE EXCEPTION 'publication % was not created here for %', publication, session_user;
  END IF;
  EXECUTE format('DROP PUBLICATION %I', publication);
  DELETE FROM calque_live.schema_publication WHERE schema_publication.publication = created;
END
$function$;

REVOKE ALL ON FUNCTION calque_live.create_schema_publication(name, name) FROM PUBLIC;
REVOKE ALL ON FUNCTION calque_live.drop_schema_publication(name) FROM PUBLIC;
GRANT USAGE ON SCHEMA calque_live TO :"role";
GRANT EXECUTE ON FUNCTION calque_live.create_schema_publication(name, name) TO :"role";
GRANT EXECUTE ON FUNCTION calque_live.drop_schema_publication(name) TO :"role";

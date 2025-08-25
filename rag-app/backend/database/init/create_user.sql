-- Create non-privileged application user for Row-Level Security (RLS)
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'ragapp_user') THEN
        CREATE USER ragapp_user WITH PASSWORD 'ragapp_password';
    END IF;
END$$;

GRANT CONNECT ON DATABASE ragapp TO ragapp_user;
GRANT USAGE ON SCHEMA public TO ragapp_user;
-- Only SELECT permissions - data loading uses superuser
GRANT SELECT ON documents TO ragapp_user;
GRANT SELECT ON document_chunks TO ragapp_user;

-- Schema owner and database loader user:
CREATE ROLE "data_discovery" LOGIN PASSWORD 'password';
-- File download job creator API user:
CREATE ROLE "job_creator" LOGIN PASSWORD 'password';
-- Metadata API user:
CREATE ROLE "dd_api" LOGIN PASSWORD 'password';

CREATE DATABASE "data_discovery" OWNER 'data_discovery';

-- Lock down the database to only allow those users to connect
REVOKE ALL PRIVILEGES ON DATABASE "data_discovery" FROM PUBLIC;
GRANT CONNECT ON DATABASE "data_discovery" TO data_discovery;
GRANT CONNECT ON DATABASE "data_discovery" TO job_creator;
GRANT CONNECT ON DATABASE "data_discovery" TO dd_api;
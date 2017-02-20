-- Schema owner and database loader user:
CREATE ROLE "data_discovery" LOGIN PASSWORD 'password';
-- File download job creator API user:
CREATE ROLE "job_creator" LOGIN PASSWORD 'password';
-- Metadata API user:
CREATE ROLE "dd_api" LOGIN PASSWORD 'password';

CREATE DATABASE "data_discovery" OWNER 'data_discovery';
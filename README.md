# dp-dd-database-loader

Microservice to load datapoints into the Data Discovery database, driven by Kafka messages.

# Database creation
----
You will need to create a postgres database and user to run the tests out of the box
- login to postgres: `psql postgres`
- create dd role: `CREATE ROLE data_discovery LOGIN PASSWORD 'password';`
- create dd db: `CREATE DATABASE "data_discovery" OWNER 'data_discovery';`

# Running the application
----

`sbt run`

Navigate to http://localhost:9000/ to initialise the application.

# How to test
----

'sbt "test-only scala.UnitTestRunner"'
or
'sbt "test-only scala.IntTestRunner"'

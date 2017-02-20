# dp-dd-database-loader

Microservice to load datapoints into the Data Discovery database, driven by Kafka messages.

### Configuration

The port that the application runs on needs to be passed as an option to the JVM `-Dhttp.port=20097`
When running locally the default port is set in the .sbtops file

| Environment variable | Default | Description
| -------------------- | ------- | -----------
| APPLICATION_SECRET   | changeme                                         | Required by Play framework
| KAFKA_ADDR           | localhost:9092                                   | The address of the Kafka instance
| KAFKA_CONSUMER_TOPIC | test                                             | The name of the Kafka topic consumed from 
| KAFKA_CONSUMER_GROUP | database-loader                                  | The name of the Kafka consumer group
| DATABASE_URL         | jdbc:postgresql://localhost:5432/data_discovery  | The URL of the database
| DATABASE_USER        | data_discovery                                   | The database user name
| DATABASE_PASSWORD    | password                                         | The password for the database user
| MIGRATION_USER       | postgres                                         | Database user to use for schema migrations
| MIGRATION_PASSWORD   | *blank*                                          | Password for database migration user

# Database creation
----
You will need to create a postgres database and user to run the tests out of the box
- Alter the [database creation script](scripts/sql/create_database.sql) to set the correct password to the data_discovery user.
- Run the script: `psql -a -f scripts/sql/create_database.sql`

All other schema and role creation will be managed automatically.

# Database migration/update
----
We're using Flyway to manage changes to the database. Flyway is invoked automatically at application startup to migrate the db to the latest version, but will fail if the db wasn't initialised by flyway.
To clean the database (drop all the tables) you can run:
```bash
sbt clean update flywayClean
```
For more information about what you can do with flyway using sbt, see: https://flywaydb.org/documentation/sbt/

# Running the application
----

```bash
sbt run
```

Navigate to http://localhost:9000/ to initialise the application.

# How to test
----

```bash
sbt test - this will only run the unit tests in the /test folder 
```
or
```bash
sbt it:test (or it's alias 'sbt int-test') - this will run the integration tests in the /it folder
```

# csv Input Format
---
0. Observation    - the actual number  -  dim_data_point.value
1. Data_Marking   - supressed etc   - dim_data_point.data_marking
2. Observation_Type  - (CI/CV) - dim_data_point.observation_type.name

3. Dimension_1 - Hierachy id - (optional) the id of a hierarchy used by this dimension 
4. Dimension_1 - name - the name of the dimension
5. Dimension_1 - value - the value of the dimension

etc for repeating dimension hierarchy/name/value triplets

### Contributing

See [CONTRIBUTING](CONTRIBUTING.md) for details.

### License

Copyright ©‎ 2017, Office for National Statistics (https://www.ons.gov.uk)

Released under MIT license, see [LICENSE](LICENSE.md) for details.

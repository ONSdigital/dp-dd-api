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

# Database creation
----
You will need to create a postgres database and user to run the tests out of the box
- login to postgres: `psql postgres`
- create dd role: `CREATE ROLE data_discovery LOGIN PASSWORD 'password';`
- create dd db: `CREATE DATABASE "data_discovery" OWNER 'data_discovery';`

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
0 - Observation    - the actual number  -  dim_data_point.value
1 - Data_Marking   - supressed etc   - dim_data_point.data_marking
2 - ValueDomain  - (Measure_Type)(range of valid values) - dim_data_point.variable.value_domain.value_domain
3 - Observation_Type  - (CI/CV) - dim_data_point.observation_type.name
4 - Obs_Type_Value   -  observation type value (95% etc)  - dim_data_point.observation_type_value
5 - Unit_Of_Measure  - 1000's of houses or Kg's of chickens etc - dim_data_point.variable.unit_type.unit_type
6 - Geographical_Hierarchy
7 - Geographic_Area  -   georgraphic_area.ext_code = Geographic Area - look up and use - requires areas to be already in db
8 - Time - dim_data_point.population.time_period
9 - Time_Type -  year/month etc -  dim_data_type.population.time_period.time_type
10 - CDID - code used on existing website to reference datasets - add to dim_data_set or metadata

11 - Dimension_1 - dimension name - concept_system.concept_system
12 - Dimension_Value_1  - dimension value - category.name
etc for repeating dimension key/value pairs

### Contributing

See [CONTRIBUTING](CONTRIBUTING.md) for details.

### License

Copyright ©‎ 2017, Office for National Statistics (https://www.ons.gov.uk)

Released under MIT license, see [LICENSE](LICENSE.md) for details.

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
sbt "test-only scala.UnitTestRunner"
```
or
```bash
sbt "test-only scala.IntTestRunner"
```


### Contributing

See [CONTRIBUTING](CONTRIBUTING.md) for details.

### License

Copyright ©‎ 2017, Office for National Statistics (https://www.ons.gov.uk)

Released under MIT license, see [LICENSE](LICENSE.md) for details.

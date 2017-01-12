FROM java:8

WORKDIR /app/

ADD target/scala-*/dp-dd-database-loader-assembly-*.jar .

ENTRYPOINT java -jar dp-dd-database-loader-assembly-*.jar

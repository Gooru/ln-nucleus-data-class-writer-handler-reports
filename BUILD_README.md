Class Reports WRITE Handler
===========================

## Prerequisites

- Gradle 2.7
- Java 8
- Kafka Consumer 0.10.1.1

## Running Build

The default task is *shadowJar* which is provided by plugin. So running *gradle* from command line will run *shadowJar* and it will create a fat jar in build/libs folder. Note that there is artifact name specified in build file and hence it will take the name from directory housing the project.

Once the far Jar is created, it could be run as any other Java application.

## Running the Jar as an application

Following command could be used, from the base directory.

Note that any options that need to be passed onto Vertx instance need to be passed at command line e.g, worker pool size etc

> java -classpath ./build/libs/nucleus-handlers-insights-events-0.1-snapshot-fat.jar: -Dvertx.logger-delegate-factory-class-name=io.vertx.core.logging.SLF4JLogDelegateFactory io.vertx.core.Launcher -conf src/main/resources/nucleus-data-class-writer-handler-reports.json -cluster -instance 1

Since we implemented Kafka consumer to consume messages, Need not to form cluster. Please make sure correct Kafka topic and group ID. If you want to deploy same handlers multiple time to handle the request traffic, group ID should be same. So that it will be act as loadbalancer.

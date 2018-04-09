#!/bin/bash

export DEBEZIUM_VERSION=0.7;
docker-compose kill aggregator;
docker-compose rm -f aggregator;
mvn clean package -f poc-ddd-aggregates/pom.xml;
docker-compose up --build -d aggregator;
docker-compose logs -f aggregator;

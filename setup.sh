#!/bin/bash

#Check Docker existence
DATE=`date +%Y.%m.%d.%H.%M`
echo $DATE
echo "Hello we are going to install Deribit prices extractor, it will take several moments to start the app, Hold on"
echo "Checking Docker"
if [ ! -x "$(command -v docker)" ]; then
  ***************
  *** ABORTED ***
  ***************
  echo "Docker is not installed, please install docker and rerun the script."
  echo "Installation link: https://www.docker.com/get-started/"

  exit 1
fi
echo "Docker is installed, will pull images"
docker-compose  -p laevitas_bridge -f infra/docker-compose.yml up -d
echo $DATE "Infrastructure (Kafka + Postgres) are installed"

echo $DATE "Waiting for the infra to gt ready!"
sleep 25

echo "You can visit http://127.0.0.1:9021 to check Kafka cluster"

echo "You can visit Postgres  via pgadmin http://127.0.0.1:5050 "

echo "To get access to pgadmin use the following credentials"
echo "email: laevitas@laevitas.ch"
echo "Password: laevitas123"

echo "***************Postgres Credentials***************"
echo "DB,USER,PASS: laevitas"



echo "Installing Deribit pipeline ..."

docker-compose  -f deribit/docker-compose.yml up -d

echo "{$DATE}Deribit Pipeline is installed successfully, proceeding to install Consumer pipeline"

docker-compose  -f persistent_consumer/docker-compose.yml up -d

echo "{$DATE} Consumer Pipeline is installed successfully"
#"table.name.format": "deribit_prices",

#$ curl -s   -X "POST" "http://localhost:8083/connectors/"   -H "Content-Type: application/json" -d '
#{
#  "name": "pg-sink-connector",
#  "config": {
#    "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
#    "tasks.max": "1",
#    "errors.log.enable": "true",
#    "errors.log.include.messages": "true",
#    "topics": "deribit_prices",
#    "connection.url": "jdbc:postgresql://postgres:5432/laevitas",
#    "connection.user": "laevitas",
#    "connection.password": "laevitas",
#    "dialect.name": "PostgreSqlDatabaseDialect",
#    "connection.attempts": "600",
#    "connection.backoff.ms": "600000",
#    "batch.size": "20",
#    "auto.create": "true",
#    "auto.evolve": "true",
#    "max.retries": "150",
#    "retry.backoff.ms": "30000",
#    "errors.tolerance": "all",
#    "errors.log.enable":true,
#    "errors.log.include.messages":true
#    "errors.retry.delay.max.ms": 60000,
#    "errors.retry.timeout": 300000
#  }
#}'
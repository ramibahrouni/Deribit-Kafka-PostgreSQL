curl -s --noproxy '*'     -X "POST" "http://localhost:8083/connectors/"      -H "Content-Type: application/json"  -d '{"name": "pg-sink-connector",
  "config": {
    "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
    "connection.attempts": 600,
    "connection.backoff.ms": 600000,
    "tasks.max": "1",
    "topics": "deribit_prices",
    "connection.url": "jdbc:postgresql://postgres:5432/laevitas",
    "connection.user": "laevitas",
    "connection.password": "laevitas",
    "dialect.name": "PostgreSqlDatabaseDialect",
    "batch.size": 20,
    "auto.evolve": true,
    "table.name.format": "deribit_prices",
    "max.retries": 150,
    "retry.backoff.ms": 30000
    }
 }'
# Kafka Sink Connector

## Running in development

```bash
mvn clean package

export CLASSPATH="$(find target/ -type f -name '*.jar' | grep '\-package' | tr '\n' ':')"
export CONFLUENT_HOME="/PATH"

${CONFLUENT_HOME}/bin/connect-standalone config/worker.properties config/solitary-connector.properties

kafka-console-producer --bootstrap-server localhost:9092 --topic bunch-of-monkeys --property "parse.key=true" --property "key.separator=:"
kafka-console-consumer --bootstrap-server localhost:9092 --topic bunch-of-monkeys --from-beginning
```

```bash
curl -s -XGET -H "Content-Type: application/json; charset=UTF-8" http://localhost:8083/connectors/ | jq .

curl -s -XPOST -H "Content-Type: application/json; charset=UTF-8" http://localhost:8083/connectors/ -d '
{
    "name": "oom-solitary-connector",
    "config": {
      "connector.class":"io.michelin.connect.SolitaryFileSinkConnector",
      "tasks.max":"1",
      "topics":"bunch-of-monkeys",
      "path":"/tmp/oom",
      "prefix":"x"
    }
}
' | jq .
```

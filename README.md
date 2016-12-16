# Stream Kafka Ingestor
A simple tool for streaming a file to a Kafka Cluster

# Usuage
```
java -jar streamingestor.jar -f <file> -t <throughout> -d <duration> -p <kafka-broker-address:port,kafka-broker-address:port...> -m <maxtupels>
```
# How to build
To build, simply run
```
mvn install
```

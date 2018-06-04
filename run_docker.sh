
echo "Clean existing docker containers ..."
# close all running docker
docker rm -f $(docker ps -a -q)

echo "Start docker containers: zookeeper, kafka, redis ..."
# Start a Zookeeper Container
docker run -d -p 2181:2181 -p 2888:2888 -p 3888:3888 --name zookeeper confluent/zookeeper

# Start a Kafka Container
docker run -d -p 9092:9092 -e KAFKA_ADVERTISED_HOST_NAME=localhost -e KAFKA_ADVERTISED_PORT=9092 --name kafka --link zookeeper:zookeeper confluent/kafka

# Start a Redis Container
docker run -d -p 6379:6379 --name redis redis:alpine


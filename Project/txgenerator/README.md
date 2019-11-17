docker build -t tx-generator .
docker run -it --rm --network master-thesis_kafka-network --name tx-generator tx-generator
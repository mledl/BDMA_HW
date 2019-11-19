


kafkacat -C -b localhost:9092 -t new_txs
kafkacat -P -b localhost:9092 -t topic


./kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic new_txs,fraud_txs

./kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic "new_txs"
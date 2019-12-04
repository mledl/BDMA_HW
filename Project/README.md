


kafkacat -C -b localhost:9092 -t new_txs
kafkacat -P -b localhost:9092 -t topic


./kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic new_txs,fraud_txs

./kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic "new_txs"


TODO:
1. setup prod env
    - save/read model from hadoop
    - read data from hadoop
    - real time stats about everything 
    
2. Make pretty ok ML model with spark [modelbuilder]
3. Make some streaming stats with spark streaming 
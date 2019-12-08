# TxGenerator
Holds code for transaction generator which is used to simulate realtime payments. Pushes txs into
Kafka topic.

### Local development
    /Users/pawelurbanowicz/venv/bin/python setup.py install
    python3 app.py
### Production deploy
    docker build -t tx-generator .
    docker run -it --rm --network master-thesis_kafka-network --name tx-generator tx-generator

### Architecture 


### TODO/IDEAS BELOW


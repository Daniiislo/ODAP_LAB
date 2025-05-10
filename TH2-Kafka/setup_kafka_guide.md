# Set up Kafka in linux

## Install JDK

Link: [https://www.oracle.com/vn/java/technologies/downloads/](https://www.oracle.com/vn/java/technologies/downloads/)

```bash
sudo dpkg -i filename.deb
```

## Install Kafka

Link: [https://kafka.apache.org/downloads](https://kafka.apache.org/downloads)

Tutorial: [https://kafka.apache.org/quickstart](https://kafka.apache.org/quickstart)

- Generate CLUSTER_ID

```bash
 KAFKA_CLUSTER_ID="$(bin/kafka-storage.sh random-uuid)"
```

- Format Log Directories

```bash

bin/kafka-storage.sh format --standalone -t $KAFKA_CLUSTER_ID -c config/server.properties
```

- Start the Kafka Server

```bash
bin/kafka-server-start.sh config/server.properties
```

- Configurate the server.properties: Open file config/server.properties in eidtor and configute it.

## Create a topic

```bash
bin/kafka-topics.sh --create --topic test --bootstrap-server localhost:9092 --replication-factor 1 --partitions 3
```

- Kiểm tra danh sách topic đã tạo

```bash
bin/kafka-topics.sh --list --bootstrap-server localhost:9092
```

## Create Kafka Producer

```bash
bin/kafka-console-producer.sh --bootstrap-server localhost:9092  --topic test
```

## Create Kafka Consumer

```bash
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic test --from-beginning
```
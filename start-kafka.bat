@echo off
echo Downloading Kafka Binary...
powershell -Command "& {Invoke-WebRequest -Uri 'https://downloads.apache.org/kafka/3.6.1/kafka_2.13-3.6.1.tgz' -OutFile 'kafka.tgz'}"

echo Extracting Kafka...
powershell -Command "& {tar -xzf kafka.tgz -C kafka-local --strip-components 1}"

echo Starting Zookeeper...
start cmd /k "cd kafka-local && .\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties"

echo Waiting for Zookeeper to start...
timeout /t 10

echo Starting Kafka...
start cmd /k "cd kafka-local && .\bin\windows\kafka-server-start.bat .\config\server.properties"

echo Waiting for Kafka to start...
timeout /t 10

echo Creating topic...
cd kafka-local && .\bin\windows\kafka-topics.bat --create --topic steam_reviews_stream --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1

echo Kafka and Zookeeper are running.
echo To stop them, close the command windows or run stop-kafka.bat 
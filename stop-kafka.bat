@echo off
echo Stopping Kafka...
cd kafka-local && .\bin\windows\kafka-server-stop.bat

echo Waiting for Kafka to stop...
timeout /t 5

echo Stopping Zookeeper...
cd kafka-local && .\bin\windows\zookeeper-server-stop.bat

echo Kafka and Zookeeper have been stopped. 
#!/bin/bash
KAFKA_HOME=/usr/local/Cellar/kafka/3.5.1
KAFKA_PRODUCER_SCRIPT="$KAFKA_HOME/bin/kafka-console-producer"

# Check if Kafka producer script exists
if [ ! -f $KAFKA_PRODUCER_SCRIPT ]; then
    echo  $KAFKA_PRODUCER_SCRIPT
    echo "Kafka producer script not found. Please provide the correct path."
    exit 1
fi

# Define Kafka properties
KAFKA_TOPIC="test"
KAFKA_SERVER="localhost:29092"  # Change to your Kafka server address

# JSON file to read
#JSON_FILE="/Users/sebastian.pizarro/0code/0nexus/aa-dec/data/farmers-protest-tweets-2021-2-4.json"
JSON_FILE="/Users/sebastian.pizarro/0code/0nexus/aa-dec/data/sample.json"

# Read and send each line of the JSON file as a Kafka message
while IFS=, read -r json; do
    MESSAGE="$json"  # Customize this based on your JSON structure
   #$KAFKA_PRODUCER_SCRIPT --broker-list $KAFKA_SERVER --topic $KAFKA_TOPIC --property parse.key=true --property key.separator=: <<<$MESSAGE
   $KAFKA_PRODUCER_SCRIPT --broker-list $KAFKA_SERVER --topic $KAFKA_TOPIC --property key.separator=: <<<$MESSAGE
   echo $MESSAGE
done < "$JSON_FILE"

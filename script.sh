#!/bin/bash

# Start Zookeeper in a new terminal
gnome-terminal -- bash -c 'bin/zookeeper-server-start.sh config/zookeeper.properties; exec bash'

# Wait for Zookeeper to start
sleep 10

# Start Kafka server in a new terminal
gnome-terminal -- bash -c 'bin/kafka-server-start.sh config/server.properties; exec bash'

# Wait for Kafka server to start
sleep 10

gnome-terminal -- bash -c 'start-all.sh; exec bash'

sleep 10

# Start Python producer in a new terminal
gnome-terminal -- bash -c 'python3 producer.py; exec bash'

# Start Python consumer in a new terminal
gnome-terminal -- bash -c 'python3 consumer.py; exec bash'

# Keep terminals open
read -p "Press any key to close terminals..."


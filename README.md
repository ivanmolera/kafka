brew install kafka

brew services start zookeeper

brew services start kafka

/usr/local/Cellar/kafka/1.0.0/bin/

./kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic test-topic

./kafka-topics --list --zookeeper localhost:2181


brew services stop zookeeper
brew services stop kafka


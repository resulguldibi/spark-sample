# spark-sample

 This sample contains examples about apache spark rdd operation and integrations with kafka & cassandra.

- change directory to spark-sample project directory. <br/>
  cd <<spark-sample-directory>>
- start kafka, zookeeper and cassandra container. <br/>
  docker-compose up -d

# configure cassandra
- connect cassandra container to create keyspace and table. <br/> 
  docker exec -it cassandra cqlsh  
- create keyspace in cassandra.  
  CREATE KEYSPACE my_keyspace WITH replication = {'class' : 'NetworkTopologyStrategy','my-datacenter' : 1};  
- create table in cassandra.  
  CREATE TABLE my_keyspace.my_table (id text primary key,count int,word text);  

# configure kafka
- connect kafka container to create topic.  
  docker exec -it kafka bash  
- change directory to /opt/kafka/bin in kafka container.  
  cd /opt/kafka/bin  
- create topicin kafka using kafka-topics.sh   
  ./kafka-topics.sh --zookeeper zookeeper:2181 --replication-factor 1 --partition 1 --topic Test_Topic --create   
- verify created topic displaying topic list.  
 ./kafka-topics.sh --zookeeper zookeeper:2181 --list  
- produce sample message using kafka-console-producer.sh   
 ./kafka-console-producer.sh --broker-list localhost:9092 --topic Test_Topic  
- verify produced message displaying messages in created topic.  
 ./kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic Test_Topic2 --from-beginning  


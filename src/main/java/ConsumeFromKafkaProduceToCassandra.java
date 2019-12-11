import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.datastax.spark.connector.cql.CassandraConnector;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;

import java.util.*;
import java.util.regex.Pattern;


public final class ConsumeFromKafkaProduceToCassandra {
    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) throws Exception {

        String brokers = "localhost:9092";
        String groupId = "test-group2";
        String topics = "Test_Topic2";

        SparkConf sparkConf = new SparkConf()
                .setAppName("ConsumeFromKafkaProduceToCassandra")
                .setMaster("local[2]")
                .set("spark.cassandra.connection.host", "localhost")
                .set("spark.cassandra.connection.port", "9042")
                .set("spark.cassandra.connection.keep_alive_ms", "10000")
                .set("spark.executor.memory", "2g");

        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

        javaSparkContext.setLogLevel("ERROR");

        JavaStreamingContext javaStreamingContext = new JavaStreamingContext(javaSparkContext, Durations.seconds(2));

        Set<String> topicsSet = new HashSet<>(Arrays.asList(topics.split(",")));
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        // Create direct kafka stream with brokers and topics
        JavaInputDStream<ConsumerRecord<String, String>> messages = KafkaUtils.createDirectStream(
                javaStreamingContext,
                    LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Subscribe(topicsSet, kafkaParams));

        // Get the lines, split them into words, count the words and print
        JavaDStream<String> lines = messages.map(ConsumerRecord::value);
        JavaDStream<String> words = lines.flatMap(x -> Arrays.asList(SPACE.split(x)).iterator());
        JavaPairDStream<String, Integer> wordCounts = words.mapToPair(s -> new Tuple2<>(s, 1))
                .reduceByKey(Integer::sum);


        wordCounts.foreachRDD((VoidFunction<JavaPairRDD<String, Integer>>) rdd -> {

            final SparkConf _sparkConfig = rdd.context().getConf();
            final CassandraConnector cassandraConnector = CassandraConnector.apply(_sparkConfig);

            rdd.foreachPartitionAsync((VoidFunction<Iterator<Tuple2<String, Integer>>>) tuples -> {

                try (Session _session = cassandraConnector.openSession()) {
                    PreparedStatement prepared = _session.prepare("insert into my_keyspace.my_table (id,word,count) values (?, ?, ?)");

                    while (tuples.hasNext()) {
                        final Tuple2<String, Integer> tuple = tuples.next();
                        final UUID uuid = UUID.randomUUID();
                        final String randomUUIDString = uuid.toString();
                        BoundStatement bound = prepared.bind(randomUUIDString, tuple._1, tuple._2);
                        _session.execute(bound);
                    }
                }
            });
        });


        // Start the computation
        javaStreamingContext.start();
        javaStreamingContext.awaitTermination();
    }
}
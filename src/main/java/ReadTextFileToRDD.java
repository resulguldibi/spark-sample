import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class ReadTextFileToRDD {

    public static void main(String[] args) {

        //initialize spark conf
        SparkConf sparkConf = new SparkConf()
                .setAppName("SparkDemo")
                .setMaster("local[2]")
                .set("spark.executor.memory", "2g");

        //initialize spark context with sparkConf
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);



        //set log level
        javaSparkContext.setLogLevel("ERROR");

        //read lines from text file to JavaRDD<String> (lines)
        JavaRDD<String> lines = javaSparkContext.textFile("data/text-file1.txt");

        System.out.println("lines in text file...");
        lines.foreach(line -> System.out.println(line));

        /*

        System.out.println("lines in text file(V2)...");
        lines.collect().forEach(line -> System.out.println(line));

        for(String line:lines.collect()){
            System.out.println(line);
        }

        */

        JavaRDD<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());

        System.out.println("words in lines...");
        words.foreach(word -> System.out.println(word));

        JavaRDD<Tuple2<String, Integer>> wordsAndLengths = words.distinct().map(word -> new Tuple2<String, Integer>(word, word.length()));

        System.out.println("words and lengths...");
        wordsAndLengths.foreach(wordAndLength -> System.out.println(String.format("word : %s, length : %d", wordAndLength._1, wordAndLength._2)));

        System.out.println("words and counts...");
        JavaPairRDD<String, Integer> wordPairs = words.mapToPair(word -> new Tuple2<String, Integer>(word, 1));

        JavaPairRDD<String, Integer> wordsAndCounts = wordPairs.reduceByKey((a,b) -> a+b);

        wordsAndCounts.foreach(wordAndCount -> System.out.println(String.format("word : %s, count : %d", wordAndCount._1, wordAndCount._2)));

    }


}

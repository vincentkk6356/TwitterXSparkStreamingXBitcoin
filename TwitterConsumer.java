import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class TwitterConsumer {


    public static final String TWEET_SCHEMA =
            "{" + "\"type\":\"record\","
                    + "\"name\":\"Doc\","
                    + "\"doc\":\"adoc\"," + "\"fields\":["
                    + "  { \"name\":\"created_at\", \"type\":[\"string\",\"null\"]},"
                    + "  { \"name\":\"text\", \"type\":[\"string\",\"null\"]}"
                    + "]}";




    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf()
                .setAppName("tweetAnalyzer")
                .setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaStreamingContext ssc = new JavaStreamingContext(sc, new Duration(2000));

        Collection<String> topics = Arrays.asList("twitter_alpha");
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "localhost:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", ByteArrayDeserializer.class);
        kafkaParams.put("group.id", "group11");

        JavaInputDStream<ConsumerRecord<String,byte[]>> directKafkaStream =
                KafkaUtils.createDirectStream(
                        ssc,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.<String, byte[]>Subscribe(topics, kafkaParams)
                );
        directKafkaStream.mapToPair(record -> new Tuple2<>(record.key(), record.value()));


        directKafkaStream.foreachRDD(rdd -> {
            rdd.foreach(avroRecord -> {
                Schema.Parser parser = new Schema.Parser();
                Schema schema = parser.parse(TWEET_SCHEMA);
                Injection<GenericRecord, byte[]> recordInjection = GenericAvroCodecs.toBinary(schema);
                GenericRecord record = recordInjection.invert(avroRecord.value()).get();

                System.out.println("date= " + record.get("created_at")
                        + ", content= " + record.get("text"));
            });
        });

        ssc.start();
        ssc.awaitTermination();
    }


}



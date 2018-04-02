package com.group11.spark.kafka;


import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;


import java.util.Properties;

public class twitterProducer{


//    public static final String USER_SCHEMA = "{"
//            + "\"type\":\"record\","
//            + "\"name\":\"myrecord\","
//            + "\"fields\":["
//            + "  { \"name\":\"str1\", \"type\":\"string\" },"
//            + "  { \"name\":\"str2\", \"type\":\"string\" },"
//            + "  { \"name\":\"int1\", \"type\":\"int\" }"
//            + "]}";

    public static final String TWEET_SCHEMA =
            "{" + "\"type\":\"record\","
                    + "\"name\":\"Doc\","
                    + "\"doc\":\"adoc\"," + "\"fields\":["
                    + "  { \"name\":\"created_at\", \"type\":[\"string\",\"null\"]},"
                    + "  { \"name\":\"text\", \"type\":[\"string\",\"null\"]}"
                    + "]}";

    private static Producer<String, byte[]> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                KafkaProperties.BOOTSTRAPSERVER);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "group11");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                ByteArraySerializer.class.getName());

        return new KafkaProducer<>(props);

    }

    static void runProducer(final int sendMessageCount) throws Exception {

        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(TWEET_SCHEMA);
        Injection<GenericRecord, byte[]> recordInjection = GenericAvroCodecs.toBinary(schema);

        final Producer<String, byte[]> producer = createProducer();

        try {
            for (int i = 0; i < 1000; i++) {
                GenericData.Record avroRecord = new GenericData.Record(schema);
                avroRecord.put("created_at", "Created at:" + i);
                avroRecord.put("text", "Content:" + i);

                byte[] bytes = recordInjection.apply(avroRecord);


                final ProducerRecord<String, byte[]> record =
                        new ProducerRecord<>(KafkaProperties.TOPIC, bytes);

                RecordMetadata metadata = producer.send(record).get();

                System.out.printf("sent record(key=%s value=%s) " +
                                "meta(partition=%d, offset=%d) \n",
                        record.key(), record.value(), metadata.partition(),
                        metadata.offset());

            }
        } finally {
            producer.flush();
            producer.close();
        }
    }

    public static void main(String... args) throws Exception {
        if (args.length == 0) {
            runProducer(5);
        } else {
            runProducer(Integer.parseInt(args[0]));
        }
    }
}

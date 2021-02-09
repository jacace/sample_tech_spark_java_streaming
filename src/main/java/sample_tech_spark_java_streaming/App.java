package sample_tech_spark_java_streaming;

import java.util.*;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka010.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import scala.Tuple2;
//not used
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import org.apache.kafka.common.TopicPartition;

/**
 * Hello world!
 */
public final class App {
    private App() {
    }

    /**
     * Says hello to the world.
     * 
     * @param args The arguments of the program.
     */
    public static void main(String[] args) {

        new Config().load();
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put(StreamsConfig.APPLICATION_ID_CONFIG, Config.ConsumerId);
        kafkaParams.put("group.id", Config.GroupId);
        kafkaParams.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, Config.BootstrapServers);
        kafkaParams.put(StreamsConfig.SECURITY_PROTOCOL_CONFIG, Config.SecurityProtocol);
        kafkaParams.put(SaslConfigs.SASL_MECHANISM, Config.SaslMechanism);
        kafkaParams.put(SaslConfigs.SASL_JAAS_CONFIG, Config.SaslJaasConfig);
        kafkaParams.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, StringDeserializer.class);
        kafkaParams.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, StringDeserializer.class); //Serdes.String().getClass()
        // config.put(StreamsConfig.AUTO_OFFSET_RESET_CONFIG, "latest"); //earliest
        //kafkaParams.put("enable.auto.commit", false);

        String master = Config.SparkMaster;
        Duration microBatchDuration = new Duration(Config.MicroBatchDuration);

        Collection<String> topics = Arrays.asList(Config.TopicIn);
        SparkConf conf = new SparkConf().setAppName(Config.ConsumerId).setMaster(master);
        JavaStreamingContext jsc = new JavaStreamingContext(conf, microBatchDuration);

        JavaInputDStream<ConsumerRecord<String, String>> stream = null;
        stream = KafkaUtils.createDirectStream(jsc, LocationStrategies.PreferConsistent(),
                ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams));

        stream.mapToPair(record -> new Tuple2<>(record.key(), record.value()));

        /*
        stream.mapToPair(new PairFunction<ConsumerRecord<String, String>, String, String>() {
            @Override
            public Tuple2<String, String> call(ConsumerRecord<String, String> record) {
                return new Tuple2<>(record.key(), record.value());
            }
        });        
        */
    }
}

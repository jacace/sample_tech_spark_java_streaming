package sample_tech_spark_java_streaming;

import java.util.*;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka010.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import scala.Tuple2;
//not used
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.serializer.KryoSerializer;
import org.apache.kafka.common.TopicPartition;
import com.amazon.deequ.VerificationSuite;
import com.amazon.deequ.checks.*;
import com.amazon.deequ.constraints.*;

/**
 * Hello world!
 */
public final class App {

    private static Boolean isKafa = false;

    private App() {
    }

    /**
     * Says hello to the world.
     * 
     * @param args The arguments of the program.
     */
    public static void main(String[] args) {

        Map<String, Object> kafkaParams = new Config().load();
        String master = Config.SparkMaster;
        Duration microBatchDuration = new Duration(Config.MicroBatchDuration);
        Collection<String> topics = Arrays.asList(Config.TopicIn);

        SparkConf conf = new SparkConf().setAppName(Config.AppName).setMaster(master);
        conf.set("spark.serializer", KryoSerializer.class.getName());
        JavaSparkContext sparkContext = JavaSparkContext.fromSparkContext(SparkContext.getOrCreate(conf));
        JavaStreamingContext jsc = new JavaStreamingContext(sparkContext, microBatchDuration);
        JavaDStream<ConsumerRecord<String, String>> stream = null;

        if (!isKafa) {
            JavaRDD<ConsumerRecord<String, String>> javaRDD = sparkContext
                    .parallelize(Item.buildConsumerRecordList(Config.TopicIn));
            LinkedList<JavaRDD<ConsumerRecord<String, String>>> list = new LinkedList<>();
            list.add(javaRDD);
            stream = jsc.queueStream(list);
        } else {
            JavaInputDStream<ConsumerRecord<String, String>> stream2 = KafkaUtils.createDirectStream(jsc,
                    LocationStrategies.PreferConsistent(),
                    ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams));
        }

        // To achieve exactly once semantics
        stream.foreachRDD(new VoidFunction<JavaRDD<ConsumerRecord<String, String>>>() {
            private static final long serialVersionUID = 1L;

            @Override
            public void call(JavaRDD<ConsumerRecord<String, String>> rdd) throws Exception {

                // retrieve offset
                // OffsetRange[] offsetRanges = ((HasOffsetRanges) rdd.rdd()).offsetRanges();

                rdd.foreach(data -> {
                    System.out.println("Key: " + data.key() + ". Value: " + data.value());
                });
                System.out.println("Num of Records in RDD: " + Long.toString(rdd.count()));

                // ((CanCommitOffsets) stream.inputDStream()).commitAsync(offsetRanges);
            }
        });

        try {
            // Start the computation
            jsc.start();
            jsc.awaitTermination();
        } catch (Exception ex) {
            System.out.println("An Error happened:" + ex.getMessage());
        } finally {
            if (sparkContext != null) {
                sparkContext.close();
                jsc.close();
            }
        }

        /*
         * stream.mapToPair(new PairFunction<ConsumerRecord<String, String>, String,
         * String>() {
         * 
         * @Override public Tuple2<String, String> call(ConsumerRecord<String, String>
         * record) { return new Tuple2<>(record.key(), record.value()); } });
         */
    }
}

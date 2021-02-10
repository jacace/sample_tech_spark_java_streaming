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
import com.amazon.deequ.VerificationSuite;
import com.amazon.deequ.checks.*;
import com.amazon.deequ.constraints.*;
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
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class); // Serdes.String().getClass()
        kafkaParams.put("enable.auto.commit", false); // to commit offsets to Kafka yourself
                                                      // after you know your output has been stored
                                                      // using the commitAsync AP
        // kafkaParams.put(StreamsConfig.AUTO_OFFSET_RESET_CONFIG, "latest"); //earliest

        String master = Config.SparkMaster;
        Duration microBatchDuration = new Duration(Config.MicroBatchDuration);

        Collection<String> topics = Arrays.asList(Config.TopicIn);
        SparkConf conf = new SparkConf().setAppName(Config.ConsumerId).setMaster(master);

        JavaStreamingContext jsc = new JavaStreamingContext(conf, microBatchDuration);
        final JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(jsc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams));

        // Map operations run in the executors
        // Lambda version: stream.mapToPair(record -> new Tuple2<>(record.key(), record.value()));
        stream.mapToPair(new PairFunction<ConsumerRecord<String, String>, String, String>() {
            @Override
            public Tuple2<String, String> call(ConsumerRecord<String, String> record) {
                System.out.println("Reading key: " + record.key() + ", with value: " + record.value());
                return new Tuple2<>(record.key(), record.value());
            }
        });

        // To achieve exactly once semantics
        stream.foreachRDD((VoidFunction<JavaRDD<ConsumerRecord<String, String>>>) rdd -> {
            // retrieve offset
            OffsetRange[] offsetRanges = ((HasOffsetRanges) rdd.rdd()).offsetRanges();

            // simple test here
            rdd.mapToPair(new PairFunction<ConsumerRecord<String, String>, String, String>() {
                @Override
                public Tuple2<String, String> call(ConsumerRecord<String, String> record) {
                    System.out.println("Reading key: " + record.key() + ", with value: " + record.value());
                    return new Tuple2<>(record.key(), record.value());
                }
            });

            // DEEQU test starts here (inspired from: // https://databricks.com/blog/2020/03/04/how-to-monitor-data-stream-quality-using-spark-streaming-and-delta-lake.html)
            // .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
            // 1) reassign our current state to the previous next state
            // val stateStoreCurr = stateStoreNext

            // 2) run analysis on the current batch, aggregate with saved state
            // val metricsResult = AnalysisRunner.run(data=batchDF, ...)

            //Check check1 = new Check(CheckLevel.Error(), "integrity checks", null);
            //check1.hasSize(null, null);

            // 3) verify the validity of our current microbatch
            //VerificationSuite verifier = new VerificationSuite();
            // verifier.onData(rdd); //DataSet<Row>
            // verifier.addCheck(check1);
            // val verificationResult = verifier.run$default$3(); // check other run()
            // options: requiredAnalysis aggregateWith, saveStatesWith, metricsRepository,
            // saveOrAppendResultsWithKey)

            // if verification fails, write batch to bad records table
            // if (verificationResult.status != CheckStatus.Success()) {

            // }

            // write the current results into the metrics table
            // Metric_results.write
            // .format("delta")
            // .mode("overwrite")
            // .saveAsTable("deequ_metrics")
            // }
            // .start()

            // and commit some time later, after outputs have completed
            ((CanCommitOffsets) stream.inputDStream()).commitAsync(offsetRanges);
        });

        try

        {
            // Start the computation
            jsc.start();
            jsc.awaitTermination();
        } catch (Exception ex) {
            System.out.println("An Error happened:" + ex.getMessage());
        }

        System.out.println("Adone deal!");

        /*
         * stream.mapToPair(new PairFunction<ConsumerRecord<String, String>, String,
         * String>() {
         * 
         * @Override public Tuple2<String, String> call(ConsumerRecord<String, String>
         * record) { return new Tuple2<>(record.key(), record.value()); } });
         */
    }
}

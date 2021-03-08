package sample_tech_spark_java_streaming;

import java.util.*;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka010.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.serializer.KryoSerializer;
import scala.Tuple2;

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
        JavaDStream<ConsumerRecord<String, String>> productStream = null;
        JavaDStream<ConsumerRecord<String, String>> salesStream = null;

        if (!isKafa) {
            JavaRDD<ConsumerRecord<String, String>> productsRDD = sparkContext
                    .parallelize(Item.buildItemRecordList(Config.TopicIn));
            LinkedList<JavaRDD<ConsumerRecord<String, String>>> productslist = new LinkedList<>();
            productslist.add(productsRDD);
            productStream = jsc.queueStream(productslist);

            //Only neeed for Join demo
            JavaRDD<ConsumerRecord<String, String>> salesRDD = sparkContext
                    .parallelize(Item.buildSalesRecordList(Config.TopicIn));
            LinkedList<JavaRDD<ConsumerRecord<String, String>>> saleslist = new LinkedList<>();
            saleslist.add(salesRDD);
            salesStream = jsc.queueStream(saleslist);
        } else {
            JavaInputDStream<ConsumerRecord<String, String>> stream3 = KafkaUtils.createDirectStream(jsc,
                    LocationStrategies.PreferConsistent(),
                    ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams));
        }

        joinDemo(productStream, salesStream);
        // To achieve exactly once semantics
        productStream.foreachRDD(new VoidFunction<JavaRDD<ConsumerRecord<String, String>>>() {
            private static final long serialVersionUID = 1L;

            @Override
            public void call(JavaRDD<ConsumerRecord<String, String>> rdd) throws Exception {

                // retrieve offset
                // OffsetRange[] offsetRanges = ((HasOffsetRanges) rdd.rdd()).offsetRanges();

                rdd.foreach(data -> {
                    System.out.println("Key: " + data.key() + ". Value: " + data.value());
                });                

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

    }

    private static void joinDemo(JavaDStream<ConsumerRecord<String, String>> productStream,
                                 JavaDStream<ConsumerRecord<String, String>> salesStream) {
        ObjectMapper jacksonParser = new ObjectMapper();
        JavaPairDStream<Object, Object> s1 = productStream.mapToPair(record -> new Tuple2<Object, Object>(record.key(),
                jacksonParser.readValue(record.value(), Item.class)));
        JavaPairDStream<Object, Object> s2 = salesStream.mapToPair(record -> new Tuple2<Object, Object>(record.key(),
                jacksonParser.readValue(record.value(), DailySales.class)));
        JavaPairDStream<Object, Tuple2<Object, Object>> s3 = s1.join(s2);

        s3.foreachRDD(new VoidFunction<JavaPairRDD<Object, Tuple2<Object, Object>>>() {
            private static final long serialVersionUID = 1L;            

            @Override
            public void call(JavaPairRDD<Object, Tuple2<Object, Object>> rdd) throws Exception {
                System.out.println("Num of Records in RDD: " + Long.toString(rdd.count()));
                rdd.foreach(data -> {
                    System.out.println("Key: " + data._1().toString() + ". Obj in Value 1: " + ((Item) data._2()._1()).name
                            + ". Obj in Value 2: " + Integer.toString(((DailySales) data._2()._2()).soldUnits));
                });
            }
        });
    }
}

package sample_tech_spark_java_streaming;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;

public class Config {
    
    public static String AppName;
    public static String TopicIn;
    public static int MicroBatchDuration;
    public static String SparkMaster;
    // public static String SslEndpointIdentificationAlgorithm;

    public Map<String, Object> load() {
        Map<String, Object> kafkaParams = new HashMap<>();

        try {
            String file = "application.properties";
            InputStream fins = getClass().getClassLoader().getResourceAsStream(file);
            Properties appSettings = new Properties();
            if (fins != null)
                appSettings.load(fins);                   
                        
            //kafka config
            kafkaParams.put(StreamsConfig.APPLICATION_ID_CONFIG, (String) appSettings.get("consumerid"));
            kafkaParams.put("groupIdPreffix", (String) appSettings.get("groupidpreffix")); //groupid not used
            kafkaParams.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, (String) appSettings.get("bootstrap.servers"));
            kafkaParams.put(StreamsConfig.SECURITY_PROTOCOL_CONFIG, (String) appSettings.get("security.protocol"));
            kafkaParams.put(SaslConfigs.SASL_MECHANISM, (String) appSettings.get("sasl.mechanism"));
            kafkaParams.put(SaslConfigs.SASL_JAAS_CONFIG,  (String) appSettings.get("sasl.jaas.config"));
            kafkaParams.put("enable.auto.commit", Boolean.parseBoolean((String) appSettings.get("enable.auto.commit")));             
            kafkaParams.put("key.deserializer", Serdes.String().getClass().getName());
            kafkaParams.put("value.deserializer", Serdes.String().getClass().getName());            
            // enable.auto.commit: false to commit offsets to Kafka yourself after you know your output has been stored using the commitAsync AP
            // kafkaParams.put(StreamsConfig.AUTO_OFFSET_RESET_CONFIG, "latest"); //earliest

            //general config
            AppName = (String) appSettings.get("consumerid");
            TopicIn = (String) appSettings.get("topicin");
            MicroBatchDuration = Integer.parseInt((String) appSettings.get("microbatchduration"));
            SparkMaster = (String) appSettings.get("sparkmaster");     

            fins.close();

        } catch (IOException e) {
            System.out.println("Could not load settings file.");
            System.out.println(e.getMessage());
        }

        return kafkaParams;

    }

}

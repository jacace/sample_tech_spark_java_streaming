package sample_tech_spark_java_streaming;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class Config {

    public static String ConsumerId;
    public static String BootstrapServers;
    public static String SecurityProtocol;
    public static String SaslMechanism;
    public static String SaslJaasConfig;
    public static String TopicIn;
    public static String GroupId;
    public static int MicroBatchDuration;
    public static String SparkMaster;
    // public static String SslEndpointIdentificationAlgorithm;

    public void load() {
        try {
            String file = "application.properties";
            InputStream fins = getClass().getClassLoader().getResourceAsStream(file);
            Properties appSettings = new Properties();
            if (fins != null)
                appSettings.load(fins);

            // This is where you add your config variables:
            // ConsumerId = Boolean.parseBoolean((String) appSettings.get("DEBUG"));
            ConsumerId = (String) appSettings.get("consumerid");
            BootstrapServers = (String) appSettings.get("bootstrap.servers");
            SecurityProtocol = (String) appSettings.get("security.protocol");
            SaslMechanism = (String) appSettings.get("sasl.mechanism");
            SaslJaasConfig = (String) appSettings.get("sasl.jaas.config");
            TopicIn = (String) appSettings.get("topicin");
            GroupId = (String) appSettings.get("groupid");
            MicroBatchDuration = Integer.parseInt((String) appSettings.get("microbatchduration"));
            SparkMaster = (String) appSettings.get("sparkmaster");            
            fins.close();

        } catch (IOException e) {
            System.out.println("Could not load settings file.");
            System.out.println(e.getMessage());
        }

    }
}

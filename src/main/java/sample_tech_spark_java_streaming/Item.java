package sample_tech_spark_java_streaming;

import java.util.LinkedList;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.codehaus.jackson.map.ObjectMapper;

//Sample POJO from deeque project
public class Item {

    public String id;
    public String name;
    public String review;
    public String classification;
    public int rating;

    public String setId(String id) {
        return this.id = id;
    }

    public String getId() {
        return this.id;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getName() {
        return this.name;
    }

    public void setReview(String review) {
        this.review = review;
    }

    public String getReview() {
        return this.review;
    }

    public void setClassification(String classification) {
        this.classification = classification;
    }

    public String getClassification() {
        return this.classification;
    }

    public void setRating(int rating) {
        this.rating = rating;
    }

    public int getRating() {
        return this.rating;
    }

    public Item(String id, String name, String review, String classification, int rating) {
        setId(id);
        setName(name);
        setReview(review);
        setClassification(classification);
        setRating(rating);
    }

    public Item() {
    }

    public static LinkedList<ConsumerRecord<String, String>> buildItemRecordList(String topic) {

        LinkedList<ConsumerRecord<String, String>> list = new LinkedList<>();
        ObjectMapper jacksonParser = new ObjectMapper();
        int partition = 0;
        long offset = 0;
        String key = null;

        try {
            list.add(new ConsumerRecord<>(topic, partition, offset, "1", jacksonParser.writeValueAsString(new Item("1", "Thingy A", "awesome thing.", "high", 0))));
            list.add(new ConsumerRecord<>(topic, partition, offset, "2", jacksonParser.writeValueAsString(new Item("2", "Thingy B", "available at http://thingb.com", null, 0))));
            list.add(new ConsumerRecord<>(topic, partition, offset, "3", jacksonParser.writeValueAsString(new Item("3", null, null, "low", 5))));
            list.add(new ConsumerRecord<>(topic, partition, offset, "4", jacksonParser.writeValueAsString(new Item("4", "Thingy D", "checkout https://thingd.ca", "low", 10))));
            list.add(new ConsumerRecord<>(topic, partition, offset, "5", jacksonParser.writeValueAsString(new Item("5", "Thingy E", null, "high", 12))));
        } catch (Exception e) {
            e.printStackTrace();
        }

        return list;
    }

    public static LinkedList<ConsumerRecord<String, String>> buildSalesRecordList(String topic) {

        LinkedList<ConsumerRecord<String, String>> list = new LinkedList<>();
        ObjectMapper jacksonParser = new ObjectMapper();
        int partition = 0;
        long offset = 0;
        String key = null;

        try {
            list.add(new ConsumerRecord<>(topic, partition, offset, "1", jacksonParser.writeValueAsString(new DailySales("1", 0))));
            list.add(new ConsumerRecord<>(topic, partition, offset, "2", jacksonParser.writeValueAsString(new DailySales("2", 10))));
        } catch (Exception e) {
            e.printStackTrace();
        }

        return list;
    }
}

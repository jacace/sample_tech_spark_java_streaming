package sample_tech_spark_java_streaming;

public class DailySales {

    public String id;
    public int soldUnits;

    public String setId(String id) {
        return this.id = id;
    }

    public String getId() {
        return this.id;
    }

    public void setSoldUnits(int soldUnits) {
        this.soldUnits = soldUnits;
    }

    public int getSoldUnits() {
        return this.soldUnits;
    }

    public DailySales() {
    }

    public DailySales(String id, int soldUnits) {
        setId(id);
        setSoldUnits(soldUnits);
    }

}
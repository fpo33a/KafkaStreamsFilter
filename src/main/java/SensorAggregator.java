

public class SensorAggregator  {

    private String data = "";

    private int nbElement = 0;
    private int total = 0;

    public SensorAggregator() {
        //System.out.println( "   **** SensorAggregator ");
    }
    public String getData() {
        return data;
    }

    public void setData(String data) {
        this.data = this.data + " - " + data;
        this.nbElement++;
        this.setTotal(this.getTotal()+this.getSensorValue(data));
        System.out.println( " ----- "+this.getTotal() + " --- " + this.getNbElement());
    }

    public int getNbElement() {
        return nbElement;
    }

    public void setNbElement(int nbElement) {
        this.nbElement = nbElement;
    }

    public int getTotal() {
        return total;
    }

    public void setTotal(int total) {
        this.total = total;
    }

    //--------------------------------------------------

    // ok this is not the right way to do a json field content extraction but this is fast
    private int getSensorValue(String value) {
        String field = "\"SensorValue\": ";
        int pos = value.indexOf(field);
        int start = pos + field.length();
        int end = value.indexOf("}", start);
        String result = value.substring(start, end);

        return Integer.parseInt(result);
    }

    //--------------------------------------------------

    // to do: from and to json

}

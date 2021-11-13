import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import java.util.ArrayList;

public class SensorAggregator  {

    private ArrayList<Integer> values = new ArrayList<Integer>();

    //--------------------------------------------------

    public SensorAggregator() {
    }

    //--------------------------------------------------

    public void setData(String data) {
       this.values.add(this.getSensorValue(data));
    }

    //--------------------------------------------------

    public String dump () {
        String result = "";
        for (int i = 0; i < values.size(); i++) {
            if (i > 0) result += "," + values.get(i);
            else result += values.get(i);
        }
        return result;
    }

    //--------------------------------------------------

    public int getNbElement() {
        return values.size();
    }

    //--------------------------------------------------

    public int getTotal() {
        int total = 0;
        for (int i = 0; i < values.size(); i++) total += values.get(i);
        return total;
    }

    //--------------------------------------------------

    public double getMean() {
        int nbElement = this.getNbElement();
        if (nbElement > 0) return (double)  (double)this.getTotal() / (double) nbElement;
        return 0;
    }

    //--------------------------------------------------

    public double getStandardDeviation()
    {
        double standardDeviation = 0.0;
        double mean = this.getMean();

        for(int num: this.values) {
            standardDeviation += Math.pow(num - mean, 2);
        }

        return Math.sqrt(standardDeviation/this.getNbElement());
    }

    //--------------------------------------------------

    public byte [] serializeValues () {
        String result = "";
        for (int i = 0; i < values.size(); i++)
        {
            if (i > 0) result += ",";
            result += values.get(i);
        }
        return result.getBytes();
    }

    //--------------------------------------------------

    public void deserializeValues ( String data ) {
        values = new ArrayList<Integer>();
        String elements [] = data.split(",");
        for (int i = 0; i < elements.length; i++)
        {
            values.add(Integer.parseInt(elements[i]));
        }
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

}

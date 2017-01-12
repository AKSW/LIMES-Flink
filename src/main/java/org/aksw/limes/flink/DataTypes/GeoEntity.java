package org.aksw.limes.flink.DataTypes;

import org.apache.flink.api.java.tuple.Tuple3;

/**
 * @author Christopher Rost <c.rost@studserv.uni-leipzig.de>
 * @date 04.12.16
 */
public class GeoEntity extends Tuple3<String,Double,Double>{

    /**
     * Standard Constructor
     */
    public GeoEntity() {
        super();
    }

    /**
     * Extended constructor
     *
     * @param url as String
     * @param lati latitude value as Double
     * @param longi longitude value as Double
     */
    public GeoEntity(String url, Double lati, Double longi)
    {
        super(url,lati,longi);
    }

    public GeoEntity(Tuple3<String, Double, Double> tuple3)
    {
        super(tuple3.getField(0), tuple3.getField(1), tuple3.getField(2));
    }

    public String getUri()
    {
        return getField(0);
    }

    public Double getLatitude()
    {
        return getField(1);
    }

    public Double getLongitude()
    {
        return getField(2);
    }
}

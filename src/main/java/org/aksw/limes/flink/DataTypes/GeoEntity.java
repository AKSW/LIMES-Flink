package org.aksw.limes.flink.DataTypes;

import org.apache.flink.api.java.tuple.Tuple3;

/**
 * @author Christopher Rost <c.rost@studserv.uni-leipzig.de>
 * @date 04.12.16
 */
public class GeoEntity extends Tuple3<String,Double,Double>{

    public GeoEntity() {
        super();
    }

    public GeoEntity(String url, Double lati, Double longi) {
        super(url,lati,longi);
    }

    public Double getUri()
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

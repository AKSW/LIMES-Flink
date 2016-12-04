package org.aksw.limes.flink.Transformation;

import org.aksw.limes.flink.DataTypes.GeoEntity;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

/**
 * @author Christopher Rost <c.rost@studserv.uni-leipzig.de>
 * @date 04.12.16
 */
public class UriLatLongGroupReduce implements GroupReduceFunction<Tuple3<String,String,String>, GeoEntity> {
    @Override
    public void reduce(Iterable<Tuple3<String, String, String>> iterable, Collector<GeoEntity> collector) throws Exception {
        String url = null;
        Double lati = null;
        Double longi = null;
        for (Tuple3<String, String, String> x : iterable)
        {
            url = x.getField(0);

            if (x.getField(1).equals("lat") || x.getField(1).equals("latitude"))
            {
                lati = Double.parseDouble(x.getField(2));
            } else if (x.getField(1).equals("long") || x.getField(1).equals("longitude")) {
                longi = Double.parseDouble(x.getField(2));
            }
        }
        collector.collect(new GeoEntity(url, lati, longi));
    }
}

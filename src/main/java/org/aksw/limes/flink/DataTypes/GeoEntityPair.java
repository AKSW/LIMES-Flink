package org.aksw.limes.flink.DataTypes;

import org.apache.flink.api.java.tuple.Tuple2;

/**
 * @author Christopher Rost <c.rost@studserv.uni-leipzig.de>
 * @date 10.01.17
 */
public class GeoEntityPair extends Tuple2<GeoEntity, GeoEntity>
{
    public GeoEntityPair() {
    }

    public GeoEntityPair(GeoEntity geoEntityA, GeoEntity geoEntityB) {
        super(geoEntityA, geoEntityB);
    }

    public GeoEntity getFirstGeoEntity()
    {
        return this.getField(0);
    }

    public GeoEntity getSecondGeoEntity()
    {
        return this.getField(1);
    }

}

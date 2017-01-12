package org.aksw.limes.flink.Transformation;

import org.aksw.limes.flink.DataTypes.GeoEntityPair;
import org.aksw.limes.flink.DataTypes.UriPair;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * @author Christopher Rost <c.rost@studserv.uni-leipzig.de>
 * @date 10.01.17
 */
public class CreateUriPairsMap implements MapFunction<GeoEntityPair, UriPair>
{
    @Override
    public UriPair map(GeoEntityPair geoEntityPair) throws Exception {
        return new UriPair(geoEntityPair.getFirstGeoEntity().getUri(),geoEntityPair.getSecondGeoEntity().getUri());
    }
}

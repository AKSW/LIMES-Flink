package org.aksw.limes.flink.Transformation;

import org.aksw.limes.flink.DataTypes.EntitySimilarity;
import org.aksw.limes.flink.DataTypes.GeoEntityPair;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

/**
 * @author Christopher Rost <c.rost@studserv.uni-leipzig.de>
 * @date 12.01.17
 */
public class MeasureFlatMap implements FlatMapFunction<GeoEntityPair, EntitySimilarity>
{

    private double threshold = 0.d;

    public MeasureFlatMap()
    {
        super();
    }

    public MeasureFlatMap(double threshold) {
        this.threshold = threshold;
    }

    @Override
    public void flatMap(GeoEntityPair geoEntityPair, Collector<EntitySimilarity> collector) throws Exception
    {
        double sim = 0;
        double resultSim;
        double entry;

        entry = geoEntityPair.getFirstGeoEntity().getLatitude() - geoEntityPair.getSecondGeoEntity().getLatitude();
        entry = entry * entry;

        sim += entry;

        entry = geoEntityPair.getFirstGeoEntity().getLongitude() - geoEntityPair.getSecondGeoEntity().getLongitude();
        entry = entry * entry;

        sim += entry;

        resultSim = 1.0 / (1 + Math.sqrt(sim));

        if (resultSim > threshold)
        {
            collector.collect(
                    new EntitySimilarity(
                            geoEntityPair.getFirstGeoEntity().getUri(),
                            geoEntityPair.getSecondGeoEntity().getUri(),
                            resultSim
                    )
            );
        }
    }
}

package org.aksw.limes.flink.Transformation;

import org.aksw.limes.flink.DataTypes.BlockIndex;
import org.aksw.limes.flink.DataTypes.BlockIndexedGeoEntity;
import org.aksw.limes.flink.DataTypes.GeoEntity;
import org.aksw.limes.flink.Helper;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * @author Christopher Rost <c.rost@studserv.uni-leipzig.de>
 * @date 13.12.16
 */
public class BlockIndexMap implements MapFunction<GeoEntity, BlockIndexedGeoEntity>
{
    private double threshold = 1.0;
    private int granularity = 4;

    public BlockIndexMap(int granularity) {
        this.granularity = granularity;
    }

    public BlockIndexMap(double threshold, int granularity) {
        this.threshold = threshold;
        this.granularity = granularity;
    }

    @Override
    public BlockIndexedGeoEntity map(GeoEntity geoEntity) throws Exception {
        return new BlockIndexedGeoEntity(
                new BlockIndex(
                        Helper.getIndex(geoEntity.getLatitude(), granularity, threshold),
                        Helper.getIndex(geoEntity.getLongitude(), granularity, threshold)
                ),
                geoEntity
        );
    }
}

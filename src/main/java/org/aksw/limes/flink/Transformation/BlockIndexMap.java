package org.aksw.limes.flink.Transformation;

import org.aksw.limes.flink.DataTypes.BlockIndex;
import org.aksw.limes.flink.DataTypes.BlockIndexedGeoEntity;
import org.aksw.limes.flink.DataTypes.GeoEntity;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * @author Christopher Rost <c.rost@studserv.uni-leipzig.de>
 * @date 13.12.16
 */
public class BlockIndexMap implements MapFunction<GeoEntity, BlockIndexedGeoEntity> {
    double threshold = 1.0;
    int granularity = 4;

    public BlockIndexMap(int granularity) {
        this.granularity = granularity;
    }

    @Override
    public BlockIndexedGeoEntity map(GeoEntity geoEntity) throws Exception {
        int blockIdX = (int) java.lang.Math.floor((granularity * geoEntity.getLatitude()) / threshold);
        int blockIdY = (int) java.lang.Math.floor((granularity * geoEntity.getLongitude()) / threshold);

        return new BlockIndexedGeoEntity(new BlockIndex(blockIdX, blockIdY), geoEntity);
    }
}

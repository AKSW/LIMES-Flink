package org.aksw.limes.flink.DataTypes;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

/**
 * @author Christopher Rost <c.rost@studserv.uni-leipzig.de>
 * @date 13.12.16
 */
public class BlockIndexedGeoEntity extends Tuple2<BlockIndex, GeoEntity> {
    public BlockIndexedGeoEntity() {
        super();
    }

    public BlockIndexedGeoEntity(BlockIndex blockIndex, GeoEntity geoEntity) {

        super(blockIndex, geoEntity);
    }

    /**
     *
     * @return BlockIndex
     */
    public BlockIndex getBlockIndex()
    {
        return getField(0);
    }

    public GeoEntity getGeoEntity()
    {
        return getField(1);
    }
}

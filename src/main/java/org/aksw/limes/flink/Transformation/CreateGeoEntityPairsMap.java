package org.aksw.limes.flink.Transformation;

import org.aksw.limes.flink.DataTypes.BlockIndex;
import org.aksw.limes.flink.DataTypes.BlockIndexedGeoEntity;
import org.aksw.limes.flink.DataTypes.GeoEntity;
import org.aksw.limes.flink.DataTypes.GeoEntityPair;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * @author Christopher Rost <c.rost@studserv.uni-leipzig.de>
 * @date 10.01.17
 */
public class CreateGeoEntityPairsMap implements MapFunction<Tuple2<Tuple2<BlockIndexedGeoEntity,BlockIndex>,BlockIndexedGeoEntity>, GeoEntityPair>
{
    @Override
    public GeoEntityPair map(Tuple2<Tuple2<BlockIndexedGeoEntity, BlockIndex>, BlockIndexedGeoEntity> source) throws Exception {
        Tuple2<BlockIndexedGeoEntity, BlockIndex> sourceBlockEntity = source.getField(0);
        BlockIndexedGeoEntity sourceBlockedIndexGeoEntity = sourceBlockEntity.getField(0);
        GeoEntity sourceGeoEntity = sourceBlockedIndexGeoEntity.getGeoEntity();

        BlockIndexedGeoEntity targetBlockedIndexGeoEntity = source.getField(1);
        GeoEntity targetGeoEntity = targetBlockedIndexGeoEntity.getGeoEntity();
        return new GeoEntityPair(sourceGeoEntity, targetGeoEntity);
    }
}

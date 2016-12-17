package org.aksw.limes.flink.Transformation;

import org.aksw.limes.flink.DataTypes.BlockIndex;
import org.aksw.limes.flink.DataTypes.BlockIndexedGeoEntity;
import org.aksw.limes.flink.DataTypes.GeoEntity;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @author Christopher Rost <c.rost@studserv.uni-leipzig.de>
 * @date 17.12.16
 */
public class CountCompareBlocksGroupReduce implements GroupReduceFunction<Tuple2<BlockIndexedGeoEntity,BlockIndex>, Tuple2<GeoEntity,Integer>> {
    @Override
    public void reduce(Iterable<Tuple2<BlockIndexedGeoEntity, BlockIndex>> iterable, Collector<Tuple2<GeoEntity, Integer>> collector) throws Exception {
        int count = 0;
        GeoEntity z = null;

        for (Tuple2<BlockIndexedGeoEntity, BlockIndex> x : iterable)
        {
            count++;
            if (z == null)
            {
                BlockIndexedGeoEntity b = x.getField(0);
                z = b.getGeoEntity();
            }


        }
        collector.collect(new Tuple2<>(z,count));
    }
}

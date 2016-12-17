package org.aksw.limes.flink.Transformation;

import org.aksw.limes.flink.DataTypes.BlockIndex;
import org.aksw.limes.flink.DataTypes.BlockIndexedGeoEntity;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.ArrayList;

/**
 * @author Christopher Rost <c.rost@studserv.uni-leipzig.de>
 * @date 17.12.16
 */
public class GetBlocksToCompareFlatMap implements FlatMapFunction<BlockIndexedGeoEntity, Tuple2<BlockIndexedGeoEntity,BlockIndex>>
{
    int granularity = 4;

    public GetBlocksToCompareFlatMap(int granularity) {
        this.granularity = granularity;
    }

    @Override
    public void flatMap(BlockIndexedGeoEntity currentBlockIndexedGeoEntity, Collector<Tuple2<BlockIndexedGeoEntity, BlockIndex>> collector) throws Exception
    {
        int dim = 2;
        ArrayList<Integer> currentBlock = new ArrayList<>();

        //Add current block to result list
        currentBlock.add(currentBlockIndexedGeoEntity.getBlockIndex().getBlockIdX());
        currentBlock.add(currentBlockIndexedGeoEntity.getBlockIndex().getBlockIdY());

        ArrayList<ArrayList<Integer>> result = new ArrayList<ArrayList<Integer>>();
        ArrayList<ArrayList<Integer>> hr3result = new ArrayList<ArrayList<Integer>>();
        result.add(currentBlock);

        ArrayList<ArrayList<Integer>> toAdd;
        ArrayList<Integer> id;

        for (int i = 0; i < dim; i++) {
            for (int j = 0; j < Math.pow(2 * granularity + 1, i); j++) {
                //System.out.println("Result"+result);
                id = result.get(j);
                //System.out.println(j+" -> "+id);
                toAdd = new ArrayList<ArrayList<Integer>>();
                for (int k = 0; k < 2 * granularity; k++) {
                    toAdd.add(new ArrayList<Integer>());
                }
                //System.out.println(toAdd.size());
                for (int k = 0; k < dim; k++) {
                    if (k != i) {
                        for (int l = 0; l < 2 * granularity; l++) {
                            toAdd.get(l).add(id.get(k));
                        }
                    } else {
                        for (int l = 0; l < granularity; l++) {
                            toAdd.get(l).add(id.get(k) - (l + 1));
                        }
                        for (int l = 0; l < granularity; l++) {
                            toAdd.get(l + granularity).add(id.get(k) + l + 1);
                        }
                    }
                    //System.out.println(i+". "+(k+1)+". "+toAdd);
                }
                //Merge results
                for (int l = 0; l < 2 * granularity; l++) {
                    result.add(toAdd.get(l));
                }
            }
        }

        //now run hr3 check
        int alphaPowered = (int) Math.pow(granularity, dim);
        ArrayList<Integer> block;
        int hr3Index;
        int index;
        for (int i = 0; i < result.size(); i++) {
            hr3Index = 0;
            block = result.get(i);
            for (int j = 0; j < dim; j++) {
                if (block.get(j) == currentBlock.get(j)) {
                    hr3Index = 0;
                    break;
                } else {
                    index = (Math.abs(currentBlock.get(j) - block.get(j)) - 1);
                    hr3Index = hr3Index + (int) Math.pow(index, dim);
                }
            }
            if (hr3Index < alphaPowered){
                collector.collect(new Tuple2<>(currentBlockIndexedGeoEntity,new BlockIndex(block.get(0), block.get(1))));
                hr3result.add(block);
            }
        }
    }
}

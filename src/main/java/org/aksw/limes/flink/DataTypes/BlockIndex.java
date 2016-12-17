package org.aksw.limes.flink.DataTypes;

import org.apache.flink.api.java.tuple.Tuple2;

/**
 * @author Christopher Rost <c.rost@studserv.uni-leipzig.de>
 * @date 17.12.16
 */
public class BlockIndex extends Tuple2<Integer, Integer>
{
    public BlockIndex() {
    }

    public BlockIndex(Integer blockIdX, Integer blockIdY) {
        super(blockIdX, blockIdY);
    }

    public int getBlockIdX()
    {
        return getField(0);
    }

    public int getBlockIdY()
    {
        return getField(1);
    }
}

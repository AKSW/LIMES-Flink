package org.aksw.limes.flink.Transformation;

import junit.framework.TestCase;
import org.aksw.limes.flink.DataTypes.BlockIndex;
import org.aksw.limes.flink.DataTypes.BlockIndexedGeoEntity;
import org.aksw.limes.flink.DataTypes.GeoEntity;
import org.aksw.limes.flink.Helper;
import org.aksw.limes.flink.Importer;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;

import java.util.List;

/**
 * Test class for BlockIndexMap transformation class
 *
 * @author Christopher Rost <c.rost@studserv.uni-leipzig.de>
 * @date 26.12.16
 */
public class BlockIndexMapTest extends TestCase{

    private ExecutionEnvironment _environment;
    private int _granularity;
    private double _threshold;

    /**
     * Set up test class
     *
     * @throws Exception
     */
    public void setUp() throws Exception {
        super.setUp();
        _environment = ExecutionEnvironment.getExecutionEnvironment();
    }

    public void testBlockIndexMap() throws Exception
    {
        _granularity = 4;
        _threshold = 1.0;

        DataSet<GeoEntity> geoEntityDataSet = _environment.fromElements(
                new GeoEntity("url1", 66.66, 77.77),
                new GeoEntity("url2", 88.88, 99.99)
        );

        List<BlockIndexedGeoEntity> list = geoEntityDataSet.map(new BlockIndexMap(_granularity)).collect();

        assertEquals(2,list.size());

        for (BlockIndexedGeoEntity blockedGeoEntity : list)
        {
            assertNotNull(blockedGeoEntity.getBlockIndex());
            switch (blockedGeoEntity.getGeoEntity().getUri())
            {
                case "url1":
                    assertEquals(blockedGeoEntity.getBlockIndex().getBlockIdX(), Helper.getIndex(66.66, _granularity, _threshold));
                    assertEquals(blockedGeoEntity.getBlockIndex().getBlockIdY(), Helper.getIndex(77.77, _granularity, _threshold));
                    break;
                case "url2":
                    assertEquals(blockedGeoEntity.getBlockIndex().getBlockIdX(), Helper.getIndex(88.88, _granularity, _threshold));
                    assertEquals(blockedGeoEntity.getBlockIndex().getBlockIdY(), Helper.getIndex(99.99, _granularity, _threshold));
                    break;
            }
        }
    }
}

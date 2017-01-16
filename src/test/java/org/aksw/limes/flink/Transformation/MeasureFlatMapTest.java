package org.aksw.limes.flink.Transformation;

import junit.framework.TestCase;
import org.aksw.limes.flink.DataTypes.EntitySimilarity;
import org.aksw.limes.flink.DataTypes.GeoEntity;
import org.aksw.limes.flink.DataTypes.GeoEntityPair;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

/**
 * @author Christopher Rost <c.rost@studserv.uni-leipzig.de>
 * @date 15.01.17
 */
public class MeasureFlatMapTest extends TestCase
{
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

    public void testMeasureMap() throws Exception
    {
        GeoEntityPair geoEntityPair1 = new GeoEntityPair(
                new GeoEntity(
                       "url1_1",
                        51.968,
                        4.61185
                ),
                new GeoEntity(
                        "url1_2",
                        50.7894,
                        4.58331
                )
        );
        GeoEntityPair geoEntityPair2 = new GeoEntityPair(
                new GeoEntity(
                        "url2_1",
                        51.968,
                        4.61185
                ),
                new GeoEntity(
                        "url2_2",
                        51.9392,
                        5.23364
                )
        );
        DataSet<GeoEntityPair> dataSet= _environment.fromElements(geoEntityPair1,geoEntityPair2);

        DataSet<EntitySimilarity> resultDataSet = dataSet.flatMap(new MeasureFlatMap());

        for (EntitySimilarity entitySimilarity : resultDataSet.collect())
        {
            switch (entitySimilarity.getUri1())
            {
                case "url1_1":
                    assertEquals("url1_2", entitySimilarity.getUri2());
                    assertEquals(0.45893759, entitySimilarity.getSimilarity(), 0.000001);
                    break;
                case "url2_1":
                    assertEquals("url2_2", entitySimilarity.getUri2());
                    assertEquals(0.6163, entitySimilarity.getSimilarity(), 0.0001);
                    break;
            }
        }
    }
}

package org.aksw.limes.flink.DataTypes;

import org.apache.flink.api.java.tuple.Tuple3;

/**
 * @author Christopher Rost <c.rost@studserv.uni-leipzig.de>
 * @date 12.01.17
 */
public class EntitySimilarity extends Tuple3<String, String, Double>
{
    public EntitySimilarity() {
    }

    public EntitySimilarity(String uri1, String uri2, Double similarity) {
        super(uri1, uri2, similarity);
    }

    public String getUri1()
    {
        return this.getField(0);
    }

    public String getUri2()
    {
        return this.getField(1);
    }

    public double getSimilarity()
    {
        return (double) this.getField(2);
    }

}

package org.aksw.limes.flink.DataTypes;

import org.apache.flink.api.java.tuple.Tuple2;

/**
 * @author Christopher Rost <c.rost@studserv.uni-leipzig.de>
 * @date 10.01.17
 */
public class UriPair extends Tuple2<String, String>
{
    public UriPair() {
    }

    public UriPair(String uri1, String uri2) {
        super(uri1, uri2);
    }


}

package org.aksw.limes.flink;

import java.util.Base64;

/**
 * @author Christopher Rost <c.rost@studserv.uni-leipzig.de>
 * @date 26.11.16
 */
public class Helper {

    public static String decodeB64(String stringToDecode)
    {
        return new String(Base64.getDecoder().decode(stringToDecode));
    }
}

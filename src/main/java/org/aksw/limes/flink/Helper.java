package org.aksw.limes.flink;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.security.MessageDigest;
import java.util.Base64;

/**
 * @author Christopher Rost <c.rost@studserv.uni-leipzig.de>
 * @date 26.11.16
 */
public class Helper {

    /**
     * Checks if a file with the desired file name already exists. If the file exists, the name will be extended by a number.
     * E.g. file name myOwnFile.csv exists --> myOwnFile1.csv will be returned
     *
     * @param desiredName the desired file name inclusive the path and extension
     * @param extension the file name extension in format ".csv"
     * @return a unique file name that not exist
     */
    public static String getUniqueFilename(String desiredName, String extension)
    {
        String prefix = desiredName.split(extension)[0];
        String uniqueName = desiredName;
        File outputFile = new File(desiredName);
        int i = 0;
        while (outputFile.exists()) {
            i++;
            uniqueName = prefix + Integer.toString(i) + extension;
            outputFile = new File(uniqueName);
        }
        return uniqueName;
    }

    /**
     * Calculates int index of given coordinate as Double
     *
     * @param coordinate the coordinate as double
     * @param granularity
     * @param threshold
     * @return the index as int
     */
    public static int getIndex(Double coordinate, int granularity, double threshold)
    {
        return (int) java.lang.Math.floor((granularity * coordinate) / threshold);
    }



}

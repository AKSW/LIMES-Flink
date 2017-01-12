package org.aksw.limes.flink.Jobs;

import org.aksw.limes.flink.Common.Configuration;
import org.aksw.limes.flink.DataTypes.GeoEntity;
import org.aksw.limes.flink.Importer;
import org.aksw.limes.flink.Transformation.UriLatLongGroupReduce;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.log4j.Logger;

import java.io.File;
import java.security.MessageDigest;

/**
 * @author Christopher Rost <c.rost@studserv.uni-leipzig.de>
 * @date 10.01.17
 */
public class CreateCacheFiles
{
    private Configuration _config;
    private ExecutionEnvironment _executionEnv;

    public CreateCacheFiles(Configuration config)
    {
        this._config = config;
    }
    public static void main(Configuration config) throws Exception
    {
        CreateCacheFiles createCacheFilesJob = new CreateCacheFiles(config);
        createCacheFilesJob.runJob();
    }

    public void runJob() throws Exception
    {
        this._executionEnv = ExecutionEnvironment.getExecutionEnvironment();

        if (!isCached(this._config.getSourceFile()))
        {
            System.out.println("Creating cache file for input file: " + this._config.getSourceFile());
            createCache(this._config.getSourceFile());
            this._executionEnv.execute("CreateSourceCacheFiles");
        }
        if (!isCached(this._config.getTargetFile()))
        {
            System.out.println("Creating cache file for input file: " + this._config.getTargetFile());
            createCache(this._config.getTargetFile());
            this._executionEnv.execute("CreateTargetCacheFiles");
        }

    }

    /**
     * Get Geo Entity dataset from cache or create cache csv if not exists
     *
     * @param fileName
     * @return
     * @throws Exception
     */
    private void createCache(String fileName) throws Exception
    {
        String workingDir = System.getProperty("user.dir");
        MessageDigest messageDigest = MessageDigest.getInstance("MD5");
        messageDigest.update(fileName.getBytes());
        String encryptedString = Integer.toString(new String(messageDigest.digest()).hashCode());

        Importer importer = new Importer(this._executionEnv);
        importer.getRdfDataSetFromCsv(fileName)
                .groupBy(0)
                .reduceGroup(new UriLatLongGroupReduce())
                .writeAsCsv("file:///" + workingDir + "/cache/" + encryptedString + ".csv","\n",importer.CSV_DELIMITER_COMMA);
    }

    private boolean isCached(String fileName) throws Exception
    {
        MessageDigest messageDigest = MessageDigest.getInstance("MD5");
        messageDigest.update(fileName.getBytes());
        String encryptedString = Integer.toString(new String(messageDigest.digest()).hashCode());

        String cacheFilePath = "cache/" + encryptedString + ".csv";
        File f = new File(cacheFilePath);
        return (f.exists() && !f.isDirectory());
    }
}

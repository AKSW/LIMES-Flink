package org.aksw.limes.flink.Jobs;

import org.aksw.limes.flink.Common.Configuration;
import org.aksw.limes.flink.DataTypes.*;
import org.aksw.limes.flink.Helper;
import org.aksw.limes.flink.Importer;
import org.aksw.limes.flink.Transformation.*;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.JoinOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.MessageDigest;

/**
 * @author Christopher Rost <c.rost@studserv.uni-leipzig.de>
 * @date 10.01.17
 */

public class CreateMapping
{
    private static final Logger logger = LoggerFactory.getLogger(CreateMapping.class.getName());

    private Configuration _config;
    private ExecutionEnvironment _executionEnv;

    public CreateMapping(Configuration config)
    {
        this._config = config;
    }

    public static void main(Configuration config) throws Exception
    {
        CreateMapping createMappingJob = new CreateMapping(config);
        createMappingJob.runJob();
    }

    public void runJob() throws Exception
    {
        this._executionEnv = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<GeoEntity> sourceGeoEntityDs = _getFromCache(this._config.getSourceFile());
        DataSet<GeoEntity> targetGeoEntityDs = _getFromCache(this._config.getTargetFile());

        logger.info("Without HR3 there are " + Long.toString(sourceGeoEntityDs.count() * targetGeoEntityDs.count()) +
                " pairs to compare.");

        DataSet<BlockIndexedGeoEntity> sourceBlockIndex = sourceGeoEntityDs.map(new BlockIndexMap(_config.getGranularity()));
        DataSet<BlockIndexedGeoEntity> targetBlockIndex = targetGeoEntityDs.map(new BlockIndexMap(_config.getGranularity()));

        DataSet<Tuple2<BlockIndexedGeoEntity,BlockIndex>> blocksToCompareDs = sourceBlockIndex
                .flatMap(new GetBlocksToCompareFlatMap(_config.getGranularity()));

        JoinOperator.DefaultJoin<Tuple2<BlockIndexedGeoEntity, BlockIndex>, BlockIndexedGeoEntity> joined = blocksToCompareDs
                .join(targetBlockIndex)
                .where(1)
                .equalTo(0);

        DataSet<GeoEntityPair> geoEntityPair = joined.map(new CreateGeoEntityPairsMap());

        DataSet<EntitySimilarity> uriPairsWithSimilarity = geoEntityPair.flatMap(new MeasureFlatMap(_config.getSimThreshold()));

        uriPairsWithSimilarity.writeAsCsv("file:///" + Helper.getUniqueFilename(this._config.getOutputFile(), ".csv"),"\n",";");

        //blocksToCompareDs.groupBy(0).reduceGroup(new CountCompareBlocksGroupReduce()).print();

        this._executionEnv.execute("CreateComparePairs");
        logger.info("Finished mapping task. [" + uriPairsWithSimilarity.count() + "] pairs to compare were detected.");
    }

    /**
     * Get Geo Entity dataset from cache or create cache csv if not exists
     *
     * @param fileName
     * @return
     * @throws Exception
     */
    private DataSet<GeoEntity> _getFromCache(String fileName) throws Exception
    {
        MessageDigest messageDigest = MessageDigest.getInstance("MD5");
        messageDigest.update(fileName.getBytes());
        String encryptedString = Integer.toString(new String(messageDigest.digest()).hashCode());
        String cacheFilePath = "cache/" + encryptedString + ".csv";
        Importer importer = new Importer(this._executionEnv);
        logger.info("Load cache file: " + cacheFilePath);
        return importer.getGeoEntityDataSetFromCsv(cacheFilePath);
    }
}

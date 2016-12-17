package org.aksw.limes.flink;

import org.aksw.limes.flink.DataTypes.BlockIndex;
import org.aksw.limes.flink.DataTypes.BlockIndexedGeoEntity;
import org.aksw.limes.flink.DataTypes.GeoEntity;
import org.aksw.limes.flink.Exception.InvalidArgumentException;
import org.aksw.limes.flink.Transformation.BlockIndexMap;
import org.aksw.limes.flink.Transformation.CountCompareBlocksGroupReduce;
import org.aksw.limes.flink.Transformation.GetBlocksToCompareFlatMap;
import org.aksw.limes.flink.Transformation.UriLatLongGroupReduce;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.util.Collector;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;

/**
 * Main Job class with main method
 *
 * @author Christopher Rost (c.rost@studserv.uni-leipzig.de)
 * @version Nov. 26 2016
 */
public class MainJob {

    private static final String sourceCsvArg = "sourceCsv";
    private static final String targetCsvArg = "targetCsv";
    private static final String resultCsvArg = "resultCsv";

    public static final int granularity = 4;

    /**
     * Main method will be executed at program start
     *
     * @param args array of aguments
     * @throws Exception
     */
    public static void main(String[] args) throws Exception
    {
        ParameterTool parameters = ParameterTool.fromArgs(args);

        try {
            validateParameters(parameters);
        } catch (InvalidArgumentException | FileNotFoundException e) {
            System.out.println(e.getMessage());
            printSyntaxDocumentation();
            return;
        }
        System.out.println(parameters.get(sourceCsvArg));
        System.out.println(parameters.get(targetCsvArg));
        System.out.println(parameters.get(resultCsvArg));

        run(parameters);
    }

    private static void run(ParameterTool parameters) throws Exception
    {
        // set up the execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        Importer importer = new Importer(env);

        DataSet<Tuple3<String,String,String>> rdfSourceDataSet = importer.getRdfDataSetFromCsv(parameters.get(sourceCsvArg));
        DataSet<Tuple3<String,String,String>> rdfTargetDataSet = importer.getRdfDataSetFromCsv(parameters.get(targetCsvArg));

        DataSet<GeoEntity> groupedRdfSourceDs = rdfSourceDataSet
                .groupBy(0)
                .reduceGroup(new UriLatLongGroupReduce());

        DataSet<GeoEntity> groupedRdfTargetDs = rdfTargetDataSet
                .groupBy(0)
                .reduceGroup(new UriLatLongGroupReduce());

        DataSet<BlockIndexedGeoEntity> sourceBlockIndex = groupedRdfSourceDs.map(new BlockIndexMap(granularity));
        DataSet<BlockIndexedGeoEntity> targetBlockIndex = groupedRdfTargetDs.map(new BlockIndexMap(granularity));

        DataSet<Tuple2<BlockIndexedGeoEntity,BlockIndex>> blocksToCompareDs;

        blocksToCompareDs = sourceBlockIndex
                .flatMap(new GetBlocksToCompareFlatMap(granularity));

        blocksToCompareDs.print();

        blocksToCompareDs.groupBy(0).reduceGroup(new CountCompareBlocksGroupReduce()).print();

        System.out.println("done");
    }

    /**
     * Print syntax documentation to console output
     */
    private static void printSyntaxDocumentation()
    {
        System.out.println("Please use the syntax given in the gitHub description: https://github.com/AKSW/LIMES-Flink");
        System.out.println(
                "Example parameters: --" + sourceCsvArg + " \"path/to/source.csv\" --" + targetCsvArg +
                        " \"path/to/target.csv\" --" + resultCsvArg + " \"path/to/result.csv\"");
    }

    /**
     * Validate given parameters
     *
     * @param parameters
     * @throws InvalidArgumentException
     * @throws FileNotFoundException
     */
    private static void validateParameters(ParameterTool parameters) throws InvalidArgumentException, FileNotFoundException
    {
        // Check if parameters were given
        if(parameters.getNumberOfParameters() < 1){
            throw new InvalidArgumentException("No arguments given");
        }

        // Check if necessary parameters are set
        if (!parameters.has(sourceCsvArg) || !parameters.has(targetCsvArg) || !parameters.has(resultCsvArg))
        {
            throw new InvalidArgumentException("One or more necessary arguments missing.");
        }

        // Check if source and target csv files exist
        String sourceCsvFilePath = parameters.get(sourceCsvArg, "");
        File sourceCsvFile = new File(sourceCsvFilePath);

        if(!sourceCsvFile.exists()) {
            throw new FileNotFoundException("File " + sourceCsvFilePath + " not found.");
        }

        String targetCsvFilePath = parameters.get(targetCsvArg, "");
        File targetCsvFile = new File(targetCsvFilePath);

        if(!targetCsvFile.exists()) {
            throw new FileNotFoundException("File " + targetCsvFilePath + " not found.");
        }
    }
}

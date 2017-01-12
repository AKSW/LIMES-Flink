package org.aksw.limes.flink;

import org.aksw.limes.flink.DataTypes.GeoEntity;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;

import java.io.File;

/**
 * @author Christopher Rost <c.rost@studserv.uni-leipzig.de>
 * @date 26.11.16
 */
public class Importer {

    private ExecutionEnvironment env;

    public final String CSV_DELIMITER_QM = "~¿~";
    public final String CSV_DELIMITER_COMMA = ",";

    public Importer(ExecutionEnvironment env) {
        this.env = env;
    }

    /**
     * Imports the *.csv from the given path.
     *
     * @param pathToCsv path to *.csv file
     * @return DataSet<Tuple4> the csv representation as flink dataset
     */
    public DataSet<Tuple3<String,String,String>> getRdfDataSetFromCsv(String pathToCsv){
        File csvFile = new File(pathToCsv);

        return this.env.readCsvFile(csvFile.toString())
                .ignoreFirstLine()
                .includeFields("111")
                .fieldDelimiter(CSV_DELIMITER_QM)
                .ignoreInvalidLines()
                .types(String.class,String.class,String.class);
    }

    public DataSet<GeoEntity> getGeoEntityDataSetFromCsv(String pathToCsv)
    {
        File csvFile = new File(pathToCsv);
        DataSet<Tuple3<String,Double,Double>> geoEntityTuple3 = this.env.readCsvFile(csvFile.toString())
                .ignoreFirstLine()
                .includeFields("111")
                .fieldDelimiter(CSV_DELIMITER_COMMA)
                .ignoreInvalidLines()
                .types(String.class,Double.class,Double.class);
        return geoEntityTuple3.map(new MapFunction<Tuple3<String,Double,Double>, GeoEntity>() {
            @Override
            public GeoEntity map(Tuple3<String, Double, Double> input) throws Exception {
                return new GeoEntity(input);
            }
        });
    }
}

package org.aksw.limes.flink;

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

    private final String CSV_DELIMITER = "~¿~";

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
        File conceptAttrFile = new File(pathToCsv);

        //Get concept_attributes.csv
        return this.env.readCsvFile(conceptAttrFile.toString())
                .ignoreFirstLine()
                .includeFields("111")
                .fieldDelimiter(CSV_DELIMITER)
                .ignoreInvalidLines()
                .types(String.class,String.class,String.class);
    }
}

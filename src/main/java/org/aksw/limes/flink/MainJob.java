package org.aksw.limes.flink;

import org.aksw.limes.flink.Common.Configuration;
import org.aksw.limes.flink.Exception.InvalidArgumentException;
import org.aksw.limes.flink.Jobs.CreateCacheFiles;
import org.aksw.limes.flink.Jobs.CreateMapping;
import org.apache.flink.api.java.utils.ParameterTool;

import java.io.File;
import java.io.FileNotFoundException;

/**
 * Main Job class with main method
 *
 * @author Christopher Rost (c.rost@studserv.uni-leipzig.de)
 * @version Nov. 26 2016
 */
public class MainJob {

    private static final String ARG_CONFIG = "config";
    private static final String ARG_CREATE_CACHE = "createcache";

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
        Configuration configuration = Configuration.readJsonConfigFromFile(parameters.getRequired(ARG_CONFIG));

        if (parameters.has(ARG_CREATE_CACHE))
        {
            CreateCacheFiles.main(configuration);
        } else {
            CreateMapping.main(configuration);
        }
    }

    /**
     * Print syntax documentation to console output
     */
    private static void printSyntaxDocumentation()
    {
        System.out.println("Please use the syntax given in the gitHub description: https://github.com/AKSW/LIMES-Flink");
        System.out.println(
                "Example parameters: --" + ARG_CONFIG + " \"path/to/config.json\" [--createcache]");
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
        if (!parameters.has(ARG_CONFIG))
        {
            throw new InvalidArgumentException("One or more necessary arguments missing.");
        }

        // Check if config files exist
        String configFilePath = parameters.get(ARG_CONFIG, "");
        File configFile = new File(configFilePath);

        if(!configFile.exists()) {
            throw new FileNotFoundException("File " + configFilePath + " not found.");
        }
    }
}

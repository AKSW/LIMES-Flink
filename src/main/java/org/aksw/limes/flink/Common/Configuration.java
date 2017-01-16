package org.aksw.limes.flink.Common;

import org.aksw.limes.flink.Exception.ConfigurationException;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

/**
 * @author Christopher Rost <c.rost@studserv.uni-leipzig.de>
 * @date 07.01.17
 */

public class Configuration
{
    private static final String SOURCE_FILE = "sourceFile";
    private static final String SOURCE_TYPE = "sourceType";
    private static final String SOURCE_IS_ONE_ENTITY_PER_LINE = "sourceIsOneEntityPerLine";
    private static final String TARGET_FILE = "targetFile";
    private static final String TARGET_TYPE = "targetType";
    private static final String TARGET_IS_ONE_ENTITY_PER_LINE = "targetIsOneEntityPerLine";
    private static final String OUTPUT_FILE = "outputFile";
    private static final String GRANULARITY = "granularity";
    private static final String SIM_THRESHOLD = "simThreshold";

    private String sourceFile, sourceType, targetFile, targetType, outputFile;
    private int granularity = 4;
    private double simThreshold = 0.d;
    private boolean sourceIsOneEntityPerLine, targetIsOneEntityPerLine;

    /**
     * Constructor for Config
     * @param sourceFile path to source file
     * @param sourceIsOneEntityPerLine  true if one line is in format: uri latitude-value longitude-value
     *                                  false if one entity is described in two lines:
     *                                  (1) uri 'latitude' latitude-value
     *                                  (2) uri 'longitude' longitude-value
     * @param targetFile path to target file
     * @param targetIsOneEntityPerLine  true if one line is in format: uri latitude-value longitude-value
     *                                  false if one entity is described in two lines:
     *                                  (1) uri 'latitude' latitude-value
     *                                  (2) uri 'longitude' longitude-value
     */
    public Configuration(
            String sourceFile,
            boolean sourceIsOneEntityPerLine,
            String targetFile,
            boolean targetIsOneEntityPerLine
    ) {
        this.sourceFile = sourceFile;
        this.sourceIsOneEntityPerLine = sourceIsOneEntityPerLine;
        this.targetFile = targetFile;
        this.targetIsOneEntityPerLine = targetIsOneEntityPerLine;
        this.sourceType = this.targetType = "csv";
    }

    /**
     * Reads json configuration file from given path and returns configuration object
     *
     * @param path to configuration json file
     * @return Configuration object
     */
    public static Configuration readJsonConfigFromFile(String path) throws ConfigurationException
    {

        try {
            BufferedReader br = new BufferedReader(new FileReader(path));
            StringBuilder sb = new StringBuilder();
            String line = br.readLine();

            while (line != null)
            {
                sb.append(line);
                sb.append(System.lineSeparator());
                line = br.readLine();
            }
            String jsonString = sb.toString();
            br.close();

            JSONObject configFileObject = new JSONObject(jsonString);

            String sourceFile = configFileObject.getString(SOURCE_FILE);
            boolean sourceIsOneEntityPerLine = configFileObject.getBoolean(SOURCE_IS_ONE_ENTITY_PER_LINE);
            String sourceType = configFileObject.getString(SOURCE_TYPE);
            String targetFile = configFileObject.getString(TARGET_FILE);
            boolean targetIsOneEntityPerLine = configFileObject.getBoolean(TARGET_IS_ONE_ENTITY_PER_LINE);
            String targetType = configFileObject.getString(TARGET_TYPE);
            String outputFile = configFileObject.getString(OUTPUT_FILE);
            String granularity = configFileObject.getString(GRANULARITY);
            String simThreshold = configFileObject.getString(SIM_THRESHOLD);

            Configuration config = new Configuration(
                    sourceFile,
                    sourceIsOneEntityPerLine,
                    targetFile,
                    targetIsOneEntityPerLine
            );

            if (sourceType != null)
            {
                config.setSourceType(sourceType);
            }

            if (targetType != null)
            {
                config.setTargetType(targetType);
            }

            if (outputFile != null)
            {
                config.setOutputFile(outputFile);
            }

            if (granularity != null)
            {
                config.setGranularity(Integer.parseInt(granularity));
            }

            if (simThreshold != null)
            {
                config.setSimThreshold(Double.parseDouble(simThreshold));
            }

            return config;
        } catch (FileNotFoundException fileNotFoundException) {
            throw new ConfigurationException("File [" + path + "] not found!", fileNotFoundException);
        } catch (IOException ioException) {
            throw new ConfigurationException("Can not read file [" + path + "]!", ioException);
        }
    }

    public String getSourceFile() {
        return sourceFile;
    }

    public Configuration setSourceFile(String sourceFile) {
        this.sourceFile = sourceFile;
        return this;
    }

    public String getTargetFile() {
        return targetFile;
    }

    public Configuration setTargetFile(String targetFile) {
        this.targetFile = targetFile;
        return this;
    }

    public String getOutputFile() {
        return outputFile;
    }

    public Configuration setOutputFile(String outputFile) {
        this.outputFile = outputFile;
        return this;
    }

    public boolean isSourceIsOneEntityPerLine() {
        return sourceIsOneEntityPerLine;
    }

    public Configuration setSourceIsOneEntityPerLine(boolean sourceIsOneEntityPerLine) {
        this.sourceIsOneEntityPerLine = sourceIsOneEntityPerLine;
        return this;
    }

    public boolean isTargetIsOneEntityPerLine() {
        return targetIsOneEntityPerLine;
    }

    public Configuration setTargetIsOneEntityPerLine(boolean targetIsOneEntityPerLine) {
        this.targetIsOneEntityPerLine = targetIsOneEntityPerLine;
        return this;
    }

    public String getSourceType() {
        return sourceType;
    }

    public Configuration setSourceType(String sourceType) {
        this.sourceType = sourceType;
        return this;
    }

    public String getTargetType() {
        return targetType;
    }

    public Configuration setTargetType(String targetType) {
        this.targetType = targetType;
        return this;
    }

    public int getGranularity() {
        return granularity;
    }

    public Configuration setGranularity(int granularity) {
        this.granularity = granularity;
        return this;
    }

    public double getSimThreshold() {
        return simThreshold;
    }

    public Configuration setSimThreshold(double simThreshold) {
        this.simThreshold = simThreshold;
        return this;
    }
}

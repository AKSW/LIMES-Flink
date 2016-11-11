package org.aksw.limes.flink;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.utils.ParameterTool;

public class MainJob {

    public static void main(String[] args) throws Exception
    {
        ParameterTool parameters = ParameterTool.fromArgs(args);

        if(parameters.getNumberOfParameters() < 1){
            printSyntaxDocumentation();
            return;
        }

        switch (parameters.get("process", "none")){
            case "default":
                try {
                    runDefault(parameters);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                break;
            default:
                printSyntaxDocumentation();
        }
    }

    private static void runDefault(ParameterTool parameters) throws Exception
    {
        // set up the execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        System.out.println("done");
    }

    private static void printSyntaxDocumentation()
    {
        System.out.println("Please use the syntax given in the gitHub description: https://github.com/AKSW/LIMES-Flink");
    }
}

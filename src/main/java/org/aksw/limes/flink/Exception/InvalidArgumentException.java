package org.aksw.limes.flink.Exception;

/**
 * Exception class for invalid program arguments.
 *
 * @author Christopher Rost (c.rost@studserv.uni-leipzig.de)
 * @version Nov. 26 2016
 */
public class InvalidArgumentException extends Exception {

    /**
     * Std. constructor
     */
    public InvalidArgumentException() {
        super();
    }

    /**
     * Constructor with failure message
     *
     * @param message
     */
    public InvalidArgumentException(String message) {
        super(message);
    }

}

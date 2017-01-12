package org.aksw.limes.flink.Exception;

/**
 * @author Christopher Rost <c.rost@studserv.uni-leipzig.de>
 * @date 07.01.17
 */
public class ConfigurationException extends Exception
{
    public ConfigurationException()
    {
        super();
    }

    public ConfigurationException(String message)
    {
        super(message);
    }

    public ConfigurationException(String message, Throwable cause)
    {
        super(message, cause);
    }
}

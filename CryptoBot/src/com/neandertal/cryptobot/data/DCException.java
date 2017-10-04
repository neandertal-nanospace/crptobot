package com.neandertal.cryptobot.data;

public class DCException extends Exception
{
    private static final long serialVersionUID = 1796766789007590L;

    public DCException(String message, Throwable cause)
    {
        super(message, cause);
    }

    public DCException(String message)
    {
        super(message);
    }
  
}

package com.neandertal.cryptobot.data;

import java.util.ArrayList;
import java.util.List;

public class DataCollectionHeaders
{
    public static enum TYPE
    {
        DATE,
        DOUBLE,
        LONG
    }
    
    private List<Header> headers = new ArrayList<>();
    private int dateHeaderIndex = -1;
    
    public void addHeader(String label, TYPE type)
    {
        Header h = new Header();
        h.label = label;
        h.type = type;
        headers.add(h);
        
        if (TYPE.DATE == type)
        {
            dateHeaderIndex = headers.size() - 1;
        }
    }
    
    public String getHeaderLabel(int index)
    {
        return headers.get(index).label;
    }
    
    public TYPE getHeaderType(int index)
    {
        return headers.get(index).type;
    }
    
    public int getDateHeaderIndex()
    {
        return dateHeaderIndex;
    }
    
    private class Header
    {
        public String label;
        public TYPE type;
        
        @Override
        public String toString()
        {
            StringBuilder sb = new StringBuilder();
            sb.append(label).append("[").append(type.name()).append("]");
            return sb.toString();
        }
    }
}

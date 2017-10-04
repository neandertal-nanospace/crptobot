package com.neandertal.cryptobot.data;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class DataCollectionRow implements Comparable<DataCollectionRow>
{
    private DataCollectionHeaders headers;
    private List<Number> values = new ArrayList<>();

    public DataCollectionRow(DataCollectionHeaders headers)
    {
        this.headers = headers;
    }
    
    public void addValue(Number d)
    {
        values.add(d);
    }
    
    public Number getValue(int i)
    {
        return values.get(i);
    }
    
    @Override
    public int compareTo(DataCollectionRow row)
    {
        int i = headers.getDateHeaderIndex();
        
        Date date = new Date(values.get(i).longValue());
        Date rowDate = new Date(row.values.get(i).longValue());
        return -date.compareTo(rowDate);
    }
}
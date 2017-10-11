package com.neandertal.cryptobot.data;

import java.sql.Date;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

public class DataColumn
{
    private static SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd", Locale.ENGLISH);
    private static NumberFormat doubleFormat = NumberFormat.getInstance(Locale.ENGLISH);
    private static NumberFormat longFormat = NumberFormat.getIntegerInstance(Locale.ENGLISH);
    static {
        doubleFormat.setGroupingUsed(false);
        longFormat.setGroupingUsed(false);
    }
    private String header;
    private Class<?> type;
    private List<Object> values = new ArrayList<>();
    
    public DataColumn(String header, Class<?> type)
    {
        if (header == null || header.isEmpty())
        {
            throw new IllegalArgumentException("Header is empty!");
        }
        
        if (type == null)
        {
            throw new IllegalArgumentException("Type is empty!");
        }
        
        this.header = header;
        this.type = type;
    }
    
    public String getHeader()
    {
        return header;
    }
    
    public String getHeaderAndType()
    {
        return header + "[" + type.getSimpleName() + "]";
    }
    
    public Class<?> getType()
    {
        return type;
    }
    
    public void addValue(Object value, int index)
    {
        if (value != null && !type.isInstance(value))
        {
            throw new IllegalArgumentException("Value is of wrong type, expected: " + type.getSimpleName() + " but got: " + value.getClass().getSimpleName());
        }
        
        values.add(index, value);
    }
    
    public void addValue(Object value)
    {
        if (value != null && !type.isInstance(value))
        {
            throw new IllegalArgumentException("Value is of wrong type, expected: " + type.getSimpleName() + " but got: " + value.getClass().getSimpleName());
        }
        
        values.add(value);
    }
    
    public Object getValue(int index)
    {
        return values.get(index);
    }
    
    public int getValuesCount()
    {
        return values.size();
    }
    
    public String getValueAsString(int index)
    {
        if (values.get(index) == null)
        {
            return "-";
        }
        
        if (Date.class.equals(type))
        {
            return dateFormatter.format((Date) values.get(index)); 
        }
        
        if (Long.class.equals(type))
        {
            return longFormat.format((Long) values.get(index)); 
        }
        
        if (Double.class.equals(type))
        {
            return doubleFormat.format((Double) values.get(index));
        }
        
        throw new IllegalArgumentException("Unsupported type for column: " + getHeader() + " : " + getType().getSimpleName());
    }
}

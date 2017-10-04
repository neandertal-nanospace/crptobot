package com.neandertal.cryptobot.data;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class DataCollectionInfo
{
    private static final String INFO_LINE_START = "#";
    private static final String INFO_LABEL_DELIM = ":";
    
    public static enum ENTRY
    {
        SOURCE, EXCHANGE, FROM_CURRENCY, TO_CURRENCY, START, END, RECORDS;
    }
    
    private Map<ENTRY, String> info = new HashMap<>();
    
    public String get(ENTRY entry)
    {
        return info.get(entry);
    }
    
    public void set(ENTRY entry, String val)
    {
        info.put(entry, val);
    }
    
    private void checkAll() throws DCException
    {
        for (ENTRY entry: ENTRY.values())
        {
            if (!info.containsKey(entry))
            {
                throw new DCException("Missing entry: " + entry.name());
            }
        }
    }
    
    public void fillFrom(Stream<String> stream) throws DCException
    {
        stream.filter(line -> line.startsWith(INFO_LINE_START))
              .collect(Collectors.toList())
              .forEach((s) -> {
                  s = s.substring(INFO_LINE_START.length()).trim();
                  int ind = s.indexOf(INFO_LABEL_DELIM);
                  if (ind < 0)
                  {
                      new DCException("Wrong info line: " + s);
                  }
                  
                  String label = s.substring(0, ind).trim();
                  String value = (s.length() > ind + 1) ? s.substring(ind + 1).trim() : "";

                  ENTRY entry = null;
                  try
                  {
                      entry = ENTRY.valueOf(label.toUpperCase());
                      set(entry, value);
                  }
                  catch (Exception e)
                  {
                      new DCException("Unknown info label found in file: " + label, e);
                  }
              });
        
        checkAll();
    }
    
    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        
        sb.append(getInfoLine(ENTRY.SOURCE));
        sb.append(getInfoLine(ENTRY.EXCHANGE));
        sb.append(getInfoLine(ENTRY.FROM_CURRENCY));
        sb.append(getInfoLine(ENTRY.TO_CURRENCY));
        sb.append(getInfoLine(ENTRY.START));
        sb.append(getInfoLine(ENTRY.END));
        sb.append(getInfoLine(ENTRY.RECORDS));
        
        return sb.toString();
    }
    
    private String getInfoLine(ENTRY e)
    {
        return INFO_LINE_START + " " + e.name() + INFO_LABEL_DELIM + " " + info.get(e) + "\n";
    }
}

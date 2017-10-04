package com.neandertal.cryptobot.data;

import java.util.ArrayList;
import java.util.List;

import com.neandertal.cryptobot.data.DatasetTransformer.META;

public class DataCollection
{
    private static final String SEPARATOR = ",";
    private int maxColumnLength = 0;
    
    private String source = null;
    private DataCollectionInfo info = new DataCollectionInfo();
    private DataCollectionHeaders headers = new DataCollectionHeaders();
    private List<DataCollectionRow> rows = new ArrayList<>();

    public List<String> getColumnValues(String columnHeader)
    {
        int ind = headers.indexOf(columnHeader);
        List<String> colValList = new ArrayList<>(rows.size());
        for (DataRow row: rows)
        {
            colValList.add(row.values.get(ind));
        }
        return colValList;
    }
    
/*    public boolean prepareForSave()
    {
        if (checkMeta(META.SOURCE) && 
            checkMeta(META.EXCHANGE) && 
            checkMeta(META.FROM_CURRENCY) &&
            checkMeta(META.TO_CURRENCY))
        {
            return true;
        }
        return false;
    }

    private boolean checkMeta(META m)
    {
        String val = meta.get(m);
        if (val == null || val.isEmpty())
        {
            logger.warning("Missing " + m.name() + " from file " + source);
            return false;
        }

        return true;
    }*/
    
    /*public String getInfoLine(META m)
    {
        return INFO_LINE_START + " " + m.name() + INFO_LABEL_DELIM + " " + meta.get(m) + "\n";
    }*/
    
    public String getHeadersLine()
    {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < headers.size(); i++)
        {
            String header = headers.get(i);
            sb.append(header);
            
            if  (i + 1 < headers.size())
            {
                int tabs = (int) Math.ceil((maxColumnLength - header.length())/4.0);
                for (int k = 0; k < tabs; k++)
                {
                    sb.append("\t");
                }
            }
        }
        sb.append("\n");
        return sb.toString();
    }
    
    public String getValuesAsLines()
    {
        StringBuilder sb = new StringBuilder();
        for (DataRow row: rows)
        {
            for (int i = 0; i < row.values.size(); i++)
            {
                String value = row.values.get(i);
                if (value == null)
                {
                    value = "-";
                }
                
                sb.append(value);
                if  (i + 1 < row.values.size())
                {
                    int tabs = (int) Math.ceil((maxColumnLength - value.length())/4.0);
                    for (int k = 0; k < tabs; k++)
                    {
                        sb.append("\t");
                    }
                }
                
            }
            sb.append("\n");
        }
        return sb.toString();
    }
}

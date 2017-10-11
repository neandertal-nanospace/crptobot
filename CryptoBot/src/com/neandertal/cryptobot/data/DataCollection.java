package com.neandertal.cryptobot.data;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.NumberFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.logging.Logger;
import java.util.stream.Stream;

import com.neandertal.cryptobot.data.DataCollectionInfo.ENTRY;

public class DataCollection
{
    private static final Logger logger = Logger.getLogger(DataCollection.class.getSimpleName());
    
    private static final String COLUMN_DATE = "Date";
    private static final NumberFormat nFormat = NumberFormat.getInstance(Locale.ENGLISH);
    private static final SimpleDateFormat[] dateFormatters = new SimpleDateFormat[2];
    static {
        dateFormatters[0] = new SimpleDateFormat("yyyy-MM-dd", Locale.ENGLISH);
        dateFormatters[1] = new SimpleDateFormat("MMM dd, yyyy", Locale.ENGLISH);//Sep 12, 2017
    }

    private String sourceFile = null;
    private DataCollectionInfo info = new DataCollectionInfo();
    private List<DataColumn> columns = new ArrayList<>();
    
    public static DataCollection readFromFile(Path path)
    {
        DataCollection dc = new DataCollection();
        dc.setSourceFile(path.toString());

        // read file info
        try (Stream<String> stream = Files.lines(path))
        {
            dc.info.fillFrom(stream);
        }
        catch (Exception e)
        {
            logger.warning("Failed to read file: " + path.toString());
            e.printStackTrace();
            return null;
        }
        
        // read values
        try (Stream<String> stream = Files.lines(path))
        {
            char separator = 0;
            Iterator<String> iter = stream.iterator();
            while (iter.hasNext())
            {
                String s = iter.next().trim();
                if (s.isEmpty()) continue;

                if (s.startsWith(DataCollectionInfo.INFO_LINE_START)) continue;

                if (s.startsWith(COLUMN_DATE))
                {
                    separator = readHeaderLine(s, dc);
                    continue;
                }

                readLine(s, dc, separator);
            }
        }
        catch (Exception e)
        {
            logger.warning("Failed to read file: " + path.toString());
            e.printStackTrace();
            return null;
        }

        DataColumn dateColumn = dc.columns.get(0); 
        dc.info.set(ENTRY.START, dateFormatters[0].format(dateColumn.getValue(0)));
        dc.info.set(ENTRY.END, dateFormatters[0].format(dateColumn.getValue(dateColumn.getValuesCount() - 1)));
        dc.info.set(ENTRY.RECORDS, String.valueOf(dateColumn.getValuesCount()));
        
        logger.info("Read file: " + dc.sourceFile);
        return dc;
    }

    private static char readHeaderLine(String s, DataCollection dc)
    {
        char separator = '\t';
        if (s.contains(","))
        {
            separator = ',';
        }
        
        String[] split = s.split(String.valueOf(separator) + "+");
        for (int i = 0; i < split.length; i++)
        {
            if (i == 0)
            {
                dc.columns.add(new DataColumn(COLUMN_DATE, Date.class));
                continue;
            }

            String col = split[i].trim();
            if (col.isEmpty())
            {
                throw new IllegalArgumentException("Empty column header in: " + s);
            }

            if (col.endsWith("[LONG]"))
            {
                dc.columns.add(new DataColumn(col.replace("[LONG]", ""), Long.class));
                continue;
            }
            
            dc.columns.add(new DataColumn(col.replace("[DOUBLE]", ""), Double.class));
        }
        return separator;
    }

    private static void readLine(String s, DataCollection dc, char separator)
    {
        String[] split = s.split(String.valueOf(separator) + "+");
        
        int valueIndexInColumn = 0;
        for (int i = 0; i < split.length; i++)
        {
            String v = split[i].trim();
            
            DataColumn column = dc.columns.get(i);
            
            if (i == 0)// date column
            {
                Date date = readDate(v);
                
                for (int j = 0; j < column.getValuesCount(); j++)
                {
                    Date colValue = (Date) column.getValue(j);
                    if (colValue.before(date))
                    {
                        valueIndexInColumn = j;
                    }
                }

                column.addValue(date, valueIndexInColumn);
                continue;
            }
            
            if ("0.0".equals(v) || "-".equals(v))
            {
                column.addValue(null, valueIndexInColumn);
                continue;
            }
            
            try
            {
                if (column.getType().equals(Long.class))
                {
                    Number number = nFormat.parse(v);
                    Long l = number.longValue();
                    column.addValue(l, valueIndexInColumn);
                    continue;
                }
                
                
                Number number = nFormat.parse(v);
                Double d = number.doubleValue();
                column.addValue(d, valueIndexInColumn);
            }
            catch (ParseException e)
            {
                throw new IllegalArgumentException("Failed to parse value: " + v + " in line: " + s, e);
            }
        }
    }

    private static Date readDate(String d)
    {
        Date date = null;
        for (SimpleDateFormat sdf: dateFormatters)
        {
            try
            {
                date = sdf.parse(d);
                return date;
            }
            catch (ParseException e) {}
        }
        throw new IllegalArgumentException("Date is unknown format: " + d);
    }
    
    public static void saveAsFile(DataCollection dc, Path newPath)
    {
        try (BufferedWriter writer = Files.newBufferedWriter(newPath))
        {
            writer.write(dc.toString());
            logger.info("Wrote file: " + newPath.toString());
        }
        catch (IOException e)
        {
            logger.warning("Failed to write file: " + newPath.toString());
            e.printStackTrace();
        }
    }
    
    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append(info.toString());
        sb.append("\n");
        sb.append(columnsToString());
        return sb.toString();
    }
    
    private int calculateColumnLength()
    {
        int maxColumnLength = 0;
        for (DataColumn column: columns)
        {
            maxColumnLength = Math.max(maxColumnLength, column.getHeaderAndType().length());
            for (int i = 0; i < column.getValuesCount(); i++)
            {
                maxColumnLength = Math.max(maxColumnLength, column.getValueAsString(i).length());
            }
        }
        maxColumnLength += 4 - maxColumnLength%4;
        return maxColumnLength;
    }

    private String columnsToString()
    {
        int columnLength = calculateColumnLength();
        
        StringBuilder sb = new StringBuilder();
        //headers
        for (int col = 0; col < columns.size(); col++)
        {
            DataColumn column = columns.get(col);
            String columnHeader = column.getHeaderAndType();
            sb.append(columnHeader);

            if (col + 1 < columns.size())
            {
                int tabs = (int) Math.ceil((columnLength - columnHeader.length()) / 4.0);
                for (int k = 0; k < tabs; k++)
                {
                    sb.append("\t");
                }
            }
        }
        sb.append("\n");
        
        //column values
        DataColumn dateColumn = columns.get(0);
        for (int row = 0; row < dateColumn.getValuesCount(); row++)
        {
            for (int col = 0; col < columns.size(); col++)
            {
                DataColumn column = columns.get(col);
                String columnValue = column.getValueAsString(col);
                sb.append(columnValue);

                if (col + 1 < columns.size())
                {
                    int tabs = (int) Math.ceil((columnLength - columnValue.length()) / 4.0);
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

    public String getSourceFile()
    {
        return sourceFile;
    }

    public void setSourceFile(String sourceFile)
    {
        this.sourceFile = sourceFile;
    }

    public DataCollectionInfo getInfo()
    {
        return info;
    }
    
    public List<DataColumn> getColumns()
    {
        return columns;
    }
}

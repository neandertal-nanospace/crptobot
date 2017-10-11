package com.neandertal.cryptobot.data;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.NumberFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;
import java.util.logging.Logger;
import java.util.stream.Stream;

import com.neandertal.cryptobot.data.DataCollectionInfo.ENTRY;

public class DatasetTransformer
{
/*    private static final SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd", Locale.ENGLISH);
    private static final SimpleDateFormat df2 = new SimpleDateFormat("MMM dd, yyyy", Locale.ENGLISH);//Sep 12, 2017
    private static final NumberFormat numberFormat = NumberFormat.getInstance(Locale.ENGLISH);
    static {
        numberFormat.setGroupingUsed(false);
    }*/
    
/*    private static final String INFO_LINE_START = "#";
    private static final String INFO_LABEL_DELIM = ":";*/
    

    /*private static enum META
    {
        SOURCE, EXCHANGE, FROM_CURRENCY, TO_CURRENCY, START, END, RECORDS;
    }*/

/*    public static enum TIME_COLUMNS
    {
        YEAR, MONTH, DAY, WEEKDAY;
        
        public static boolean contains(TIME_COLUMNS[] a, TIME_COLUMNS t)
        {
            for (TIME_COLUMNS v : a)
            {
                if (v == t) return true;
            }
            return false;
        }
    }*/

    private static final String PATH_RAW_DATA = "D:\\Workspace\\BTCbot\\raw_data";
    private static final String PATH_DC_DATA = "D:\\Workspace\\BTCbot\\dc_data";
    private static final String PATH_IN = "D:\\Workspace\\BTCbot\\in_data";

    private static final Logger logger = Logger.getLogger(DatasetTransformer.class.getSimpleName());

    public static void main(String[] args)
    {
        List<DataCollection> dcList = readFiles(PATH_RAW_DATA);
        saveFiles(PATH_DC_DATA, dcList);
        
        String source = "";
        String exchange = "";
        String from = "BTC";
        String to = "USD";
        
        for (DataCollection dc: dcList)
        {
           if (from.equals(dc.getInfo().get(ENTRY.FROM_CURRENCY)) && 
               to.equals(dc.getInfo().get(ENTRY.TO_CURRENCY))) 
           {
               try
               {
                  TrainingSet ts = generateTrainingSet(dc, new TIME_COLUMNS[]{}, new String[]{"Low"}, 4, new String[]{"Low"}, 1);
                  saveTrainingSet(ts);
               }
               catch (Exception e)
               {
                   logger.warning("Failed to create input file from :" + dc.getSourceFile());
                   e.printStackTrace();
               }
           }
        }
    }

    private static List<DataCollection> readFiles(String dirPath)
    {
        List<DataCollection> dcList = new ArrayList<>();
        try (Stream<Path> paths = Files.walk(Paths.get(dirPath)))
        {
            paths.filter(Files::isRegularFile).forEach((path) -> {
                if (!path.getFileName().toString().endsWith(".txt"))
                {
                    return;
                }
                
                try
                {
                    DataCollection dc = DataCollection.readFromFile(path);
                    dcList.add(dc);
                }
                catch (Exception e)
                {
                    logger.warning("Failed to read file:" + path);
                    e.printStackTrace();
                }
            });
        }
        catch (IOException e)
        {
            logger.warning("Failed to read files in folder:" + dirPath);
            e.printStackTrace();
        }
        
        return dcList;
    }

    private static void saveFiles(String dirPath, List<DataCollection> dcList)
    {
        for (DataCollection dc: dcList)
        {
            File file = new File(dc.getSourceFile());
            String filename = file.getName();
            if (filename.indexOf(".") > 0)
            {
                filename = filename.substring(0, filename.lastIndexOf("."));
            }
            Path newPath = Paths.get(dirPath, filename + ".dc");
            
            DataCollection.saveAsFile(dc, newPath);
        }
    }
    

    
    
    private static TrainingSet generateTrainingSet(DataCollection dc, TIME_COLUMNS[] timeCols, String[] inColumns, int inSet, String[] outColumns, int outSet) throws Exception
    {
        TrainingSet ts = new TrainingSet();
        ts.source = dc.source;
        
        if (TIME_COLUMNS.contains(timeCols, TIME_COLUMNS.YEAR)) ts.headers.add(TIME_COLUMNS.YEAR.name());
        if (TIME_COLUMNS.contains(timeCols, TIME_COLUMNS.MONTH)) ts.headers.add(TIME_COLUMNS.MONTH.name());
        if (TIME_COLUMNS.contains(timeCols, TIME_COLUMNS.DAY)) ts.headers.add(TIME_COLUMNS.DAY.name());
        if (TIME_COLUMNS.contains(timeCols, TIME_COLUMNS.WEEKDAY)) ts.headers.add(TIME_COLUMNS.WEEKDAY.name());
        
        int[] inIndex = new int[inColumns.length];
        for (int i = 0; i < inSet; i++)
        {
            int j = 0;
            for (String inCol: inColumns)
            {
                ts.headers.add(inCol + "_in" + i);
                inIndex[j] = dc.headers.indexOf(inCol);
                
                if (inIndex[j] < 1) throw new Exception("No column: " + inCol + " in file: " + dc.source);
                j++;
            }
        }
        
        int[] outIndex = new int[outColumns.length];
        for (int i = 0; i < outSet; i++)
        {
            int j = 0;
            for (String outCol: outColumns)
            {
                ts.headers.add(outCol +"_out" + i);
                outIndex[j] = dc.headers.indexOf(outCol);
                if (outIndex[j] < 1) throw new Exception("No column: " + outCol + " in file: " + dc.source);
                j++;
            }
        }
        
        List<DataRow> rows = new ArrayList<>(dc.rows);
        while (rows.size() >= inSet+outSet)
        {
            DataRow tsRow = new DataRow();
            ts.rows.add(tsRow);
            for (int i = 0; i < inSet; i++)
            {
                DataRow r = rows.remove(rows.size() - 1);
                
                if (i == 0)
                {
                    Date date = dateFormatter.parse(r.values.get(0));
                    Calendar calendar = new GregorianCalendar(TimeZone.getTimeZone("GMT"));
                    calendar.setTime(date);
                    if (TIME_COLUMNS.contains(timeCols, TIME_COLUMNS.YEAR)) tsRow.values.add(String.valueOf(calendar.get(Calendar.YEAR)));
                    //Add one to month {0 - 11}
                    if (TIME_COLUMNS.contains(timeCols, TIME_COLUMNS.MONTH)) tsRow.values.add(String.valueOf(calendar.get(Calendar.MONTH) + 1));
                    if (TIME_COLUMNS.contains(timeCols, TIME_COLUMNS.DAY)) tsRow.values.add(String.valueOf(calendar.get(Calendar.DAY_OF_MONTH)));
                    if (TIME_COLUMNS.contains(timeCols, TIME_COLUMNS.WEEKDAY)) tsRow.values.add(String.valueOf(calendar.get(Calendar.DAY_OF_WEEK)));
                }
                
                for (int inInd: inIndex)
                {
                    tsRow.values.add(r.values.get(inInd));
                }
            }
            
            for (int i = 0; i < outSet; i++)
            {
                DataRow r = rows.remove(rows.size() - 1);
                for (int outInd: outIndex)
                {
                    tsRow.values.add(r.values.get(outInd));
                }
            }
        }
        
        return ts;
    }
    
    private static void saveTrainingSet(TrainingSet ts)
    {
        File file = new File(ts.source);
        String filename = file.getName();
        if (filename.indexOf(".") > 0)
        {
            filename = filename.substring(0, filename.lastIndexOf("."));
        }
        Path path = Paths.get(PATH_IN, filename + ".in");

        try (BufferedWriter writer = Files.newBufferedWriter(path))
        {
            writer.write(ts.getHeadersLine());
            writer.write(ts.getRowsAsLines());
            
            logger.info("Wrote file: " + path.toString());
        }
        catch (IOException e)
        {
            logger.warning("Failed to write file: " + path.toString());
            e.printStackTrace();
        }
    }

    /*private static class DataCollection
    {
        public String source = null;
        public char separator = ',';
        public Map<META, String> meta = new HashMap<>();
        public List<String> headers = new ArrayList<>();
        public List<DataRow> rows = new ArrayList<>();
        public int maxColumnLength = 0;

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
        
        public boolean prepareForSave()
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
        }
        
        public String getInfoLine(META m)
        {
            return INFO_LINE_START + " " + m.name() + INFO_LABEL_DELIM + " " + meta.get(m) + "\n";
        }
        
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
    }*/
    
    /*private static class DataRow implements Comparable<DataRow>
    {
        public List<String> values = new ArrayList<>();

        @Override
        public int compareTo(DataRow row)
        {
            try
            {
                Date date = dateFormatter.parse(values.get(0));
                Date rowDate = dateFormatter.parse(row.values.get(0));
                return -date.compareTo(rowDate);
            }
            catch (ParseException e)
            {
                e.printStackTrace();
            }
            return 0;
        }
    }*/
    
    private static class TrainingSet
    {
        public String source = null;
        public char separator = ';';
        public List<String> headers = new ArrayList<>();
        public List<DataRow> rows = new ArrayList<>();
        
        public String getHeadersLine()
        {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < headers.size(); i++)
            {
                String header = headers.get(i);
                if (i > 0)
                {
                    sb.append(separator);
                }
                sb.append(header);
            }
            sb.append("\n");
            return sb.toString();
        }
        
        public String getRowsAsLines()
        {
            StringBuilder sb = new StringBuilder();
            for (DataRow row: rows)
            {
                for (int i = 0; i < row.values.size(); i++)
                {
                    String value = row.values.get(i);
                    if (value == null)
                    {
                        value = "0.0";//TODO
                    }
                    
                    if (i > 0)
                    {
                        sb.append(separator);
                    }
                    sb.append(value);
                }
                sb.append("\n");
            }
            return sb.toString();
        }
    }
}

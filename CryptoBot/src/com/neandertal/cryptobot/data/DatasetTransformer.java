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

public class DatasetTransformer
{
    private static final SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd", Locale.ENGLISH);
    private static final SimpleDateFormat df2 = new SimpleDateFormat("MMM dd, yyyy", Locale.ENGLISH);//Sep 12, 2017
    private static final NumberFormat numberFormat = NumberFormat.getInstance(Locale.ENGLISH);
    static {
        numberFormat.setGroupingUsed(false);
    }
    
/*    private static final String INFO_LINE_START = "#";
    private static final String INFO_LABEL_DELIM = ":";*/
    private static final String COLUMN_DATE = "Date";

    /*private static enum META
    {
        SOURCE, EXCHANGE, FROM_CURRENCY, TO_CURRENCY, START, END, RECORDS;
    }*/

    public static enum TIME_COLUMNS
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
    }

    private static final String PATH_RAW_DATA = "D:\\Workspace\\BTCbot\\raw_data";
    private static final String PATH_DC_DATA = "D:\\Workspace\\BTCbot\\dc_data";
    private static final String PATH_IN = "D:\\Workspace\\BTCbot\\in_data";

    private static final Logger logger = Logger.getLogger(DatasetTransformer.class.getSimpleName());

    public static void main(String[] args)
    {
        readRawFiles(PATH_RAW_DATA);
        List<DataCollection> dcList = readDcFiles(PATH_DC_DATA);
        
        String source = "";
        String exchange = "";
        String from = "BTC";
        String to = "USD";
        
        for (DataCollection dc: dcList)
        {
           if (from.equals(dc.meta.get(META.FROM_CURRENCY)) && 
               to.equals(dc.meta.get(META.TO_CURRENCY))) 
           {
               try
               {
                   TrainingSet ts = generateTrainingSet(dc, new TIME_COLUMNS[]{}, new String[]{"Low"}, 4, new String[]{"Low"}, 1);
                   saveTrainingSet(ts);
               }
               catch (Exception e)
               {
                   logger.warning("Failed to create input file from :" + dc.source);
                   e.printStackTrace();
               }
           }
        }
    }

    private static List<DataCollection> readDcFiles(String dcPath)
    {
        List<DataCollection> dcList = new ArrayList<>();
        
        try (Stream<Path> paths = Files.walk(Paths.get(dcPath)))
        {
            paths.filter(Files::isRegularFile).forEach((path) -> {
                if (!path.getFileName().toString().endsWith(".dc"))
                {
                    return;
                }
                
                DataCollection dc = readRawFile(path);
                
                if (dc.rows.isEmpty())
                {
                    logger.warning("File is empty: " + dc.source);
                }
                else if (dc.headers.size() < 2)
                {
                    logger.warning("File is only one column: " + dc.source);
                }
                else
                {
                    dcList.add(dc);
                }
            });
        }
        catch (IOException e)
        {
            logger.warning("Failed to read raw files in folder:" + dcPath);
            e.printStackTrace();
        }
        
        return dcList;
    }
    
    private static void readRawFiles(String rawPath)
    {
        try (Stream<Path> paths = Files.walk(Paths.get(rawPath)))
        {
            paths.filter(Files::isRegularFile).forEach((path) -> {
                if (!path.getFileName().toString().endsWith(".txt"))
                {
                    return;
                }
                
                DataCollection dc = readRawFile(path);
                if (dc != null && dc.prepareForSave())
                {
                    saveRawFile(dc);
                }
            });
        }
        catch (IOException e)
        {
            logger.warning("Failed to read raw files in folder:" + rawPath);
            e.printStackTrace();
        }
    }

    private static DataCollection readRawFile(Path path)
    {
        DataCollection dc = new DataCollection();
        dc.source = path.toString();
        
        Date[] seDates = new Date[2];
        
        // read file into stream, try-with-resources
        try (Stream<String> stream = Files.lines(path))
        {
            stream.forEach((s) -> {
                s = s.trim();
                if (s.isEmpty()) return;

                if (s.startsWith(COLUMN_DATE))
                {
                    if (s.contains(","))
                    {
                        dc.separator = ',';
                    }
                    else
                    {
                        dc.separator = '\t';
                    }
                    
                    String[] split = s.split(String.valueOf(dc.separator) + "+");
                    for (int i = 0; i < split.length; i++)
                    {
                        String col = (i == 0) ? COLUMN_DATE: split[i];
                        col = col.trim();
                        if (col.isEmpty()) continue;
                        dc.headers.add(col);
                        dc.maxColumnLength = Math.max(dc.maxColumnLength, col.length());
                    }
                    return;
                }
                
                /*if (s.startsWith(INFO_LINE_START))
                {
                    s = s.substring(INFO_LINE_START.length());
                    String[] split = s.split(INFO_LABEL_DELIM);
                    String label = split[0].trim();
                    String value = (split.length > 1) ? split[1].trim(): "";

                    META meta = null;
                    try
                    {
                        meta = META.valueOf(label.toUpperCase());
                    }
                    catch (Exception e){}
                    if (meta != null)
                    {
                        dc.meta.put(meta, value);
                    }
                    return;
                }*/
                
                String[] split = s.split(String.valueOf(dc.separator) + "+");
                DataRow row = new DataRow();
                for (int i = 0; i < split.length; i++)
                {
                    String v = split[i].trim();
                    if (i == 0)//date column
                    {
                        Date date = null;
                        try
                        {
                            date = dateFormatter.parse(v);
                        }
                        catch (ParseException e)
                        {
                            try
                            {
                                date = df2.parse(v);
                            }
                            catch (ParseException e2) 
                            {
                                logger.warning("Failed to parse date: " + v + " in file: " + dc.source);
                                return;
                            }
                        }
                        
                        if (seDates[0] == null || seDates[0].after(date))
                        {
                            seDates[0] = date;
                        }
                        if (seDates[1] == null || seDates[1].before(date))
                        {
                            seDates[1] = date;
                        }
                        
                        row.values.add(dateFormatter.format(date));
                    }
                    else
                    {
                        try
                        {
                            NumberFormat format = null;
                            if ("0.0".equals(v) || "-".equals(v))
                            {
                                row.values.add(null);
                            }
                            else if (v.contains("."))
                            {
                                format = NumberFormat.getInstance(Locale.ENGLISH);
                                Number number = format.parse(v);
                                Double d = number.doubleValue();
                                format.setGroupingUsed(false);
                                row.values.add(format.format(d));
                            }
                            else
                            {
                                format = NumberFormat.getIntegerInstance(Locale.ENGLISH);
                                Number number = format.parse(v);
                                Long l = number.longValue();
                                format.setGroupingUsed(false);
                                row.values.add(format.format(l));
                            }
                            
                        }
                        catch (ParseException e)
                        {
                            logger.warning("Failed to parse number: " + v + " in file: " + dc.source);
                            return;
                        }
                    }
                    dc.maxColumnLength = Math.max(dc.maxColumnLength, v.length());
                }
                dc.rows.add(row);
            });
            
            Collections.sort(dc.rows);
            dc.maxColumnLength += 4 - dc.maxColumnLength%4;
            
            dc.meta.put(META.START, dateFormatter.format(seDates[0]));
            dc.meta.put(META.END, dateFormatter.format(seDates[1]));
            dc.meta.put(META.RECORDS, String.valueOf(dc.rows.size()));
        }
        catch (Exception e)
        {
            logger.warning("Failed to read raw file: " + path.toString());
            e.printStackTrace();
            return null;
        }
        
        logger.info("Read file: " + dc.source);
        return dc;
    }

    private static void saveRawFile(DataCollection dc)
    {
        File file = new File(dc.source);
        String filename = file.getName();
        if (filename.indexOf(".") > 0)
        {
            filename = filename.substring(0, filename.lastIndexOf("."));
        }
        Path path = Paths.get(PATH_DC_DATA, filename + ".dc");

        try (BufferedWriter writer = Files.newBufferedWriter(path))
        {
            /*writer.write(dc.getInfoLine(META.SOURCE));
            writer.write(dc.getInfoLine(META.EXCHANGE));
            writer.write(dc.getInfoLine(META.FROM_CURRENCY));
            writer.write(dc.getInfoLine(META.TO_CURRENCY));
            writer.write(dc.getInfoLine(META.START));
            writer.write(dc.getInfoLine(META.END));
            writer.write(dc.getInfoLine(META.RECORDS));
            writer.newLine();*/
            writer.write(dc.getHeadersLine());
            writer.write(dc.getValuesAsLines());
            
            logger.info("Wrote file: " + path.toString());
        }
        catch (IOException e)
        {
            logger.warning("Failed to write file: " + path.toString());
            e.printStackTrace();
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

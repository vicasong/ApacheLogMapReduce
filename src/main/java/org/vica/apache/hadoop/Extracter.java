package org.vica.apache.hadoop;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.vica.apache.hadoop.custom.LogRecordWritable;
import org.vica.apache.log.LogAnalyser;
import org.vica.apache.log.LogRecord;

import java.io.IOException;
import java.text.SimpleDateFormat;

/**
 * Extract Data Step
 * Created by Vica-tony on 9/23/2016.
 */
public class Extracter {

    /**
     * Extract And Transform Data Mapper
     */
    public static class ExtractMapper extends Mapper<Object, Text, Text, LogRecordWritable> {

        final SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd-HH");
        LogRecordWritable logRecordWritable = new LogRecordWritable();
        Text date = new Text();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String source = value.toString();
            if (source != null && source.length() > 0) {
                try {
                    LogRecord record = LogAnalyser.read(source);
                    date.set(format.format(record.getDateTime()));
                    logRecordWritable.setRecord(record);
                } catch (Exception e) {
                    LogRecord record = new LogRecord();
                    record.setUnhandled(source);
                    logRecordWritable.setRecord(record);
                    date.clear();
                    date.set("error");
                }
                context.write(date, logRecordWritable);
            }
        }
    }


    /**
     * Common Load Data Mapper
     */
    public static class LoadMapper extends Mapper<Text, LogRecordWritable, Text, LogRecordWritable> {

        @Override
        protected void map(Text key, LogRecordWritable value, Context context) throws IOException, InterruptedException {
            context.write(key,value);
        }
    }
}

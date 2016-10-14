package org.vica.apache;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vica.apache.hadoop.Extracter;
import org.vica.apache.hadoop.Loader;
import org.vica.apache.hadoop.custom.LogRecordWritable;

import java.io.IOException;
import java.util.NavigableMap;
import java.util.Properties;

/**
 * The Main Program
 * Created by Vica-tony on 9/22/2016.
 */
public class Parser {

    private static Logger logger = LoggerFactory.getLogger(Parser.class);

    private static Properties properties;

    public static Properties getProperties(){
        return properties;
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        properties = new Properties();
        try {
            properties.load(Parser.class.getClassLoader().getResourceAsStream("conf.properties"));
        } catch (IOException e) {
            logger.error(e.getMessage(),e);
            System.exit(1);
        }
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS",properties.getProperty("fs.defaultFS"));
        conf.set("hbase.zookeeper.quorum", properties.getProperty("hbase.zookeeper.quorum"));

        if(args.length>1) {
            /**
             * Run The Job
             */
            Job analysis = Job.getInstance(conf);
            analysis.setMapperClass(Extracter.ExtractMapper.class);
            analysis.setOutputKeyClass(Text.class);
            analysis.setOutputValueClass(LogRecordWritable.class);
            analysis.setJarByClass(Parser.class);
            analysis.setOutputFormatClass(SequenceFileOutputFormat.class);
            analysis.setNumReduceTasks(0);
//        SequenceFileOutputFormat.setOutputCompressionType(analysis, SequenceFile.CompressionType.RECORD);
//        SequenceFileOutputFormat.setOutputCompressorClass(analysis, GzipCodec.class);
            FileInputFormat.addInputPath(analysis, new Path(args[0]));
            FileOutputFormat.setOutputPath(analysis, new Path(args[1]));

            Job counter = Job.getInstance(conf);
            counter.setMapperClass(Extracter.LoadMapper.class);
            //ken
            counter.setMapOutputKeyClass(Text.class);
            counter.setMapOutputValueClass(LogRecordWritable.class);
            counter.setJarByClass(Parser.class);
            counter.setInputFormatClass(SequenceFileInputFormat.class);

            int num = 0;
            try {
                num = Integer.parseInt(args[2]);
            } catch (Exception e) {
                num = 1;
            }

            counter.setNumReduceTasks(num);
            TableMapReduceUtil.initTableReducerJob("access-info", Loader.CounterReducer.class, counter);
            FileInputFormat.addInputPath(counter, new Path(args[1]));
//        FileOutputFormat.setOutputPath(counter,new Path(args[2]));

            JobControl control = new JobControl("apache-log-group");
            ControlledJob job1 = new ControlledJob(conf);
            job1.setJob(analysis);
            job1.setJobName("analysis");
            ControlledJob job2 = new ControlledJob(conf);
            job2.setJob(counter);
            job2.setJobName("counter");
            job2.addDependingJob(job1);
            control.addJob(job1);
            control.addJob(job2);
            Thread thread = new Thread(control);
            thread.start();

            while (true) {
                if (control.allFinished()) {
                    System.out.println(control.getSuccessfulJobList());
                    control.stop();
                    System.exit(0);
                } else if (control.getFailedJobList().size() > 0) {
                    System.out.println(control.getFailedJobList());
                    control.stop();
                    System.exit(1);
                }
            }
        }else {
            /**
             * Load Data
             */
            try {
                String tableName = "access-info";
                if(args.length==1){
                    tableName = args[0];
                }
                Scan scan = new Scan();
                scan.setMaxVersions(10);
                Connection connection = ConnectionFactory.createConnection(conf);
                HTable table = (HTable) connection.getTable(TableName.valueOf(tableName));
                ResultScanner rsc = table.getScanner(scan);
                for (Result result : rsc) {
                    //rowKey
                    System.out.println("- "+Bytes.toString(result.getRow()));
                    NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> map = result.getMap();
                    for (byte[] key : map.keySet()) {
                        //columnFamilies
                        System.out.println("   | " + Bytes.toString(key));
                        for (byte[] tk : map.get(key).keySet()) {
                            //columns
                            System.out.println("      |-- " + Bytes.toString(tk));
                            for (Long l : map.get(key).get(tk).keySet()) {
                                //values and timestamps
                                System.out.println("        |-- value[" + Bytes.toString(map.get(key).get(tk).get(l)) + "] (timestamp:" + l + ")");
                            }
                        }
                    }
                }
                rsc.close();
            } catch (IOException e) {
                logger.error(e.getMessage(), e);
            }
        }

    }
}

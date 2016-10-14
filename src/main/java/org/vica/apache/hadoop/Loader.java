package org.vica.apache.hadoop;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.vica.apache.hadoop.custom.LogRecordWritable;
import org.vica.apache.log.LogRecord;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * The Analysis Layer
 * Created by Vica-tony on 9/23/2016.
 */
public class Loader {
    /*
    access-info
    |--datetime(key)
        |--counter
            |--page
            |--user
            |--post
            |--get
            |--put
            |--post
            |--delete
            |--error
            |--connect
            |--other
            ...
        |--max
            |--page
            |--ip

    create 'access-info',{NAME=>'counter',VERSIONS=>1},{NAME=>'max',VERSIONS=>10}
     */

    /**
     * Count The PV/UV
     */
    public static class CounterReducer extends TableReducer<Text, LogRecordWritable, ImmutableBytesWritable>{
        @Override
        protected void reduce(Text key, Iterable<LogRecordWritable> values, Context context) throws IOException, InterruptedException {
            Map<String,Integer> count = new HashMap<String, Integer>();
            Map<String,Integer> users = new HashMap<String, Integer>();
            Map<String,Integer> pages = new HashMap<String, Integer>();
            int pv=0;
            for(LogRecordWritable writable:values){
                String mk=null;
                LogRecord record = writable.getRecord();
                if(record.getUnhandled()!=null){
                    mk="error";
                }else if(record.getUrl().startsWith("/")){
                    users.put(record.getIp(),users.get(record.getIp())!=null?(users.get(record.getIp())+1):1);
                    pages.put(record.getUrl(),pages.get(record.getUrl())!=null?(pages.get(record.getUrl())+1):1);
                    mk=record.getMethod()!=null?record.getMethod().toLowerCase():null;
                    pv++;
                }else {
                    mk="other";
                }
                count.put(mk,count.get(mk)!=null?(count.get(mk)+1):1);
            }
            Put put = new Put(key.getBytes());
            for(String column : count.keySet()){
                if(column!=null){
                    //ken
                    put.addColumn(Bytes.toBytes("counter"),Bytes.toBytes(column),Bytes.toBytes(String.valueOf(count.get(column))));
                }
            }
            int uv = users.size();
            put.addColumn(Bytes.toBytes("counter"),Bytes.toBytes("user"),Bytes.toBytes(String.valueOf(uv)));
            put.addColumn(Bytes.toBytes("counter"),Bytes.toBytes("page"),Bytes.toBytes(String.valueOf(pv)));
            put.addColumn(Bytes.toBytes("max"),Bytes.toBytes("page"),Bytes.toBytes(max(pages)));
            put.addColumn(Bytes.toBytes("max"),Bytes.toBytes("ip"),Bytes.toBytes(max(users)));
            context.write(new ImmutableBytesWritable(key.getBytes()),put);
        }


        private String max(Map<String,Integer> map){
            int val = 0;
            String max = "-";
            for(String key:map.keySet()){
                if(map.get(key)>val){
                    val = map.get(key);
                    max = key;
                }
            }
            return max;
        }

    }
}

package org.vica.apache.hadoop.custom;

import com.dyuproject.protostuff.LinkedBuffer;
import com.dyuproject.protostuff.ProtostuffIOUtil;
import com.dyuproject.protostuff.runtime.RuntimeSchema;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Writable;
import org.vica.apache.log.LogRecord;

import java.io.*;

/**
 * Costumed LogRecord Writable For Transformation
 * Created by Vica-tony on 9/23/2016.
 */
public class LogRecordWritable implements Writable {

    private static final RuntimeSchema<LogRecord> schema = RuntimeSchema.createFrom(LogRecord.class);
    private LogRecord record;
    private BytesWritable data = new BytesWritable();

    /**
     * Get record object
     * @return
     */
    public LogRecord getRecord() {
        return record;
    }
    public void setRecord(LogRecord record) {
        this.record = record;
    }

    /**
     * Deserialize the byte data
     * @param data
     */
    public void read(byte[] data){
        record = schema.newMessage();
        ProtostuffIOUtil.mergeFrom(data,record,schema);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        byte[] data = ProtostuffIOUtil.toByteArray(record,schema, LinkedBuffer.allocate(LinkedBuffer.DEFAULT_BUFFER_SIZE));
        this.data.set(data,0,data.length);
        this.data.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.data.readFields(dataInput);
        this.data.setCapacity(this.data.getLength());
        byte[] data = this.data.getBytes();
        read(data);
    }
}

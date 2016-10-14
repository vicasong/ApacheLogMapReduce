package org.vica.apache.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.vica.apache.Parser;

import java.io.IOException;
import java.util.List;

/**
 * The HBase Operations
 * Created by Vica-tony on 9/22/2016.
 */
public class Hbase {

    private static Configuration configuration;
    static {
        configuration = new Configuration();
        configuration.set("hbase.zookeeper.quorum", Parser.getProperties().getProperty("hbase.zookeeper.quorum"));
//        configuration.set("hbase.rootdir",Parser.getProperties().getProperty("hbase.rootdir"));
//        configuration.set("fs.defaultFS",Parser.getProperties().getProperty("fs.defaultFS"));
    }

    /**
     * Create A Table By Specified Name And ColumnFamilies
     * @param name The TableName (Contains namespace And qualifier Or qualifier Only For default Namespace)
     * @param columnFamily The ColumnFamilies
     * @throws IOException IO
     */
    public static void createTable(String name, String ... columnFamily) throws IOException {
        Connection connection = ConnectionFactory.createConnection(configuration);
        try {
            HBaseAdmin admin = (HBaseAdmin) connection.getAdmin();
            HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(name));
            for(String cf : columnFamily) {
                descriptor.addFamily(new HColumnDescriptor(cf));
            }
            if (!admin.tableExists(name)) {
                admin.createTable(descriptor);
            } else {
                throw new IOException("table named " + name + " already exist.");
            }
        }finally {
            connection.close();
        }
    }

    /**
     * Insert Into Some Values
     * @param tableName The Table Be Wrote
     * @param holder The Values Be Added
     * @throws IOException IO
     */
    public static void insert(String tableName, ColumnFamiliesValueHolder holder) throws IOException {
        holder.validate();
        Connection connection = ConnectionFactory.createConnection(configuration);
        try {
            Put put = new Put(holder.getRowKey());
            HTable table = (HTable) connection.getTable(TableName.valueOf(tableName));
            HColumnDescriptor[] familes = table.getTableDescriptor().getColumnFamilies();
            for(HColumnDescriptor family:familes){
                for(int i=0;i<holder.getFamilies().size();i++){
                    if(family.getNameAsString().equals(holder.getFamilies().get(i))){
                        for(int j=i;j<holder.getColumns().get(i).size();j++){
                            for(int s=j;s<holder.getValuse().get(i).get(j).size();s++){
                                put.addColumn(Bytes.toBytes(holder.getFamilies().get(i)),
                                        Bytes.toBytes(holder.getColumns().get(i).get(j)),
                                        Bytes.toBytes(holder.getValuse().get(i).get(j).get(s)));
                            }
                        }
                    }
                }
            }
            table.put(put);
        }finally {
            connection.close();
        }
    }

    /**
     * Query By RowKey
     * @param tableName The Table Name
     * @param rowKey The RowKey
     * @return The Queried Result
     * @throws IOException IO
     */
    public static Result select(String tableName, String rowKey) throws IOException {
        return select(tableName,Bytes.toBytes(rowKey), null, null);
    }

    /**
     * Query By ColumnFamilies And Columns Of Each ColumnFamily
     * @param tableName The Table Name
     * @param holder The Specified CF And C To Query
     * @return The Queried Result
     * @throws IOException IO
     */
    public static Result select(String tableName, ColumnFamiliesValueHolder holder) throws IOException {
        return select(tableName,holder.getRowKey(),holder.getFamilies(),holder.getColumns());
    }

    /**
     * Query Result With Version
     * @param tableName The Table Name
     * @param rowKey The Row Key
     * @param family The ColumnFamily
     * @param column The Column Of Family
     * @param maxVersion The Max Versions Queried
     * @return The Queried Result
     * @throws IOException IO
     */
    public static Result select(String tableName, String rowKey, String family, String column, int maxVersion) throws IOException {
        Connection connection = ConnectionFactory.createConnection(configuration);
        try {
            Get get = new Get(Bytes.toBytes(rowKey));
            HTable table = (HTable) connection.getTable(TableName.valueOf(tableName));
            get.addColumn(Bytes.toBytes(family),Bytes.toBytes(column));
            get.setMaxVersions(maxVersion);
            Result result = table.get(get);
            return result;
        }finally {
            connection.close();
        }
    }

    /**
     * Delete A Column Value Of ColumnFamily From Table
     * @param tableName The Table Name
     * @param rowKey The Row Key
     * @param family The ColumnFamily
     * @param column The Column Of Family To Delete
     * @throws IOException IO
     */
    public static void delete(String tableName, String rowKey, String family, String column) throws IOException {
        Connection connection = ConnectionFactory.createConnection(configuration);
        try {
            Delete delete = new Delete(Bytes.toBytes(rowKey));
            if(family!=null) {
                if(column!=null) {
                    delete.addColumns(Bytes.toBytes(family), Bytes.toBytes(column));
                }else {
                    delete.addFamily(Bytes.toBytes(family));
                }
            }
            HTable table = (HTable) connection.getTable(TableName.valueOf(tableName));
            table.delete(delete);
        }finally {
            connection.close();
        }
    }

    /**
     * Delete A Record Form Table By RowKey
     * @param tableName The Table Name
     * @param rowKey The Row Key To Delete
     * @throws IOException IO
     */
    public static void delete(String tableName,String rowKey) throws IOException {
        delete(tableName,rowKey,null,null);
    }

    /**
     * Drop Table
     * @param tableName The Table Name
     * @throws IOException IO
     */
    public static void dropTable(String tableName) throws IOException {
        Connection connection = ConnectionFactory.createConnection(configuration);
        try {
            HBaseAdmin admin = (HBaseAdmin) connection.getAdmin();
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
        }finally {
            connection.close();
        }
    }

    private static Result select(String tableName, byte[] rowKey, List<String> families, List<List<String>> columns) throws IOException {
        Connection connection = ConnectionFactory.createConnection(configuration);
        try {
            Get get = new Get(rowKey);
            HTable table = (HTable) connection.getTable(TableName.valueOf(tableName));
            if(families!=null) {
                for (int i = 0; i < families.size(); i++) {
                    if(columns!=null) {
                        for (int j = i; j < columns.get(i).size(); j++) {
                            get.addColumn(Bytes.toBytes(families.get(i)),
                                    Bytes.toBytes(columns.get(i).get(j)));
                        }
                    }else {
                        get.addFamily(Bytes.toBytes(families.get(i)));
                    }
                }
            }
            Result result = table.get(get);
            return result;
        }finally {
            connection.close();
        }
    }
}

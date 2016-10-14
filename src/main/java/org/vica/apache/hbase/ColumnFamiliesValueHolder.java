package org.vica.apache.hbase;

import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
import java.util.List;

/**
 * The Value Info Of Hbase
 * Created by Vica-tony on 9/22/2016.
 */
public class ColumnFamiliesValueHolder {
    private byte[] rowKey;
    private List<String> families;
    private List<List<String>> columns;
    private List<List<List<String>>> valuse;

    public ColumnFamiliesValueHolder(String rowKey){
        this.rowKey = Bytes.toBytes(rowKey);
        families = new ArrayList<String>();
        columns = new ArrayList<List<String>>();
        valuse = new ArrayList<List<List<String>>>();
    }

    public ColumnFamiliesValueHolder(String rowkey, String family, String[] columns, String[][] values){
        this(rowkey);
        addFamily(family);
        addColumns(columns);
        addValues(values);
    }

    public ColumnFamiliesValueHolder(String rowKey, String[] families, String[][] columns,String[][][] values){
        this(rowKey);
        for(int i=0; i<families.length;i++){
            addFamily(families[i]);
            addColumns(columns[i]);
            addValues(values[i]);
        }
    }

    /**
     * Add Column Family
     * @param family
     */
    public void addFamily(String family){
        //family
        families.add(family);
    }

    /**
     * Add A Column
     * @param columns
     */
    public void addColumns(String[] columns){
        //one family columns
        List<String> colum = new ArrayList<String>();
        for(String c : columns){
            colum.add(c);
        }
        this.columns.add(colum);
    }

    /**
     * Add Column Values
     * @param values
     */
    public void addValues(String[][] values){
        //values of every column
        List<List<String>> value = new ArrayList<List<String>>();
        for (String[] v : values){
            List<String> val = new ArrayList<String>();
            for(String t : v){
                val.add(t);
            }
            value.add(val);
        }
        this.valuse.add(value);
    }

    /**
     * Validate The Params Format
     */
    public void validate(){
        if(families.size()!=columns.size()||families.size()!=valuse.size())
            throw new IllegalArgumentException("Families Count Not Mach Columns Or Values");
        for (int i=0;i<columns.size();i++){
            if(columns.get(i).size()!=valuse.get(i).size()){
                throw new IllegalArgumentException("Columns Count Not Mach Values");
            }
        }
    }

    /**
     * Get The Row Key
     * @return
     */
    public byte[] getRowKey(){
        return rowKey;
    }

    /**
     * Get The Column Family
     * @return
     */
    public List<String> getFamilies() {
        return families;
    }

    /**
     * Get The Columns
     * @return
     */
    public List<List<String>> getColumns() {
        return columns;
    }

    /**
     * Get The Column Values
     * @return
     */
    public List<List<List<String>>> getValuse() {
        return valuse;
    }
}

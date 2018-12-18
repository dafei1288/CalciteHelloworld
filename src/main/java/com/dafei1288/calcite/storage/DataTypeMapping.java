package com.dafei1288.calcite.storage;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import org.apache.calcite.sql.type.SqlTypeName;

import java.math.BigDecimal;
import java.sql.Date;
import java.util.Set;

/**
 * 这里使用了GUAVA的table 作为存SQL和JAVA数据类型的数据结构
 * 这并不是一个好的设计，而是为了让大家更容易理解而做的设计
 */
public class DataTypeMapping {

    public static Table<String, SqlTypeName,Class> TYPEMAPPING= HashBasedTable.create();
    public static final String CHAR = "char";
    public static final String VARCHAR = "varchar";
    public static final String BOOLEAN = "boolean";
    public static final String DATE = "date";
    public static final String INTEGER = "integer";
    public static final String TINYINT = "tinyint";
    public static final String SMALLINT = "smallint";
    public static final String BIGINT = "bigint";
    public static final String DECIMAL = "decimal";
    public static final String NUMERIC = "numeric";
    public static final String FLOAT = "float";
    public static final String REAL = "real";
    public static final String DOUBLE = "double";
    public static final String TIME = "time";
    public static final String TIMESTAMP = "timestamp";
    public static final String ANY = "any";
    static{
        TYPEMAPPING.put(DataTypeMapping.CHAR,SqlTypeName.CHAR,Character.class);
        TYPEMAPPING.put(DataTypeMapping.VARCHAR,SqlTypeName.VARCHAR,String.class);
        TYPEMAPPING.put(DataTypeMapping.BOOLEAN,SqlTypeName.BOOLEAN,Boolean.class);
        TYPEMAPPING.put(DataTypeMapping.DATE,SqlTypeName.DATE,Date.class);
        TYPEMAPPING.put(DataTypeMapping.INTEGER,SqlTypeName.INTEGER,Integer.class);
        TYPEMAPPING.put(DataTypeMapping.TINYINT, SqlTypeName.TINYINT,Integer.class);
        TYPEMAPPING.put(DataTypeMapping.SMALLINT, SqlTypeName.SMALLINT,Integer.class);
        TYPEMAPPING.put(DataTypeMapping.BIGINT, SqlTypeName.BIGINT,Long.class);
        TYPEMAPPING.put(DataTypeMapping.DECIMAL, SqlTypeName.DECIMAL, BigDecimal.class);
        TYPEMAPPING.put(DataTypeMapping.NUMERIC, SqlTypeName.DECIMAL,Long.class);
        TYPEMAPPING.put(DataTypeMapping.FLOAT, SqlTypeName.FLOAT,Float.class);
        TYPEMAPPING.put(DataTypeMapping.REAL, SqlTypeName.REAL,Double.class);
        TYPEMAPPING.put(DataTypeMapping.DOUBLE, SqlTypeName.DOUBLE,Double.class);
        TYPEMAPPING.put(DataTypeMapping.TIME, SqlTypeName.TIME, Date.class);
        TYPEMAPPING.put(DataTypeMapping.TIMESTAMP, SqlTypeName.TIMESTAMP,Long.class);
        TYPEMAPPING.put(DataTypeMapping.ANY, SqlTypeName.ANY,String.class);
    }
    /**
     * 根据名字获取，对应的java类型
     * */
    public static Class getJavaClassByName(String name){
        Set<Table.Cell<String, SqlTypeName,Class>> table = TYPEMAPPING.cellSet();
        for(Table.Cell<String, SqlTypeName,Class> it:table){
            if(it.getRowKey().equals(name)){
                return it.getValue();
            }
        }
        return null;
    }
    public static SqlTypeName getSqlTypeByName(String name){
        for(Table.Cell<String, SqlTypeName,Class> it:TYPEMAPPING.cellSet()){
            if(it.getRowKey().equals(name)){
                return it.getColumnKey();
            }
        }
        return null;
    }
}

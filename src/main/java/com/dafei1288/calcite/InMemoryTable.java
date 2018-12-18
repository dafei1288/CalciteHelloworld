package com.dafei1288.calcite;

import com.dafei1288.calcite.storage.DataTypeMapping;
import com.dafei1288.calcite.storage.Storage;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.*;
import org.apache.calcite.schema.impl.AbstractTable;

import org.apache.calcite.sql.type.SqlTypeUtil;



public class InMemoryTable extends AbstractTable implements ScannableTable {
    private String name;
    private Storage.DummyTable _table;
    private RelDataType dataType;

    InMemoryTable(String name){
        System.out.println("InMemoryTable !!!!!!    "+name );
        this.name = name;
    }

    public InMemoryTable(String name, Storage.DummyTable it) {
        this.name = name;
        this._table = it;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
//        System.out.println("RelDataType !!!!!!");
        if(dataType == null) {
            RelDataTypeFactory.FieldInfoBuilder fieldInfo = typeFactory.builder();
            for (Storage.DummyColumn column : this._table.getColumns()) {
                RelDataType sqlType = typeFactory.createJavaType(column.getJavaClass());
                sqlType = SqlTypeUtil.addCharsetAndCollation(sqlType, typeFactory);
                fieldInfo.add(column.getName(), sqlType);
            }
            this.dataType = typeFactory.createStructType(fieldInfo);
        }
        return this.dataType;
    }


    @Override
    public Enumerable<Object[]> scan(DataContext root) {
        System.out.println("scan ...... ");
        return new AbstractEnumerable<Object[]>() {
            public Enumerator<Object[]> enumerator() {
                return new Enumerator<Object[]>(){
                    private int cur = 0;
                    @Override
                    public Object[] current() {
//                        System.out.println("cur = "+cur+" => ");
//                        for (int i =0;i<_table.getData(cur).length;i++){
//                            System.out.println(_table.getData(cur)[i]);
//                        }
                        return _table.getData(cur++);
                    }

                    @Override
                    public boolean moveNext() {
//                        System.out.println("++cur < _table.getRowCount() = "+(cur+1 < _table.getRowCount()));
                        return cur < _table.getRowCount() ;
                    }

                    @Override
                    public void reset() {

                    }

                    @Override
                    public void close() {

                    }
                };
            }
        };
    }
}

package com.dafei1288.calcite;

import com.dafei1288.calcite.storage.Storage;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

import java.util.HashMap;
import java.util.Map;

public class InMemorySchema extends AbstractSchema {
    private String dbName;
    private Map<String, Object> operand;

    public InMemorySchema(String name, Map<String, Object> operand) {
        this.operand = operand;
        this.dbName = dbName;
        System.out.println("");
        System.out.println("in this class ==> "+ this);

    }
    @Override
    public Map<String, Table> getTableMap() {

        Map<String, Table> tables = new HashMap<String, Table>();

        Storage.getTables().forEach(it->{
            //System.out.println("it = "+it.getName());
            tables.put(it.getName(),new InMemoryTable(it. getName(),it));
        });

        return tables;
    }
}

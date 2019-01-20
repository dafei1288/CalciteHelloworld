package com.dafei1288.calcite.stream;

import com.dafei1288.calcite.storage.Storage;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.TableFactory;

import java.util.Map;

public class InMemoryStreamTableFactory implements TableFactory {
    @Override
    public Table create(SchemaPlus schema, String name, Map operand, RelDataType rowType) {
        System.out.println(operand);
        System.out.println(name);
        return new InMemoryStreamTable(name, Storage.getTable(name));
    }
}

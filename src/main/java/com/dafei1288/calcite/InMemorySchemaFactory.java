package com.dafei1288.calcite;

import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;

import java.util.Map;

public class InMemorySchemaFactory implements SchemaFactory {
    @Override
    public Schema create(SchemaPlus parentSchema, String name, Map<String, Object> operand) {


        System.out.println("schema name ==>  "+ name);
        System.out.println("operand ==> "+operand);

        return new InMemorySchema(name,operand);
    }
}

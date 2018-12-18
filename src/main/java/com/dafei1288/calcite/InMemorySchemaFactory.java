package com.dafei1288.calcite;

import com.dafei1288.calcite.function.MathFunction;
import com.dafei1288.calcite.function.StringFunction;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;

import java.util.Map;

public class InMemorySchemaFactory implements SchemaFactory {
    @Override
    public Schema create(SchemaPlus parentSchema, String name, Map<String, Object> operand) {


        System.out.println("schema name ==>  "+ name);
        System.out.println("operand ==> "+operand);

        parentSchema.add("SQUARE_FUNC",ScalarFunctionImpl.create(MathFunction.class,"square"));
        parentSchema.add("TOSTRING_FUNC",ScalarFunctionImpl.create(StringFunction.class,"parseString"));
        parentSchema.add("CONCAT_FUNC",ScalarFunctionImpl.create(StringFunction.class,"concat"));

        return new InMemorySchema(name,operand);
    }
}

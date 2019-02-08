package com.dafei1288.calcite.stream.kafka;

import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.StreamableTable;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;


public class KafkaStreamTable extends AbstractTable implements StreamableTable, ScannableTable {

    @Override
    public Table stream() {
        return  this;
    }

    private String name;
    private RelDataType dataType;
    private Map operand;
    private static KafkaConsumer<String, String> consumer;

    public KafkaStreamTable(String name){
        System.out.println("KafkaStreamTable !!!!!!    "+name );
        this.name = name;
    }

    public KafkaStreamTable(String name, Map operand) {
        System.out.println("KafkaStreamTable !!!!!!    "+name +" , "+operand);
        this.name = name;
        this.operand = operand;


    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
//        System.out.println("RelDataType !!!!!!");
        if(dataType == null) {
            RelDataTypeFactory.FieldInfoBuilder fieldInfo = typeFactory.builder();
            //我们需要存储stream table的元数据信息，为了案例，我写在了kafkaStream.json文件里配置信息里colnames
            for (String col : operand.get("colnames").toString().split(",")) {
                RelDataType sqlType = typeFactory.createJavaType(String.class);
                sqlType = SqlTypeUtil.addCharsetAndCollation(sqlType, typeFactory);
                fieldInfo.add(col, sqlType);
            }
            this.dataType = typeFactory.createStructType(fieldInfo);
        }
        return this.dataType;
    }


    @Override
    public Enumerable<Object[]> scan(DataContext root) {
        System.out.println("scan ...... ");
        consumer = new KafkaConsumer<String, String>(operand);
        consumer.subscribe(Arrays.asList(operand.get("topic").toString()));

        return new AbstractEnumerable<Object[]>() {

            public Enumerator<Object[]> enumerator() {
                return new Enumerator<Object[]>(){
                    //这里设置了一个间隔时间，因为，刚才的producter里面，数据是每秒产生的，如果这里值太下，则会出现取不出值的可能
                    ConsumerRecords<String, String> records = consumer.poll(Integer.parseInt(operand.get("timeouts").toString()));
                    Iterator it =records.iterator();
                    private int cur = 0;
                    @Override
                    public Object[] current() {
                        ConsumerRecord<String, String> reco = (ConsumerRecord<String, String>) it.next();
                        return new String[]{reco.key(),reco.value()};
                    }

                    @Override
                    public boolean moveNext() {
                        //ConsumerRecord<String, String> record : records
                        return it.hasNext();
                    }

                    @Override
                    public void reset() {

                    }

                    @Override
                    public void close() {
                        consumer.close();
                    }
                };
            }
        };
    }
}

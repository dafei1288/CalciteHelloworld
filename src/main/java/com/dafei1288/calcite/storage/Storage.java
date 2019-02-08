package com.dafei1288.calcite.storage;

import org.apache.calcite.sql.type.SqlTypeName;

import java.util.*;

/**
 * 用于模拟数据库结构及数据
 *
 * author : id,name,age
 * book : id,aid,name,type
 * */
public class Storage {
    public static final String SCHEMA_NAME = "bookshop";
    public static final String TABLE_AUTHOR = "AUTHOR";
    public static final String TABLE_BOOK = "BOOK";
    public static final String TABLE_SALES = "SALES";
    public static final String TABLE_PRODUCT = "PRODUCT";
//    public static List<DummyTable> tables = new ArrayList<>();
    public static Hashtable<String,DummyTable> _bag = new Hashtable<>();
    static{
        DummyTable author = new DummyTable(TABLE_AUTHOR);
        DummyColumn id = new DummyColumn("ID","integer");
        DummyColumn name = new DummyColumn("NAME","varchar");
        DummyColumn age = new DummyColumn("AGE","integer");
        DummyColumn aid = new DummyColumn("AID","integer");
        DummyColumn type = new DummyColumn("TYPE","varchar");
        author.addColumn(id).addColumn(name).addColumn(age);
        author.addRow(1,"jacky",33);
        author.addRow(2,"wang",23);
        author.addRow(3,"dd",32);
        author.addRow(4,"ma",42);
//        tables.add(author);
        _bag.put(TABLE_AUTHOR,author);

        DummyTable book = new DummyTable(TABLE_BOOK);
        book.addColumn(id).addColumn(aid).addColumn(name).addColumn(type);
        book.addRow(1,1,"数据山","java");
        book.addRow(2,2,"大关","sql");
        book.addRow(3,1,"lili","sql");
        book.addRow(4,3,"ten","c#");
//        tables.add(book);
        _bag.put(TABLE_BOOK,book);


        DummyTable sales = new Storage.DummyTable(TABLE_SALES);
        Storage.DummyColumn rowtime = new Storage.DummyColumn("rowtime","time");
        Storage.DummyColumn productId = new Storage.DummyColumn("productId","integer");
        Storage.DummyColumn orderId = new Storage.DummyColumn("orderId","integer");
        Storage.DummyColumn units = new Storage.DummyColumn("units","integer");
        sales.addColumn(rowtime).addColumn(productId).addColumn(orderId).addColumn(units);
        sales.addRow("10:17:00" ,30 ,5 ,4);
        sales.addRow("10:17:05",10,6,1);
        sales.addRow("10:18:05",20,7,2);
        sales.addRow("10:18:07",30,8,20);
        sales.addRow("11:02:00",10,9,6);
        sales.addRow("11:04:00",10,10,1);
        sales.addRow("11:09:30",40,11,12);
        sales.addRow("11:24:11",10,12,4);
        _bag.put(TABLE_SALES,sales);

        DummyTable prod = new Storage.DummyTable(TABLE_PRODUCT);
        Storage.DummyColumn productName = new Storage.DummyColumn("name","varchar");
        prod.addColumn(productId).addColumn(productName);
        prod.addRow(10,"Beer");
        prod.addRow(20,"Wine");
        prod.addRow(30,"Cheese");
        prod.addRow(40,"Bread");
        _bag.put(TABLE_PRODUCT,prod);
    }

    public static Collection<DummyTable> getTables(){
        return _bag.values();
    }
    public static DummyTable getTable(String tableName){return _bag.get(tableName);}

    public static class DummyTable{
      private String name;
      private List<DummyColumn> columns;
      private List<List<Object>> datas = new ArrayList<>();
      public DummyTable(String name){
          this.name = name;
      }

      public String getName(){
          return this.name;
      }

        public List<DummyColumn> getColumns() {
            return columns;
        }

        public DummyTable addColumn(DummyColumn dc){
          if(this.columns == null){
              this.columns = new ArrayList<>();
          }
          this.columns.add(dc);
          return this;
        }

        public void setColumns(List<DummyColumn> columns) {
            this.columns = columns;
        }

        public Object[] getData(int index){
          return this.datas.get(index).toArray();
        }

        public int getRowCount(){
          return this.datas.size();
        }

        public void addRow(Object...objects){
          this.datas.add(Arrays.asList(objects));
        }


    }

    public static class DummyColumn{
        private String name;
        private String type;

        public DummyColumn(String name, String type) {
            this.name = name;
            this.type = type;
        }

        public String getName() {
            return name;
        }

        public String getType() {
            return type;
        }

        public void setName(String name) {
            this.name = name;
        }

        public void setType(String type) {
            this.type = type;
        }

        //我充血模型
        //获取JAVA类型
        public Class getJavaClass(){
            return DataTypeMapping.getJavaClassByName(this.type);
        }

        public SqlTypeName getSqlTypeName(){
            return DataTypeMapping.getSqlTypeByName(this.type);
        }
    }

}

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
    }

    public static Collection<DummyTable> getTables(){
        return _bag.values();
    }
    public static DummyTable getTable(String tableName){return _bag.get(tableName);}

    public static class DummyTable{
      private String name;
      private List<DummyColumn> columns;
      private List<List<Object>> datas = new ArrayList<>();
      DummyTable(String name){
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

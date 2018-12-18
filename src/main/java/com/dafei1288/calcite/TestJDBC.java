package com.dafei1288.calcite;

import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.util.ConversionUtil;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Date;
import java.util.Properties;

public class TestJDBC {
    public static void main(String[] args) {
        try {
            Class.forName("org.apache.calcite.jdbc.Driver");
        } catch (ClassNotFoundException e1) {
            e1.printStackTrace();
        }
        System.setProperty("saffron.default.charset", ConversionUtil.NATIVE_UTF16_CHARSET_NAME);
        System.setProperty("saffron.default.nationalcharset",ConversionUtil.NATIVE_UTF16_CHARSET_NAME);
        System.setProperty("saffron.default.collation.name",ConversionUtil.NATIVE_UTF16_CHARSET_NAME + "$en_US");

        Properties info = new Properties();
        String jsonmodle = "E:\\working\\others\\写作\\calcitetutorial\\src\\main\\resources\\bookshop.json";
        try {
            Connection connection =
                    DriverManager.getConnection("jdbc:calcite:model="+jsonmodle, info);
            CalciteConnection calciteConn = connection.unwrap(CalciteConnection.class);

            ResultSet  result = null;

//            result = connection.getMetaData().getTables(null, null, null, null);
//            while(result.next()) {
//                System.out.println("Catalog : " + result.getString(1) + ",Database : " + result.getString(2) + ",Table : " + result.getString(3));
//            }
//            result.close();


//            result = connection.getMetaData().getColumns(null, "bookshop", "book", null);
//            while(result.next()) {
//                System.out.println("name : " + result.getString(4) + ", type : " + result.getString(5) + ", typename : " + result.getString(6));
//            }
//            result.close();
//
//            result = connection.getMetaData().getColumns(null, "bookshop", "book", null);
//            while(result.next()) {
//                System.out.println("name : " + result.getString(4) + ", type : " + result.getString(5) + ", typename : " + result.getString(6));
//            }
//            result.close();
               Statement st = connection.createStatement();

//            result = st.executeQuery("select * from book as b");
//            while(result.next()) {
//                System.out.println(result.getString(1) + "\t" + result.getString(2) + "\t" + result.getString(3));
//            }
//            result.close();
            //connection.close();


            st = connection.createStatement();
            result = st.executeQuery("select a.name from author as a");
            while(result.next()) {
                System.out.println(result.getString(1));
            }
            result.close();


            st = connection.createStatement();
            result = st.executeQuery("select b.name,a.name from book as b inner join author as a on b.aid = a.id ");
            while(result.next()) {
                System.out.println(result.getString(1) + "\t" + result.getString(2) + "\t" );
            }
            result.close();



            st = connection.createStatement();
            //iso-8859-1
            //"+new String("数据山".getBytes(),"utf-8")+"
            result = st.executeQuery("select * from \"BOOK\" as b where b.name = '数据山'");
            while(result.next()) {
                System.out.println(result.getString(1) + "\t" + result.getString(2) + "\t" + result.getString(3) + "\t"  );
            }
            result.close();

            //group by b.type
            result = st.executeQuery("select sum(b.id) from \"BOOK\" as b ");
            while(result.next()) {
                System.out.println(result.getString(1) );
            }
            result.close();


            result = st.executeQuery("select sum(b.id),b.type from \"BOOK\" as b group by b.type");
            while(result.next()) {
                System.out.println(result.getString(1) + "\t" + result.getString(2) + "\t"  );
            }
            result.close();


            result = st.executeQuery("select sum(b.id),b.type,1 from \"BOOK\" as b group by b.type order by sum(b.id)");
            while(result.next()) {
                System.out.println(result.getString(1) + "\t" + result.getString(2) + "\t" + result.getString(3) + "\t" );
            }
            result.close();

            connection.close();

        }catch(Exception e){
            e.printStackTrace();
        }

//        System.out.println(new Date().getTime());
    }
}

package com.dafei1288.calcite;

import org.apache.calcite.jdbc.CalciteConnection;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

public class TestJDBC {
    public static void main(String[] args) {
        try {
            Class.forName("org.apache.calcite.jdbc.Driver");
        } catch (ClassNotFoundException e1) {
            e1.printStackTrace();
        }

        Properties info = new Properties();
        String jsonmodle = "E:\\working\\others\\写作\\calcitetutorial\\src\\main\\resources\\bookshop.json";
        try {
            Connection connection =
                    DriverManager.getConnection("jdbc:calcite:model="+jsonmodle, info);
            CalciteConnection calciteConn = connection.unwrap(CalciteConnection.class);

            ResultSet result = connection.getMetaData().getTables(null, null, null, null);
            while(result.next()) {
                System.out.println("Catalog : " + result.getString(1) + ",Database : " + result.getString(2) + ",Table : " + result.getString(3));
            }
            result.close();


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
            result = st.executeQuery("select * from book as b");
            while(result.next()) {
                System.out.println(result.getString(1) + "\t" + result.getString(2) + "\t" + result.getString(3));
            }
            result.close();
            //connection.close();


            st = connection.createStatement();
            result = st.executeQuery("select a.name from author as a");
            while(result.next()) {
                System.out.println(result.getString(1));
            }
            result.close();


//            st = connection.createStatement();
//            result = st.executeQuery("select b.name,a.name from book as b inner join author as a on b.aid = a.id ");
//            while(result.next()) {
//                System.out.println(result.getString(1) + "\t" + result.getString(2) + "\t" + result.getString(3));
//            }
//            result.close();-

            connection.close();
        }catch(Exception e){
            e.printStackTrace();
        }
    }
}

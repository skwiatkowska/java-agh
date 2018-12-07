package dataframe;

import java.io.File;
import java.io.FileWriter;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class A {
    private static List<String> resultSetArray=new ArrayList<>();
    String url = "jdbc:sqlite:C:\\Users\\Win10\\Desktop\\a.db";

    static Connection conn;

    public static void connect() {
        String url = "jdbc:sqlite:C:\\Users\\Win10\\Desktop\\a.db";
        conn = null;
        try {
            conn = DriverManager.getConnection(url);
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

    private static void fetchDataFromDatabase(String selectQuery) throws  Exception{
        try {


            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(selectQuery);
            int numCols = rs.getMetaData().getColumnCount();

            while(rs.next()) {
                //StringBuilder sb = new StringBuilder();

                /*for (int i = 1; i <= numCols; i++) {
                    sb.append(String.format(String.valueOf(rs.getString(i))) + " ");

                }*/
                //System.out.println(rs.getMetaData().getColumnName(1));
                resultSetArray.add(rs.getString(1));

            }

        } catch (SQLException e) {
            System.out.println("Sql exception " + e.getMessage());
        }

    }


    public static void printToCsv(List<String> resultArray) throws Exception{

        File csvOutputFile = new File("C:\\Users\\Win10\\Desktop\\1.csv");
        FileWriter fileWriter = new FileWriter(csvOutputFile, false);


        for(String mapping : resultArray) {
            fileWriter.write(mapping + "\n");
        }

        fileWriter.close();

    }
    public static void main(String args[]) throws Exception{
        connect();
        fetchDataFromDatabase("SELECT * FROM dataframe LIMIT 10");
        printToCsv(resultSetArray);

    }
}

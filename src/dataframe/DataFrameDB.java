package dataframe;

import java.sql.*;
import java.util.ArrayList;

public class DataFrameDB extends DataFrame {
    private Connection conn;
    private String tableName;


    public DataFrameDB(DataFrame df, String _tableName) {
        types = df.types;
        names = df.names;
        data = df.data;
        tableName = _tableName;
        connect();
        createNewTable();
        loadDataFrameToDataBase();
    }


    public static DataFrame fetchQueryResultToDataFrame(ResultSet rs) throws SQLException {
        int numCols = rs.getMetaData().getColumnCount();
        ArrayList<String> newNames = new ArrayList<>();
        for (int i = 0; i < numCols; i++)
            newNames.add(rs.getMetaData().getColumnName(i + 1));

        ArrayList<Class<? extends Value>> newTypes = new ArrayList<>();
        for (int i = 1; i <= numCols; i++) {
            if (rs.getString(i).contains("."))
                newTypes.add(new DoubleValue().getClass());
            else if (rs.getString(i).contains("-"))
                newTypes.add(new DateTimeValue().getClass());
            else if (rs.getString(i).matches("[0-9]+"))
                newTypes.add(new IntegerValue().getClass());
            else
                newTypes.add(new StringValue().getClass());
        }
        DataFrame newDf = new DataFrame(newNames, newTypes);

        try {
            while (rs.next()) {
                for (int i = 0; i < newDf.types.size(); i++) {
                    newDf.data.get(i).add(newDf.createValueOfExtactType(rs.getString(i + 1), i));
                }
            }

        } catch (SQLException e) {
            System.out.println("Sql exception " + e.getMessage());
        }
        return newDf;
    }


    public void connect() {
        String url = "jdbc:sqlite:C:\\Users\\Win10\\Desktop\\a.db";
        conn = null;
        try {
            conn = DriverManager.getConnection(url);
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }


    public String computeDBType(int index) {
        if (types.get(index).getName().equals("dataframe.StringValue") || types.get(index).getName().equals("dataframe.DateTimeValue"))
            return "text";
        else if (types.get(index).getName().equals("dataframe.DoubleValue") || types.get(index).getName().equals("dataframe.FloatValue"))
            return "real";
        else if (types.get(index).getName().equals("dataframe.IntegerValue"))
            return "integer";
        else return "";
    }


    public void createNewTable() {
        if (conn == null)
            connect();
        else {
            StringBuilder stringBuilder = new StringBuilder("CREATE TABLE IF NOT EXISTS " + tableName + " (");
            for (int i = 0; i < names.size() - 1; i++) {
                stringBuilder.append(names.get(i));
                stringBuilder.append(" ");
                stringBuilder.append(computeDBType(i));
                stringBuilder.append(", ");
            }
            stringBuilder.append(names.get(names.size() - 1));
            stringBuilder.append(" ");
            stringBuilder.append(computeDBType(names.size() - 1));
            stringBuilder.append(");");

            String query = stringBuilder.toString();
            try (Statement stmt = conn.createStatement()) {
                stmt.execute(query);
            } catch (SQLException e) {
                System.out.println(e.getMessage());
            }
        }
    }


    public void insert(DataFrame row) {
        if (conn == null)
            connect();
        else {
            StringBuilder stringBuilder = new StringBuilder("INSERT INTO " + tableName + " VALUES(");
            for (int i = 0; i < row.names.size() - 1; i++) {
                stringBuilder.append("?,");
            }
            stringBuilder.append("?)");
            String query = stringBuilder.toString();

            try (PreparedStatement preparedStatement = conn.prepareStatement(query)) {
                for (int i = 0; i < row.names.size(); i++) {
                    if (row.types.get(i).getName().equals("dataframe.StringValue")
                            || row.types.get(i).getName().equals("dataframe.DateTimeValue"))
                        preparedStatement.setString(i + 1, row.data.get(i).get(0).toString());
                    else if (row.types.get(i).getName().equals("dataframe.IntegerValue"))
                        preparedStatement.setInt(i + 1, Integer.parseInt(row.data.get(i).get(0).toString()));
                    else if (row.types.get(i).getName().equals("dataframe.DoubleValue")
                            || row.types.get(i).getName().equals("dataframe.FloatValue"))
                        preparedStatement.setDouble(i + 1, Double.parseDouble(row.data.get(i).get(0).toString()));
                }
                preparedStatement.executeUpdate();
            } catch (SQLException e) {
                System.out.println(e.getMessage());
            }
        }
    }


    public void loadDataFrameToDataBase() {
        if (conn == null)
            connect();
        else {
            for (int i = 0; i < data.get(0).size(); i++)
                insert(iloc(i));
            System.out.println("Add " + data.get(0).size() + " rows");
        }
    }


    public DataFrame select(String query) {
        DataFrame queryResult = null;
        if (conn == null)
            connect();
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(query)) {
            queryResult = fetchQueryResultToDataFrame(rs);


        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
        return queryResult;
    }


    public void min(String colname) {
        if (conn == null)
            connect();
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT MIN(" + colname + ") FROM " + tableName + ";")) {
            System.out.println(rs.getString(1));

        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }


    public void max(String colname) {
        if (conn == null)
            connect();
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT MAX(" + colname + ") FROM " + tableName + ";")) {
            System.out.println(rs.getString(1));

        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }


    public DataFrame groupBySql(String groupingColNames, String statFunction) {
        String[] groupingColSplit = groupingColNames.split(", ");
        ArrayList<Integer> groupingColIndexes = new ArrayList<>();
        for (int i = 0; i < groupingColSplit.length; i++) {
            if (groupingColSplit[i].toUpperCase().equals("MEAN"))
                groupingColSplit[i] = "AVG";
            groupingColIndexes.add(getIndexOfExactCol(groupingColSplit[i]));
        }
        DataFrame queryResult = null;
        StringBuilder sb = new StringBuilder();
        if (statFunction.toLowerCase().equals("max") || statFunction.toLowerCase().equals("min")) {
            for (int i = 0; i < types.size(); i++) {
                if (types.get(i).getName().equals("dataframe.StringValue") && groupingColIndexes.contains(i)) {
                    sb.append(names.get(i));
                    if (i != types.size() - 1)
                        sb.append(", ");
                }
                else if (!(types.get(i).getName().equals("dataframe.StringValue"))) {
                    sb.append(statFunction.toUpperCase());
                    sb.append("(");
                    sb.append(names.get(i));
                    sb.append(")");
                    if (i != types.size() - 1)
                        sb.append(", ");
                }
            }
        }
        else {
            for (int i = 0; i < types.size() - 1; i++) {
                if ((types.get(i).getName().equals("dataframe.StringValue") || types.get(i).getName().equals("dataframe.DateTimeValue"))
                        && groupingColIndexes.contains(i)) {
                    sb.append(names.get(i));
                    if (i != types.size() - 1)
                        sb.append(", ");
                }

                if (!(types.get(i).getName().equals("dataframe.StringValue")) &&
                        !(types.get(i).getName().equals("dataframe.DateTimeValue"))) {
                    sb.append(statFunction.toUpperCase());
                    sb.append("(");
                    sb.append(names.get(i));
                    sb.append(")");
                    if (i != types.size() - 1)
                        sb.append(", ");
                }
            }
        }

        if (conn == null)
            connect();
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT " + sb.toString() + " FROM " + tableName +
                     " group by " + groupingColNames + ";")) {
            queryResult = fetchQueryResultToDataFrame(rs);


        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
        return queryResult;
    }


    public void dropTable() {
        try {
            connect();
            ResultSet tables = conn.getMetaData().getTables(null, null, tableName, null);
            if (tables.next()) {
                try (
                        Statement stmt = conn.createStatement();
                        ResultSet rs = stmt.executeQuery("DROP TABLE " + tableName + ";")) {
                    tableName = null;
                    System.out.println("The " + tableName + "table has been deleted successfully");
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}

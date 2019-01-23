package dataframe;

import java.util.ArrayList;
import java.util.Arrays;


public class Main {

    public static void main(String[] argv) {
        ArrayList<Class<? extends Value>> types2 = new ArrayList<>();
        types2.add(new StringValue().getClass());
        types2.add(new DateTimeValue().getClass());
        types2.add(new FloatValue().getClass());
        types2.add(new FloatValue().getClass());


        // DataFrame d = db.select("SELECT id, max(total) FROM dataframe group by id");
        //d.print("");
        //DataFrame d2 = db.select("SELECT id, max(date), max(total), max(val) FROM dataframe group by id, date");
        //d2.print("");
        //DataFrame d3 = db.groupBySql("id, date", "max");
        //d3.print("");


        DataFrame df1 = new DataFrame("C:\\Users\\Win10\\Documents\\java-agh\\src\\dataframe\\files-for-testing\\groupby.csv", types2);
        DataFrameThreads df2 = new DataFrameThreads("C:\\Users\\Win10\\Documents\\java-agh\\src\\dataframe\\files-for-testing\\groupby.csv", types2);

       // for(int i = 0; i < 2; i++){
        long startTime = System.nanoTime();
        df1.groupBy(new String[]{"id"});
        long endTime = System.nanoTime();
        System.out.println("DataFrame: "+(endTime - startTime)/1000000);
        long startTime2 = System.nanoTime();
        df2.groupBy(new String[]{"id"});
        long endTime2 = System.nanoTime();
        System.out.println("DataFrameThreads: "+(endTime2 - startTime2)/1000000);
       // }
    }
}


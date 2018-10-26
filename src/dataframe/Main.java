package dataframe;

import java.util.ArrayList;


public class Main {
    public static void main(String[] argv){
        IntegerValue intv = new IntegerValue();
        DoubleValue dbv = new DoubleValue();
        DateTimeValue datev = new DateTimeValue();

        ArrayList<Class<? extends Value>> types1 = new ArrayList<Class<? extends Value>>();
        types1.add(intv.getClass());
        types1.add(dbv.getClass());
        types1.add(dbv.getClass());
        types1.add(datev.getClass());

        DataFrame df1 = new DataFrame(new String[] {"kol1", "kol2", "kol3", "kol4"}, types1);
        df1.data.get(0).add(DataFrame.getInstance(df1.types.get(0)).create("2"));
        df1.data.get(1).add(DataFrame.getInstance(df1.types.get(1)).create("2.2"));
        df1.data.get(2).add(DataFrame.getInstance(df1.types.get(2)).create("1.1"));
        df1.data.get(3).add(DataFrame.getInstance(df1.types.get(3)).create("11.11.2011"));
        df1.data.get(0).add(DataFrame.getInstance(df1.types.get(0)).create("9"));
        df1.data.get(1).add(DataFrame.getInstance(df1.types.get(1)).create("5.5"));
        df1.data.get(2).add(DataFrame.getInstance(df1.types.get(2)).create("0.3"));
        df1.data.get(3).add(DataFrame.getInstance(df1.types.get(3)).create("5.03.2000"));

        df1.print("DF1");


        ArrayList<Class<? extends Value>> types2 = new ArrayList<Class<? extends Value>>();
        types2.add(intv.getClass());
        types2.add(intv.getClass());

        SparseDataFrame sdf1 = new SparseDataFrame(new String[]{"kol1", "kol2"}, types2, "0");
        sdf1.data.get(0).add(new COOValue(1,DataFrame.getInstance(sdf1.types.get(0)).create("1")));
        sdf1.data.get(1).add(new COOValue(0,DataFrame.getInstance(sdf1.types.get(0)).create("2")));
        sdf1.data.get(0).add(new COOValue(4,DataFrame.getInstance(sdf1.types.get(0)).create("3")));
        sdf1.data.get(1).add(new COOValue(7,DataFrame.getInstance(sdf1.types.get(0)).create("4")));
        sdf1.print("SDF");

        DataFrame twoCols = df1.get(new String[]{"kol1", "kol2"}, true);
        twoCols.print("Two cols from DF1");

        DataFrame df2 = sdf1.toDense();
        df2.print("DF2 received from SDF1");

        SparseDataFrame sdf2 = new SparseDataFrame(df2, "0");
        sdf2.print("SDF2 received from DF2");

        ArrayList<Class<? extends Value>> types3 = new ArrayList<Class<? extends Value>>();
        types3.add(dbv.getClass());
        types3.add(dbv.getClass());
        types3.add(dbv.getClass());

        DataFrame df3 = new DataFrame("C:\\Users\\Win10\\Documents\\java-agh\\src\\dataframe\\data.csv",
                types3);
        System.out.println("data.csv:\tLoaded " + df3.size() + " records.");

        DataFrame firstRow = df3.iloc(0);
        firstRow.print("data.csv:\tFirst row: ");

        SparseDataFrame sdf3 = new SparseDataFrame("C:\\Users\\Win10\\Documents\\java-agh\\src\\dataframe\\sparse.csv",
                types3, "0.0");
        System.out.println("sparse.csv:\tLoaded " + sdf3.size() + " records.");

        System.out.print("sparse.csv:\tColumns' sizes after conversion: ");
        for(int i = 0; i < sdf3.names.length; i++){
            System.out.print(sdf3.data.get(i).size() + "\t");
        }
    }
}

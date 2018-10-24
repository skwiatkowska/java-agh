package dataframe;


import java.util.ArrayList;


public class Main {
    public static void main(String[] argv){
        DataFrame df1 = new DataFrame(new String[]{"kol1", "kol2", "kol3"},
                new String[]{"int", "double", "MyCustomType"});
        df1.data.get(0).add(12);
        df1.data.get(1).add(12.2);
        df1.data.get(2).add("aa");
        df1.data.get(0).add(5);
        df1.data.get(1).add(1.2);
        df1.data.get(2).add("bb");

        DataFrame firstCol = df1.get(new String[]{"kol1", "kol2"}, true);
        firstCol.print();

        SparseDataFrame sdf = new SparseDataFrame(new String[]{"kol1", "kol2"}, new String[] {"int", "int"}, "0");
        sdf.data.get(0).add(new COOValue(1,"1"));
        sdf.data.get(1).add(new COOValue(0,"2"));
        sdf.data.get(0).add(new COOValue(4,"3"));
        sdf.data.get(1).add(new COOValue(7,"4"));

        System.out.println("SDF: ");
        sdf.print();


        DataFrame df = sdf.toDense();
        System.out.println("DF: ");
        df.print();

        DataFrame oneCol = sdf.get(new String[]{"kol1"}, true);
        oneCol.print();


        SparseDataFrame sdf2 = new SparseDataFrame(df, "0");
        System.out.println("SDF2: ");
        sdf.print();


        DataFrame df2 = new DataFrame("C:\\Users\\Win10\\Documents\\java-agh\\src\\dataframe\\data.csv",
                new String[]{"double", "double", "double"});
        System.out.println("data.csv:\tLoaded " + df2.size() + " records.");

        System.out.println("data.csv:\tFirst row: ");
        DataFrame firstRow = df2.iloc(0);
        firstRow.print();

        SparseDataFrame sdf3 = new SparseDataFrame("C:\\Users\\Win10\\Documents\\java-agh\\src\\dataframe\\sparse.csv",
                new String[]{"double", "double", "double"}, "0.0");
        System.out.println("sparse.csv:\tLoaded " + sdf3.size() + " records.");

        System.out.print("sparse.csv:\tColumns' sizes after conversion: ");
        for(int i = 0; i < sdf3.names.length; i++){
            System.out.print(sdf3.data.get(i).size() + "\t");
        }
    }
}

package dataframe;

import java.util.ArrayList;


public class Main {

    public static void main(String[] argv) {
        IntegerValue intv = new IntegerValue();
        DoubleValue dbv = new DoubleValue();
        FloatValue flv = new FloatValue();
        DateTimeValue datev = new DateTimeValue();
        StringValue strv = new StringValue();


        ArrayList<Class<? extends Value>> types1 = new ArrayList<>();
        types1.add(strv.getClass());
        types1.add(intv.getClass());
        types1.add(intv.getClass());
        ArrayList<String> names1 = new ArrayList<>();
        names1.add("id");
        names1.add("v1");
        names1.add("v2");
        DataFrame df1 = new DataFrame(names1, types1);


        df1.addRow(new String[]{"a", "10", "20"});
        df1.addRow(new String[]{"b", "11", "100"});
        df1.addRow(new String[]{"a", "30", "1000"});
        df1.addRow(new String[]{"b", "7", "-1"});
        df1.addRow(new String[]{"b", "7", "0"});
        df1.addRow(new String[]{"c", "3", "-7"});



        System.out.println("\nChecking arithmetic operations on second and third column: ");
        System.out.println("Multiplying: ");
        ArrayList<Value> col1Xcol2 = df1.mulColByCol(1,2);
        for (Value v : col1Xcol2)
            System.out.print(v + "\t");

        System.out.println("\nAdding: ");
        ArrayList<Value> col1Pluscol2 = df1.addTwoCols(1,2);
        for (Value v : col1Pluscol2)
            System.out.print(v + "\t");

        System.out.println("\nSubtracting: ");
        ArrayList<Value> col1Minuscol2 = df1.subTwoCols(1,2);
        for (Value v : col1Minuscol2)
            System.out.print(v + "\t");


        System.out.println();
        DataFrame grp1 = df1.groupBy("id").max();

        ArrayList<Class<? extends Value>> types2 = new ArrayList<>();
        types2.add(strv.getClass());
        types2.add(datev.getClass());
        types2.add(flv.getClass());
        types2.add(flv.getClass());

        DataFrame df2 = new DataFrame("C:\\Users\\Win10\\Documents\\java-agh\\src\\dataframe\\files-for-testing\\groupby.csv", types2);
        System.out.println("Loaded " + df2.size() + " records.");


        DataFrame grp2 = df2.groupBy("id").max();


        ArrayList<Class<? extends Value>> types3 = new ArrayList<>();
        types3.add(strv.getClass());
        types3.add(datev.getClass());
        types3.add(dbv.getClass());
        types3.add(dbv.getClass());
        DataFrame df3 = new DataFrame("C:\\Users\\Win10\\Documents\\java-agh\\src\\dataframe\\files-for-testing\\groubymulti.csv", types3);
        System.out.println("Loaded " + df3.size() + " records.");

        DataFrame grp3 = df3.groupBy(new String[]{"id","date"}).min();

    }
}

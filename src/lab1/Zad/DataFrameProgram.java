package lab1.Zad;

public class DataFrameProgram {
    public static void main(String[] argv){
        DataFrame test = new DataFrame(new String[]{"kol1", "kol2", "kol3"},
                new String[]{"int", "double", "MyCustomType"});
        test.df.get(0).add(12);
        test.df.get(1).add(12.2);
        test.df.get(2).add("a");
        test.df.get(0).add(11);
        System.out.println(test.get("kol2").get(0));

    }
}

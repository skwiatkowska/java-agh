package lab1.Zad;

import java.util.ArrayList;


public class DataFrame {
    private String[] names;
    private String[] types;
    ArrayList<ArrayList<Object>> df = new ArrayList<ArrayList<Object>>();

    DataFrame(String[] _names, String[] _types) {
        names = _names.clone();
        types = _types.clone();
        for (int i = 0; i < _names.length; i++){
            df.add(new ArrayList<Object>());
        }
    }

    public ArrayList get(String colname){
        for (int i = 0; i < names.length; i++){
            if (names[i].equals(colname)){
                return df.get(i);
            }
        }
        return new ArrayList();
    }

    public int size(){
        return df.get(0).size();
    }

    public DataFrame get(String [] cols, boolean copy){
        String[] newTypes = new String[cols.length]; //zmiana tablicy typow
        for (int i = 0; i < cols.length; i++){
            for (int j = 0; i < names.length; j++){
                if(cols[i].equals(names[j])){
                    newTypes[i] = types[j];
                }
            }
        }

        DataFrame newDf = new DataFrame(cols, newTypes);

        for (int i = 0; i < cols.length; i++) {
            for (int j = 0; i < names.length; j++) {
                if (cols[i].equals(names[j])) {
                    if (copy){
                        for (int k = 0; k < df.get(0).size(); k++) {
                            newDf.df.get(j).add(df.get(i).get(k));
                        }
                    }
                    else {
                        newDf.df.set(j, df.get(i));
                    }
                }

            }
        }
        return newDf;
    }



    public DataFrame iloc (int i){
        DataFrame newDf = new DataFrame(names, types);
        for (int j = 0; j < names.length; ++j){
            newDf.df.get(j).add(df.get(j).get(i));
        }
        return newDf;
    }

    public DataFrame iloc (int from, int to){
        DataFrame newDf = new DataFrame(names, types);
        for(int i = from; i <= to ; i++){
            for (int j = 0; j < names.length; ++j) {
                newDf.df.get(j).add(df.get(j).get(i));
            }
        }
        return newDf;
    }
}
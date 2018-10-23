package dataframe;

import java.io.*;
import java.util.ArrayList;


public class DataFrame {
    protected String[] names;
    protected String[] types;
    protected ArrayList<ArrayList<Object>> data = new ArrayList<ArrayList<Object>>();

    public DataFrame(String[] _names, String[] _types) {
        names = _names.clone();
        types = _types.clone();
        for (int i = 0; i < _names.length; i++) {
            data.add(new ArrayList<Object>());
        }
    }


    public DataFrame() { }


    public ArrayList get(String colname) {
        for (int i = 0; i < names.length; i++) {
            if (names[i].equals(colname)) {
                return data.get(i);
            }
        }
        return new ArrayList();
    }


    public int size() {
        return data.get(0).size();
    }


    public DataFrame get(String[] cols, boolean deepCopy) {
        String[] newTypes = new String[cols.length]; //zmiana tablicy typow
        for (int i = 0; i < cols.length; i++) {
            for (int j = 0; j < names.length; j++) {
                if (cols[i].equals(names[j])) {
                    newTypes[i] = types[j];
                }
            }
        }

        DataFrame newDf = new DataFrame(cols, newTypes);
        if (deepCopy) {
            newDf.data.clear();
            for (int i = 0; i < cols.length; i++) {
                for (int j = 0; j < names.length; j++) {
                    if (cols[i].equals(names[j])) {
                        newDf.data.add(new ArrayList<Object>());
                        newDf.names[i] = names[j];
                        newDf.types[i] = types[j];
                        newDf.data.set(i, data.get(j));
                    }
                }
            }
        } else {
            for (int i = 0; i < cols.length; i++) {
                for (int j = 0; j < names.length; j++) {
                    if (cols[i].equals(names[j])) {
                        newDf.data.set(i, data.get(j));
                    }
                }
            }
        }
        return newDf;
    }


    public DataFrame iloc(int i) {
        DataFrame newDf = new DataFrame(names, types);
        for (int j = 0; j < names.length; j++) {
            newDf.data.get(j).add(data.get(j).get(i));
        }
        return newDf;
    }


    public DataFrame iloc(int from, int to) {
        DataFrame newDf = new DataFrame(names, types);
        for (int i = from; i <= to; i++) {
            for (int j = 0; j < names.length; j++) {
                newDf.data.get(j).add(data.get(j).get(i));
            }
        }
        return newDf;
    }


    public DataFrame (String path, String[] _types, String[] header){
        names = header.clone();
        types = _types.clone();

        for (int i = 0; i < types.length; i++){
            data.add(new ArrayList<Object>());
        }

        String separator = ",";

        try {
            FileInputStream fstream = new FileInputStream(path);
            BufferedReader br = new BufferedReader(new InputStreamReader(fstream));

            String line;

            while ((line = br.readLine()) != null) {
                String[] row = line.split(separator);
                for(int i = 0; i < types.length; i++){
                    data.get(i).add((row[i]));
                }
            }
            br.close();

        } catch(FileNotFoundException e){
            System.out.println("No such file.");
        } catch(IOException e){
            e.printStackTrace();
        }
    }


    public DataFrame (String path, String[] _types) {
        types = _types.clone();

        for (int i = 0; i < types.length; i++){
            data.add(new ArrayList<Object>());
        }

        boolean header = true;
        String separator = ",";

        try {
            FileInputStream fstream = new FileInputStream(path);
            BufferedReader br = new BufferedReader(new InputStreamReader(fstream));

            String line;

            while ((line = br.readLine()) != null) {
                if(header){
                    String[] _names = line.split(separator);
                    names = _names.clone();
                    header = false;
                    continue;
                }
                String[] row = line.split(separator);
                for(int i = 0; i < types.length; i++){
                    data.get(i).add((row[i]));
                }
            }
            br.close();

        } catch(FileNotFoundException e){
            System.out.println("No such file.");
        } catch(IOException e){
            e.printStackTrace();
        }
    }


    public void print(){
        for (int i = 0; i < this.names.length; i++){
            System.out.print(this.names[i] + "\t");
            for(Object o : this.data.get(i)){
                System.out.print(o + " ");
            }
            System.out.println();
        }
        System.out.println();
    }


    public boolean checkIfAllElementsAreEqual(){
        for (int i = 1; i < types.length; i++){
            if(!(types[0].equals(types[i]))){
                return false;
            }
        }
        return true;
    }
}

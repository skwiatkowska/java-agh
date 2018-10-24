package dataframe;

import java.io.*;
import java.util.ArrayList;


public class DataFrame {
    protected String[] names;
    protected ArrayList<Class<? extends Value>> types;
    protected ArrayList<ArrayList<Value>> data = new ArrayList<ArrayList<Value>>();

    public DataFrame(String[] _names, ArrayList<Class<? extends Value>> _types) {
        names = _names.clone();
        types = _types;
        for (int i = 0; i < _names.length; i++) {
            data.add(new ArrayList<Value>());
        }
    }


    public DataFrame() {
    }


    public DataFrame(String path, ArrayList<Class<? extends Value>> _types, String[] header) {
        names = header.clone();
        types = _types;

        for (int i = 0; i < types.size(); i++) {
            data.add(new ArrayList<Value>());
        }

        String separator = ",";

        try {
            FileInputStream fstream = new FileInputStream(path);
            BufferedReader br = new BufferedReader(new InputStreamReader(fstream));

            String line;

            while ((line = br.readLine()) != null) {
                String[] row = line.split(separator);
                for (int i = 0; i < types.size(); i++) {
                    data.get(i).add(getInstance(types.get(i)).create(row[i]));
                }
            }
            br.close();

        } catch (FileNotFoundException e) {
            System.out.println("No such file.");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public DataFrame(String path, ArrayList<Class<? extends Value>> _types) {
        types = _types;

        for (int i = 0; i < types.size(); i++) {
            data.add(new ArrayList<Value>());
        }

        boolean header = true;
        String separator = ",";

        try {
            FileInputStream fstream = new FileInputStream(path);
            BufferedReader br = new BufferedReader(new InputStreamReader(fstream));

            String line;

            while ((line = br.readLine()) != null) {
                if (header) {
                    String[] _names = line.split(separator);
                    names = _names.clone();
                    header = false;
                    continue;
                }
                String[] row = line.split(separator);
                for (int i = 0; i < types.size(); i++) {
                    data.get(i).add(getInstance(types.get(i)).create(row[i]));
                }
            }
            br.close();

        } catch (FileNotFoundException e) {
            System.out.println("No such file.");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static Value getInstance(Class<? extends Value> c) {
        switch (c.getName()) {
            case "dataframe.IntegerValue":
                return new IntegerValue();
            case "dataframe.DoubleValue":
                return new DoubleValue();
            case "dataframe.StringValue":
                return new StringValue();
            case "dataframe.FloatValue":
                return new FloatValue();
            case "dataframe.DateTimeValue":
                return new DateTimeValue();
            default:
                throw new IllegalArgumentException("Unknown class type.");
        }
    }

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

    public DataFrame get(String[] cols, boolean deepCopy) { //cols - nazwy kolumn
        ArrayList<Class<? extends Value>> newTypes = new ArrayList<>();

        for (int i = 0; i < cols.length; i++) {
            for (int j = 0; j < names.length; j++) {
                if (cols[i].equals(names[j])) {
                    newTypes.add(types.get(j));
                }
            }
        }

        DataFrame newDf = new DataFrame(cols, newTypes);
        createPartialFrame(cols, deepCopy, newDf);
        return newDf;
    }

    public void createPartialFrame(String[] cols, boolean deepCopy, DataFrame _df) {
        if (deepCopy) {
            _df.data.clear();
            for (int i = 0; i < cols.length; i++) {
                for (int j = 0; j < names.length; j++) {
                    if (cols[i].equals(names[j])) {
                        _df.data.add(new ArrayList<Value>());
                        _df.names[i] = names[j];
                        _df.types.set(i, types.get(j));
                        _df.data.set(i, data.get(j));
                    }
                }
            }
        }
        else {
            for (int i = 0; i < cols.length; i++) {
                for (int j = 0; j < names.length; j++) {
                    if (cols[i].equals(names[j])) {
                        _df.data.set(i, data.get(j));
                    }
                }
            }
        }
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

    public void print(String name) {
        System.out.println(name + ": ");
        for (int i = 0; i < this.names.length; i++) {
            System.out.print(this.names[i] + "\t");
            for (Object o : this.data.get(i)) {
                System.out.print(o + "\t");
            }
            System.out.println();
        }
        System.out.println();
    }


    public boolean checkIfAllElementsAreEqual() {
        for (int i = 1; i < types.size(); i++) {
            if (!(types.get(0).equals(types.get(i)))) {
                return false;
            }
        }
        return true;
    }
}
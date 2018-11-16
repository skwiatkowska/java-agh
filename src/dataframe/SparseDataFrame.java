package dataframe;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;


public class SparseDataFrame extends DataFrame {
    private String hide;


    public SparseDataFrame(ArrayList<String> _names, ArrayList<Class<? extends Value>> _types, String _hide) {
        super(_names, _types);
        hide = _hide;
    }


    public SparseDataFrame(DataFrame _df, String _hide) {
        super(_df.names, _df.types);

        if (!_df.checkIfAllElementsAreEqual()) {
            throw new IllegalArgumentException("DataFrame's types are not equal. Cannot convert DataFrame to SparseDataFrame");
        }
        hide = _hide;

        for (int i = 0; i < names.size(); i++) {
            for (int j = 0; j < _df.data.get(0).size(); j++) {
                if (!(_df.data.get(i).get(j).equals(Value.getInstance(_df.types.get(i)).create(hide)))) {
                    data.get(i).add(new COOValue(j, _df.data.get(i).get(j)));
                }
            }
        }
    }


    public SparseDataFrame(String path, ArrayList<Class<? extends Value>> _types, ArrayList<String> header, String _hide) {
        names = header;
        types = _types;
        hide = _hide;

        for (int i = 0; i < types.size(); i++) {
            data.add(new ArrayList<Value>());
        }

        String separator = ",";

        BufferedReader br = null;
        try {
            //FileInputStream fstream = new FileInputStream(path);
            br = new BufferedReader(new FileReader(path));

            String line;
            int rowsCounter = 0;

            while ((line = br.readLine()) != null) {
                String[] row = line.split(separator);
                for (int i = 0; i < row.length; i++) {
                    if (!(row[i].equals(hide))) {
                        data.get(i).add(new COOValue(rowsCounter, createValueOfExtactType(row[i], i)));
                    }
                }
                rowsCounter++;
            }
        } catch (FileNotFoundException e) {
            System.out.println("Wrong file path or there is not such file.");
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }


    public SparseDataFrame(String path, ArrayList<Class<? extends Value>> _types, String _hide) {
        types = _types;
        hide = _hide;

        for (int i = 0; i < types.size(); i++) {
            data.add(new ArrayList<Value>());
        }

        boolean header = true;
        String separator = ",";

        BufferedReader br = null;
        try {
            //FileInputStream fstream = new FileInputStream(path);
            br = new BufferedReader(new FileReader(path));

            String line;
            int rowsCounter = 0;

            while ((line = br.readLine()) != null) {
                if (header) {
                    String[] _names = line.split(separator);
                    names = new ArrayList<String>(Arrays.asList(_names));
                    header = false;
                    continue;
                }
                String[] row = line.split(separator);
                for (int i = 0; i < row.length; i++) {
                    if (!(row[i].equals(hide))) {
                        data.get(i).add(new COOValue(rowsCounter, createValueOfExtactType(row[i], i)));
                    }
                }
                rowsCounter++;
            }
        } catch (FileNotFoundException e) {
            System.out.println("Wrong file path or there is not such file.");
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }


    @Override
    public int size() { //returns the largest "first" from the entire SDF
        int theLargest = ((COOValue) data.get(0).get(data.get(0).size() - 1)).first;
        for (int i = 1; i < names.size(); i++) {
            if (((COOValue) data.get(i).get(data.get(i).size() - 1)).first > theLargest) {
                theLargest = ((COOValue) data.get(i).get(data.get(i).size() - 1)).first + 1;
            }
        }
        return theLargest;
    }


    public DataFrame toDense() { //SDF -> DF
        DataFrame newDf = new DataFrame(names, types);
        int index;
        for (int i = 0; i < names.size(); i++) {
            index = 0;
            for (int j = 0; j < data.get(i).size(); j++) {
                while (index < ((COOValue) data.get(i).get(j)).first) {
                    newDf.data.get(i).add(Value.getInstance(newDf.types.get(i)).create(hide));
                    index++;
                }
                newDf.data.get(i).add(((COOValue) data.get(i).get(j)).second);
                index++;
            }
        }

        int SDFSize = this.size();
        for (int i = 0; i < names.size(); i++) {
            while (newDf.data.get(i).size() < SDFSize) {
                newDf.data.get(i).add(Value.getInstance(newDf.types.get(i)).create(hide));
            }
        }
        return newDf;
    }


    public void print(String name) {
        for (int i = 0; i < this.names.size(); i++) {
            for (Object c : this.data.get(i)) {
                System.out.print("(" + ((COOValue) c).first + ", " + ((COOValue) c).second + ")" + "\t");
            }
            System.out.println();
        }
        System.out.println();
    }


    public ArrayList get(String colname) {
        for (int i = 0; i < names.size(); i++) {
            if (names.get(i).equals(colname)) {
                ArrayList<Class<? extends Value>> _type = new ArrayList<>(1);
                _type.add(types.get(i));
                SparseDataFrame newSdf = new SparseDataFrame(names.get(i), _type, hide);
                newSdf.data.get(0).addAll(data.get(i));
                DataFrame df = newSdf.toDense();
                return df.data.get(0);
            }
        }
        return new ArrayList();
    }


    @Override
    public DataFrame get(ArrayList<String> cols, boolean deepCopy) {
        ArrayList<Class<? extends Value>> newTypes = new ArrayList<>();

        for (int i = 0; i < cols.size(); i++) {
            for (int j = 0; j < names.size(); j++) {
                if (cols.get(i).equals(names.get(j))) {
                    newTypes.add(types.get(j));
                }
            }
        }

        SparseDataFrame newSdf = new SparseDataFrame(cols, newTypes, hide);
        createPartialFrame(cols, deepCopy, newSdf);

        DataFrame newDf = newSdf.toDense();
        int SDFSize = this.size();
        for (int i = 0; i < newDf.names.size(); i++) {
            while (newDf.data.get(i).size() < SDFSize) {
                newDf.data.get(i).add(Value.getInstance(newDf.types.get(i)).create(hide));
            }
        }
        return newDf;
    }


    public DataFrame iloc(int i) {
        DataFrame newDf = new DataFrame(names, types);
        for (int j = 0; j < names.size(); ++j) {
            newDf.data.get(j).add(Value.getInstance(newDf.types.get(i)).create(hide));
        }
        for (int j = 0; j < names.size(); ++j) {
            for (int k = 0; k < data.get(j).size(); k++) {
                if (((COOValue) data.get(j).get(k)).first == i) {
                    newDf.data.get(j).set(0, ((COOValue) data.get(j).get(k)).second);
                }
            }
        }
        return newDf;
    }


    public DataFrame iloc(int from, int to) {
        DataFrame newDf = new DataFrame(names, types);
        for (int i = from; i <= to; i++) {
            for (int j = 0; j < names.size(); j++) {
                newDf.data.get(j).add(Value.getInstance(newDf.types.get(i)).create(hide));
            }
        }

        int m = 0;
        for (int i = from; i <= to; i++) {
            for (int j = 0; j < names.size(); j++) {
                for (int k = 0; k < data.get(j).size(); k++) {
                    if (((COOValue) data.get(j).get(k)).first == i) {
                        newDf.data.get(j).set(m, ((COOValue) data.get(j).get(k)).second);
                    }
                }
            }
            m++;
        }
        return newDf;
    }
}

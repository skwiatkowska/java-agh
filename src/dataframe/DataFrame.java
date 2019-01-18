package dataframe;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.TreeMap;


public class DataFrame {
    public ArrayList<String> names;
    public ArrayList<Class<? extends Value>> types;
    public ArrayList<ArrayList<Value>> data = new ArrayList<>();


    public DataFrame(ArrayList<String> _names, ArrayList<Class<? extends Value>> _types) {
        names = _names;
        types = _types;
        for (int i = 0; i < _names.size(); i++) {
            data.add(new ArrayList<>());
        }
    }


    public DataFrame() {
    }


    public DataFrame(String path, ArrayList<Class<? extends Value>> _types, ArrayList<String> header) {
        names = header;
        types = _types;

        for (int i = 0; i < types.size(); i++) {
            data.add(new ArrayList<Value>());
        }

        String separator = ",";
        BufferedReader br = null;
        try {
            br = new BufferedReader(new FileReader(path));

            String line;

            int rowNumber = 0;
            while ((line = br.readLine()) != null) {
                String[] row = line.split(separator);
                if(row.length != types.size())
                    throw new ArrayIndexOutOfBoundsException("Wrong row " + rowNumber + " size.");
                for (int i = 0; i < types.size(); i++) {
                    data.get(i).add(createValueOfExtactType(row[i], i));
                }
                rowNumber++;
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


    public DataFrame(String path, ArrayList<Class<? extends Value>> _types) {
        types = _types;

        for (int i = 0; i < types.size(); i++) {
            data.add(new ArrayList<Value>());
        }

        boolean header = true;
        String separator = ",";
        BufferedReader br = null;
        try {
            br = new BufferedReader(new FileReader(path));

            String line;

            int rowNumber = 0;
            while ((line = br.readLine()) != null) {
                if (header) {
                    String[] _names = line.split(separator);
                    names = new ArrayList<>(Arrays.asList(_names));
                    header = false;
                    continue;
                }
                String[] row = line.split(separator);
                if(row.length != types.size())
                    throw new ArrayIndexOutOfBoundsException("Wrong row " + rowNumber + " size.");
                for (int i = 0; i < types.size(); i++) {
                    data.get(i).add(createValueOfExtactType(row[i], i));
                }
                rowNumber++;
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


    public static Value getMaxValFromCol(ArrayList<Value> list) {
        if (list == null || list.isEmpty()) {
            throw new NullPointerException("Null or empty column.");
        }
        Value max = list.get(0);
        for (int i = 1; i < list.size(); i++) {
            if (max.lt(list.get(i))) {
                max = list.get(i);
            }
        }
        return max;
    }


    public static Value getMinValFromCol(ArrayList<Value> list) {
        if (list == null || list.isEmpty()) {
            throw new NullPointerException("Null or empty column.");
        }
        Value max = list.get(0);
        for (int i = 1; i < list.size(); i++) {
            if (list.get(i).lt(max)) {
                max = list.get(i);
            }
        }
        return max;
    }


    public static Value summarizeColValues(ArrayList<Value> list) {
        if (list == null || list.isEmpty()) {
            throw new NullPointerException("Null or empty column.");
        }
        Value sum = Value.getInstance(list.get(0).getClass());
        for (Value v : list) {
            sum = sum.add(v);
        }
        return sum;
    }


    public static Value getMeanFromCol(ArrayList<Value> list) {
        if (list == null || list.isEmpty()) {
            throw new NullPointerException("Null or empty column.");
        }
        IntegerValue listSize = new IntegerValue(list.size());
        return summarizeColValues(list).div(listSize);
    }


    public static Value getVar(ArrayList<Value> list) {
        if (list == null || list.isEmpty()) {
            throw new NullPointerException("Null or empty column.");
        }
        IntegerValue listSizeMinusOne = new IntegerValue(list.size() - 1);
        Value mean = getMeanFromCol(list);

        Value sumDiffsSquared = Value.getInstance(list.get(0).getClass());
        for (Value v : list) {
            Value diff = v.sub(mean);
            sumDiffsSquared = sumDiffsSquared.add(diff.mul(diff));
        }
        return sumDiffsSquared.div(listSizeMinusOne);
    }

    public void print(String name) {
        /*
        for (int start = 0; start < this.types.size(); start++) {
            for (int j = 0; j < data.get(start).size(); j++) {
                System.out.print(data.get(start).get(j) + "\t");
            }
            System.out.println();
        }*/
        System.out.println(name + ": ");
        for (String s : names) {
            System.out.print(s + "\t");
        }

        System.out.println();
        for (int i = 0; i < data.get(0).size(); i++) {
            for (int j = 0; j < this.names.size(); j++) {
                System.out.print(data.get(j).get(i) + "\t");
            }
            System.out.println();
        }
        System.out.println();
    }


    public void addRow(String[] values) {
        if (values.length != names.size())
            throw new NullPointerException("Too little or too much value given.");
        for (int i = 0; i < data.size(); i++) {
            this.data.get(i).add(createValueOfExtactType(values[i], i));
        }
    }


    public Value createValueOfExtactType(String strValue, int i) {
        try {
            Value v = Value.getInstance(this.types.get(i)).create(strValue);
            return v;
        } catch (NumberFormatException e) {
            //System.err.println("For input data: " + strValue);
            int lineNumber = data.get(i).size();
            throw new InconsistentColumnTypeException(names.get(i), lineNumber);
        }
    }


    public int getIndexOfExactCol(String colname) {
        for (int i = 0; i < names.size(); i++) {
            if (names.get(i).equals(colname)) {
                return i;
            }
        }
        throw new IllegalArgumentException("The '" + colname + "' column not found.");
    }


    public GroupedDataFrame groupBy(String groupingColname) {
        return groupBy(new String[]{groupingColname});
    }


    public GroupedDataFrame groupBy(String[] groupingColNames) {
        ArrayList<Integer> groupingColIndexes = new ArrayList<>();
        for (int i = 0; i < groupingColNames.length; i++) {
            groupingColIndexes.add(getIndexOfExactCol(groupingColNames[i]));
        }
        if (groupingColIndexes.isEmpty())
            throw new NullPointerException("Not found any grouping column indexes. Cannot do grouping.");

        TreeMap<String, DataFrame> newTmap = new TreeMap<>();

        for (int i = 0; i < data.get(0).size(); i++) {
            StringBuilder stringBuilder = new StringBuilder();
            for (int j = 0; j < groupingColIndexes.size(); j++) {
                stringBuilder.append((data.get(groupingColIndexes.get(j)).get(i)).toString());
            }
            String key = stringBuilder.toString();

            DataFrame smallDF;
            if (newTmap.containsKey(key)) {
                smallDF = newTmap.get(key);
            }
            else {
                smallDF = new DataFrame(names, types);
                newTmap.put(key, smallDF);
            }
            for (int j = 0; j < names.size(); j++) {
                smallDF.data.get(j).add(data.get(j).get(i));
            }
        }
        return new GroupedDataFrame(newTmap, groupingColNames);
    }


    public ArrayList get(String colname) {
        for (int i = 0; i < names.size(); i++) {
            if (names.get(i).equals(colname)) {
                return data.get(i);
            }
        }
        return new ArrayList();
    }


    public int size() {
        return data.get(0).size();
    }


    public DataFrame get(ArrayList<String> cols, boolean deepCopy) { //cols - nazwy kolumn
        ArrayList<Class<? extends Value>> newTypes = new ArrayList<>();

        for (int i = 0; i < cols.size(); i++) {
            for (int j = 0; j < names.size(); j++) {
                if (cols.get(i).equals(names.get(j))) {
                    newTypes.add(types.get(j));
                }
            }
        }

        DataFrame newDf = new DataFrame(cols, newTypes);
        createPartialFrame(cols, deepCopy, newDf);
        return newDf;
    }


    public void createPartialFrame(ArrayList<String> cols, boolean deepCopy, DataFrame _df) {
        if (deepCopy) {
            _df.data.clear();
            for (int i = 0; i < cols.size(); i++) {
                for (int j = 0; j < names.size(); j++) {
                    if (cols.get(i).equals(names.get(j))) {
                        _df.data.add(new ArrayList<Value>());
                        _df.names.set(i, names.get(j));
                        _df.types.set(i, types.get(j));
                        _df.data.set(i, data.get(j));
                    }
                }
            }
        }
        else {
            for (int i = 0; i < cols.size(); i++) {
                for (int j = 0; j < names.size(); j++) {
                    if (cols.get(i).equals(names.get(j))) {
                        _df.data.set(i, data.get(j));
                    }
                }
            }
        }
    }


    public DataFrame iloc(int i) {
        if (i < 0 || i > size())
            throw new IllegalArgumentException("Negative or out of range row index given.");
        DataFrame newDf = new DataFrame(names, types);
        for (int j = 0; j < names.size(); j++) {
            newDf.data.get(j).add(data.get(j).get(i));
        }
        return newDf;
    }


    public DataFrame iloc(int from, int to) {
        if (from < 0 || from > size() || to < 0 || to > size())
            throw new IllegalArgumentException("Negative or out of range row index given.");

        if (from > to) {
            int tmp = from;
            from = to;
            to = tmp;
        }
        DataFrame newDf = new DataFrame(names, types);
        for (int i = from; i <= to; i++) {
            for (int j = 0; j < names.size(); j++) {
                newDf.data.get(j).add(data.get(j).get(i));
            }
        }
        return newDf;
    }



    public boolean checkIfAllElementsAreEqual() {
        for (int i = 1; i < types.size(); i++) {
            if (!(types.get(0).equals(types.get(i)))) {
                return false;
            }
        }
        return true;
    }


    public ArrayList<Value> getCol(int colIndex) {
        return data.get(colIndex);
    }


    public void mulColByValue(int colIndex, Value v) {
        if (colIndex < 0 || colIndex > data.size())
            throw new IllegalArgumentException("Negative or out of range column index given.");
        for (int i = 0; i < data.get(colIndex).size(); i++) {
            data.get(colIndex).set(i, data.get(colIndex).get(i).mul(v));
        }
    }


    public void addValueToColumn(int colIndex, Value v) {
        if (colIndex < 0 || colIndex > data.size())
            throw new IllegalArgumentException("Negative or out of range column index given.");
        for (int i = 0; i < data.get(colIndex).size(); i++) {
            data.get(colIndex).set(i, data.get(colIndex).get(i).add(v));
        }
    }


    public void subValueFromCol(int colIndex, Value v) {
        if (colIndex < 0 || colIndex > data.size())
            throw new IllegalArgumentException("Negative or out of range column index given.");
        for (int i = 0; i < data.get(colIndex).size(); i++) {
            data.get(colIndex).set(i, data.get(colIndex).get(i).sub(v));
        }
    }


    public void divColByValue(int colIndex, Value v) {
        if (colIndex < 0 || colIndex > data.size())
            throw new IllegalArgumentException("Negative or out of range column index given.");
        for (int i = 0; i < data.get(colIndex).size(); i++) {
            data.get(colIndex).set(i, data.get(colIndex).get(i).div(v));
        }
    }


    public ArrayList<Value> applyActionOnTwoCols(String actionName, int colIndex1, int colIndex2) {
        if (data.get(colIndex1).size() != data.get(colIndex2).size())
            throw new InvalidParameterException("Different columns' sizes: " + names.get(colIndex1) + ": "
                    + data.get(colIndex1).size() + " and " + names.get(colIndex2) + ": " + data.get(colIndex2).size()
                    + ". Cannot apply action.");

        if (data.get(colIndex1).get(0).getClass().getName().equals("dataframe.DateTimeValue")
                || data.get(colIndex2).get(0).getClass().getName().equals("dataframe.DateTimeValue")
                || data.get(colIndex1).get(0).getClass().getName().equals("dataframe.StringValue")
                || data.get(colIndex2).get(0).getClass().getName().equals("dataframe.StringValue"))
            throw new IllegalArgumentException("Different columns' types: " + names.get(colIndex1) + ": "
                    + data.get(colIndex1).get(0).getClass().getName() + " and " + names.get(colIndex2) + ": "
                    + data.get(colIndex2).get(0).getClass().getName() + ". Cannot apply action.");

        ArrayList<Value> temp = new ArrayList<>();
        for (int i = 0; i < data.get(colIndex1).size(); i++) {
            if (actionName.equals("mul"))
                temp.add(data.get(colIndex1).get(i).mul(data.get(colIndex2).get(i)));
            else if (actionName.equals("add"))
                temp.add(data.get(colIndex1).get(i).add(data.get(colIndex2).get(i)));
            else if (actionName.equals("sub"))
                temp.add(data.get(colIndex1).get(i).sub(data.get(colIndex2).get(i)));
            else if (actionName.equals("div"))
                temp.add(data.get(colIndex1).get(i).div(data.get(colIndex2).get(i)));
            else
                throw new IllegalArgumentException("Wrong action name. Cannot recognize " + actionName);
        }
        return temp;
    }


    public ArrayList<Value> mulColByCol(int colIndex1, int colIndex2) {
        return applyActionOnTwoCols("mul", colIndex1, colIndex2);
    }


    public ArrayList<Value> addTwoCols(int colIndex1, int colIndex2) {
        return applyActionOnTwoCols("add", colIndex1, colIndex2);
    }


    public ArrayList<Value> subTwoCols(int colIndex1, int colIndex2) {
        return applyActionOnTwoCols("sub", colIndex1, colIndex2);
    }


    public ArrayList<Value> divColByCol(int colIndex1, int colIndex2) {
        return applyActionOnTwoCols("div", colIndex1, colIndex2);
    }


    public class GroupedDataFrame implements Groupby {
        protected TreeMap<String, DataFrame> groupingDataTmap;
        protected String[] groupingColNames;


        public GroupedDataFrame(TreeMap<String, DataFrame> _groupedDataHashMap, String[] _groupingCols) {
            groupingDataTmap = _groupedDataHashMap;
            groupingColNames = _groupingCols;
        }

        public GroupedDataFrame(){}

        public DataFrame applyActionOnEachCol(String actionName) {
            ArrayList<Integer> groupingColIndexes = new ArrayList<>();
            for (int i = 0; i < groupingColNames.length; i++) {
                groupingColIndexes.add(getIndexOfExactCol(groupingColNames[i]));
            }
            if (groupingColIndexes.isEmpty())
                throw new NullPointerException("Not found any grouping column indexes. Cannot do grouping.");

            ArrayList<Class<? extends Value>> newTypes = new ArrayList<>();
            ArrayList<String> newNames = new ArrayList<>();

            if (actionName.equals("max") || actionName.equals("min")) {
                for (int i = 0; i < types.size(); i++) {
                    if (!(types.get(i).getName().equals("dataframe.StringValue"))
                            || groupingColIndexes.contains(i)) {
                        newTypes.add(types.get(i));
                        newNames.add(names.get(i));
                    }
                }
            }
            else {
                for (int i = 0; i < types.size(); i++) {
                    if ((!(types.get(i).getName().equals("dataframe.StringValue")) &&
                            !(types.get(i).getName().equals("dataframe.DateTimeValue")))
                            || groupingColIndexes.contains(i)) {
                        newTypes.add(types.get(i));
                        newNames.add(names.get(i));
                    }
                }
            }


            DataFrame newDf = new DataFrame(newNames, newTypes);

            for (String key : groupingDataTmap.keySet()) {
                DataFrame value = groupingDataTmap.get(key).get(newNames, true); //get(key) zwraca WARTOSC dla danego klucza
                for (int i = 0; i < value.data.size(); i++) {
                    if (groupingColIndexes.contains(i)) {
                        newDf.data.get(i).add(value.data.get(i).get(0));
                        continue;
                    }
                    Value calculatedVal;
                    if (actionName.equals("max"))
                        calculatedVal = getMaxValFromCol(value.data.get(i));
                    else if (actionName.equals("min"))
                        calculatedVal = getMinValFromCol(value.data.get(i));
                    else if (actionName.equals("mean"))
                        calculatedVal = getMeanFromCol(value.data.get(i));
                    else if (actionName.equals("var"))
                        calculatedVal = getVar(value.data.get(i));
                    else if (actionName.equals("sum"))
                        calculatedVal = summarizeColValues(value.data.get(i));
                    else if (actionName.equals("std"))
                        calculatedVal = getVar(value.data.get(i)).pow(new DoubleValue(0.5));
                    else throw new IllegalArgumentException("Cannot recognize such action: " + actionName);

                    newDf.data.get(i).add(calculatedVal);
                }
            }
            return newDf;
        }


        public DataFrame max() {
            return applyActionOnEachCol("max");
        }


        public DataFrame min() {
            return applyActionOnEachCol("min");
        }


        public DataFrame mean() {
            return applyActionOnEachCol("mean");
        }


        public DataFrame sum() {
            return applyActionOnEachCol("sum");
        }


        public DataFrame var() {
            return applyActionOnEachCol("var");
        }


        public DataFrame std() {
            return applyActionOnEachCol("std");
        }


        public DataFrame apply(Applyable a) {
            ArrayList<Integer> groupingColIndexes = new ArrayList<>();
            for (int i = 0; i < groupingColNames.length; i++) {
                groupingColIndexes.add(getIndexOfExactCol(groupingColNames[i]));
            }

            if (groupingColIndexes.isEmpty())
                throw new NullPointerException("Not found any grouping column indexes. Cannot do grouping.");

            ArrayList<Class<? extends Value>> newTypes = new ArrayList<>();
            ArrayList<String> newNames = new ArrayList<>();
            for (int i = 0; i < types.size(); i++) {
                if ((!(types.get(i).getName().equals("dataframe.StringValue")) &&
                        !(types.get(i).getName().equals("dataframe.DateTimeValue")))
                        || groupingColIndexes.contains(i)) {
                    newTypes.add(types.get(i));
                    newNames.add(names.get(i));
                }
            }

            DataFrame newDf = new DataFrame(newNames, newTypes);

            for (String key : groupingDataTmap.keySet()) {
                DataFrame value = groupingDataTmap.get(key).get(newNames, true); //get(key) zwraca WARTOSC dla danego klucza
                DataFrame row = a.apply(value, groupingColIndexes);
                for (int j = 0; j < row.data.size(); j++)
                    newDf.data.get(j).add(row.data.get(j).get(0));
            }
            return newDf;

        }
    }
}
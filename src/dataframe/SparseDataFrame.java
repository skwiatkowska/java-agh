package dataframe;

import java.io.*;
import java.util.ArrayList;


public class SparseDataFrame extends DataFrame{
    private String hide;

    public SparseDataFrame(String[] _names, String[] _types, String _hide){
        super(_names, _types);
        hide = _hide;
    }


     public SparseDataFrame(DataFrame _df, String _hide) {
        super(_df.names, _df.types);

         if(! _df.checkIfAllElementsAreEqual()){
             throw new IllegalArgumentException("DataFrame's types are not equal. Cannot convert DataFrame to SparseDataFrame");
         }
         hide = _hide;

        for (int i = 0; i < names.length; i++){
            for (int j = 0; j < _df.data.get(0).size(); j++){
                if(!(_df.data.get(i).get(j).equals(hide))){
                    data.get(i).add(new COOValue(j, _df.data.get(i).get(j).toString()));
                }
            }
        }
    }

    @Override
    public int size(){ //returns the largest "first" from the entire SDF
        int theLargest = ((COOValue)data.get(0).get(data.get(0).size()-1)).first;
        for (int i = 1; i < names.length; i++){
            if(((COOValue)data.get(i).get(data.get(i).size()-1)).first > theLargest){
                theLargest = ((COOValue)data.get(i).get(data.get(i).size()-1)).first + 1;
            }
        }
        return theLargest;
    }

    public DataFrame toDense(){ //SDF -> DF
        DataFrame newDf = new DataFrame(names, types);
        int index;
        for (int i = 0; i < names.length; i++) {
            index = 0;
            for (int j = 0; j < data.get(i).size(); j++) {
                while(index < ((COOValue)data.get(i).get(j)).first) {
                    newDf.data.get(i).add(hide);
                    index++;
                }
                newDf.data.get(i).add(((COOValue)data.get(i).get(j)).second);
                index++;
            }
        }

        int SDFSize = this.size();
        for (int i = 0; i < names.length; i++){
            while(newDf.data.get(i).size() < SDFSize){
                newDf.data.get(i).add(Integer.parseInt(hide));
            }
        }
        return newDf;
    }

    public void print(){
        for (int i = 0; i < this.names.length; i++){
            System.out.print(this.names[i] + "\t");
            for (Object c : this.data.get(i)){
                System.out.print("(" + ((COOValue)c).first + ", " + ((COOValue)c).second + ")" + "\t");
            }
            System.out.println();
        }
        System.out.println();
    }


    public SparseDataFrame (String path, String[] _types, String[] header, String _hide){
        names = header.clone();
        types = _types.clone();
        hide = _hide;

        for (int i = 0; i < types.length; i++){
            data.add(new ArrayList<Object>());
        }

        String separator = ",";

        try {
            FileInputStream fstream = new FileInputStream(path);
            BufferedReader br = new BufferedReader(new InputStreamReader(fstream));

            String line;
            int rowsCounter = 0;

            while ((line = br.readLine()) != null) {
                String[] row = line.split(separator);
                for(int i = 0; i < row.length; i++){
                    if(!(row[i].equals(hide))){
                        data.get(i).add(new COOValue(rowsCounter, row[i]));
                    }
                }
                rowsCounter++;
            }
            br.close();

        } catch(FileNotFoundException e){
            System.out.println("No such file.");
        } catch(IOException e){
            e.printStackTrace();
        }
    }

    public SparseDataFrame (String path, String[] _types, String _hide) {
        types = _types.clone();
        hide = _hide;

        for (int i = 0; i < types.length; i++){
            data.add(new ArrayList<Object>());
        }

        boolean header = true;
        String separator = ",";

        try {
            FileInputStream fstream = new FileInputStream(path);
            BufferedReader br = new BufferedReader(new InputStreamReader(fstream));

            String line;
            int rowsCounter = 0;

            while ((line = br.readLine()) != null) {
                if(header){
                    String[] _names = line.split(separator);
                    names = _names.clone();
                    header = false;
                    continue;
                }
                String[] row = line.split(separator);
                for(int i = 0; i < row.length; i++){
                    if(!(row[i].equals(hide))){
                        data.get(i).add(new COOValue(rowsCounter, row[i]));
                    }
                }
                rowsCounter++;
            }
            br.close();

        } catch(FileNotFoundException e){
            System.out.println("No such file.");
        } catch(IOException e){
            e.printStackTrace();
        }
    }


    public ArrayList get(String colname) {
        for (int i = 0; i < names.length; i++) {
            if (names[i].equals(colname)) {
                SparseDataFrame newSdf = new SparseDataFrame(new String[]{names[i]}, new String[]{types[i]}, hide);
                newSdf.data.get(0).add(data.get(i));
                DataFrame df = newSdf.toDense();
                return df.data.get(0);
            }
        }

        return new ArrayList();
    }


    @Override
    public DataFrame get(String[] cols, boolean deepCopy) {
        String[] newTypes = new String[cols.length]; //zmiana tablicy typow
        for (int i = 0; i < cols.length; i++) {
            for (int j = 0; j < names.length; j++) {
                if (cols[i].equals(names[j])) {
                    newTypes[i] = types[j];
                }
            }
        }

        SparseDataFrame newSdf = new SparseDataFrame(cols, newTypes, hide);
        if (deepCopy) {
            newSdf.data.clear();
            for (int i = 0; i < cols.length; i++) {
                for (int j = 0; j < names.length; j++) {
                    if (cols[i].equals(names[j])) {
                        newSdf.data.add(new ArrayList<Object>());
                        newSdf.names[i] = names[j];
                        newSdf.types[i] = types[j];
                        newSdf.data.set(i, data.get(j));
                    }
                }
            }
        } else {
            for (int i = 0; i < cols.length; i++) {
                for (int j = 0; j < names.length; j++) {
                    if (cols[i].equals(names[j])) {
                        newSdf.data.set(i, data.get(j));
                    }
                }
            }
        }

        DataFrame newDf = newSdf.toDense();
        int SDFSize = this.size();
        for (int i = 0; i < newDf.names.length; i++){
            while(newDf.data.get(i).size() < SDFSize){
                newDf.data.get(i).add(Integer.parseInt(hide));
            }
        }
        return newDf;
    }


    public DataFrame iloc(int i) {
        DataFrame newDf = new DataFrame(names, types);
        for(int j = 0; j < names.length; ++j){
            newDf.data.get(j).add(hide);
        }
        for (int j = 0; j < names.length; ++j) {
            for(int k = 0; k < data.get(j).size(); k++) {
                if(((COOValue)data.get(j).get(k)).first == i){
                    newDf.data.get(j).set(0,((COOValue)data.get(j).get(k)).second);
                }
            }
        }
        return newDf;
    }


    public DataFrame iloc(int from, int to) {
        DataFrame newDf = new DataFrame(names, types);
        for(int i = from; i <= to; i++) {
            for (int j = 0; j < names.length; j++) {
                newDf.data.get(j).add(hide);
            }
        }

        int m = 0;
        for (int i = from; i <= to; i++) {
            for (int j = 0; j < names.length; j++) {
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

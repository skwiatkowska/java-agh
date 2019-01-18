package dataframe;

import java.util.ArrayList;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.ReentrantLock;


public class DataFrameThreads extends DataFrame {
    public DataFrameThreads(ArrayList<String> _names, ArrayList<Class<? extends Value>> _types) {
        super(_names, _types);
    }


    public DataFrameThreads(String path, ArrayList<Class<? extends Value>> _types, ArrayList<String> header) {
        super(path, _types, header);
    }


    public DataFrameThreads(String path, ArrayList<Class<? extends Value>> _types) {
        super(path, _types);
    }


    public GroupedDataFrameThreads groupBy(String groupingColname) {
        return groupBy(new String[]{groupingColname});
    }


    public GroupedDataFrameThreads groupBy(String[] groupingColNames) {
        ReentrantLock lock = new ReentrantLock();
        int numberOfThreads = 5;

        ArrayList<Integer> groupingColIndexes = new ArrayList<>();
        for (int i = 0; i < groupingColNames.length; i++) {
            groupingColIndexes.add(getIndexOfExactCol(groupingColNames[i]));
        }
        if (groupingColIndexes.isEmpty())
            throw new NullPointerException("Not found any grouping column indexes. Cannot do grouping.");

        TreeMap<String, DataFrame> result = new TreeMap<>();
        CountDownLatch latch = new CountDownLatch(numberOfThreads);
        ExecutorService executorService = Executors.newFixedThreadPool(numberOfThreads);

        int numberOfRows = data.get(0).size(), start = 0, end = numberOfRows;
        for (int i = 0; i < numberOfThreads - 1; i++) {
            start = (int) ((1.0 / numberOfThreads) * i);
            end = (int) ((1.0 / numberOfThreads) * (i + 1));
            executorService.execute(new RunnableGroupBy(start, end, result, groupingColIndexes, latch, lock));
        }
        executorService.execute(new RunnableGroupBy(end, numberOfRows, result, groupingColIndexes, latch, lock));

        try {
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        executorService.shutdown();

        return new GroupedDataFrameThreads(result, groupingColNames);
    }


    class RunnableGroupBy implements Runnable {
        CountDownLatch countDownLatch;
        ReentrantLock lock;
        int start, koniec;
        TreeMap<String, DataFrame> result;
        ArrayList<Integer> groupingColIndexes;


        RunnableGroupBy(int _start, int _koniec, TreeMap<String, DataFrame> _result, ArrayList<Integer> _groupingColIndexes, CountDownLatch _countDownLatch, ReentrantLock _lock) {
            start = _start;
            koniec = _koniec;
            result = _result;
            groupingColIndexes = _groupingColIndexes;
            countDownLatch = _countDownLatch;
            lock = _lock;
        }


        @Override
        public void run() {
            while (start < koniec) {
                StringBuilder stringBuilder = new StringBuilder();
                for (int j = 0; j < groupingColIndexes.size(); j++) {
                    stringBuilder.append((data.get(groupingColIndexes.get(j)).get(start)).toString());
                }
                String key = stringBuilder.toString();

                DataFrame smallDF;
                if (result.containsKey(key)) {
                    smallDF = result.get(key);
                }
                else {
                    smallDF = new DataFrame(names, types);
                    result.put(key, smallDF);
                }
                for (int j = 0; j < names.size(); j++) {
                    smallDF.data.get(j).add(data.get(j).get(start));
                }
                start++;
            }
            countDownLatch.countDown();
        }
    }


    public class GroupedDataFrameThreads extends DataFrame.GroupedDataFrame implements Groupby {
        private TreeMap<String, DataFrame> groupingDataTmap;
        private String[] groupingColNames;
        private DataFrame newDf = null;
        private ArrayList<Integer> groupingColIndexes = new ArrayList<>();


        public GroupedDataFrameThreads(TreeMap<String, DataFrame> _groupedDataHashMap, String[] _groupingCols) {
            groupingDataTmap = _groupedDataHashMap;
            groupingColNames = _groupingCols;

            for (int i = 0; i < groupingColNames.length; i++) {
                groupingColIndexes.add(getIndexOfExactCol(groupingColNames[i]));
            }
            if (groupingColIndexes.isEmpty())
                throw new NullPointerException("Not found any grouping column indexes. Cannot do grouping.");

        }


        public DataFrame applyActionOnEachCol(String actionName) {
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
            newDf = new DataFrame(newNames, newTypes);


            CountDownLatch latch = new CountDownLatch(groupingDataTmap.size());
            ReentrantLock lock = new ReentrantLock();
            ExecutorService executorService = Executors.newFixedThreadPool(5);

            for (String key : groupingDataTmap.keySet()) {
                try {
                    executorService.execute(new RunnableOperation(key, actionName, latch, lock));
                } catch (IllegalArgumentException e) {
                    System.err.println("Cannot recognize such action: " + actionName);
                }

            }

            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            executorService.shutdown();
            return newDf;
        }


        class RunnableOperation implements Runnable {
            ReentrantLock lock;
            String key;
            String actionName;
            private CountDownLatch countDownLatch;


            RunnableOperation(String _key, String _actionName, CountDownLatch _countDownLatch, ReentrantLock _lock) {
                countDownLatch = _countDownLatch;
                lock = _lock;
                key = _key;
                actionName = _actionName;
            }


            @Override
            public void run() {
                ArrayList<Value> tmpArray = new ArrayList<>();
                DataFrame value = groupingDataTmap.get(key).get(newDf.names, true); //get(key) zwraca WARTOSC dla danego klucza
                for (int i = 0; i < value.data.size(); i++) {
                    if (groupingColIndexes.contains(i)) {
                        tmpArray.add(value.data.get(i).get(0));
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
                    tmpArray.add(calculatedVal);
                }
                lock.lock();
                for (int i = 0; i < tmpArray.size(); i++) {
                    newDf.data.get(i).add(tmpArray.get(i));
                }
                lock.unlock();

                countDownLatch.countDown();
            }
        }
    }
}
package dataframe;

import java.util.ArrayList;
import java.util.Collections;

public class Median implements Applyable {

    public DataFrame apply(DataFrame _df, ArrayList<Integer> groupingColIndexes) {
        ArrayList<Class<? extends Value>> newTypes = new ArrayList<>();
        ArrayList<String> newNames = new ArrayList<>();
        for (int i = 0; i < _df.types.size(); i++) {
            if ((!(_df.types.get(i).getName().equals("dataframe.StringValue")) &&
                    !(_df.types.get(i).getName().equals("dataframe.DateTimeValue")))
                    || groupingColIndexes.contains(i)) {
                newTypes.add(_df.types.get(i));
                newNames.add(_df.names.get(i));
            }
        }

        DataFrame newDf = new DataFrame(newNames, newTypes);
        for (int i = 0; i < _df.data.size(); i++) {
            if (groupingColIndexes.contains(i)) {
                newDf.data.get(i).add(_df.data.get(i).get(0));
                continue;
            }
            Value v = getMedian(_df.data.get(i));
            newDf.data.get(i).add(v);

        }
        return newDf;
    }


    public Value getMedian(ArrayList<Value> list) {
        if (list == null || list.isEmpty()) {
            throw new NullPointerException("Null or empty column.");
        }
        Collections.sort(list);
        int middle = list.size() / 2;

        if (list.size() % 2 == 1)
            return list.get(middle);
        else
            return ((list.get(middle - 1).add(list.get(middle))).div(new DoubleValue(2.0)));
    }
}

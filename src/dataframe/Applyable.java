package dataframe;

import java.util.ArrayList;

public interface Applyable {
    DataFrame apply(DataFrame _df, ArrayList<Integer> groupingColIndexes);
}

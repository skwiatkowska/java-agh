package dataframe;

public class InconsistentColumnTypeException extends NumberFormatException {
    String colName;
    int rowIndex;


    public InconsistentColumnTypeException(String _colName, int _rowIndex) {
        super("Wrong data type in \"" + _colName + "\" column, in line " + _rowIndex);
        colName = _colName;
        rowIndex = _rowIndex;
    }
}

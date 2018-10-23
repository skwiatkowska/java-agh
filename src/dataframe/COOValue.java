package dataframe;

final public class COOValue {
    protected int first;
    protected String second;

    public COOValue(int _first, String _second){
        first = _first;
        second = _second;
    }

    public int getFirst(){
        return first;
    }

    public String getSecond(){
        return second;
    }
}

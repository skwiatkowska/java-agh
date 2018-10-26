package dataframe;

public abstract class Value {
    protected Object value;

    public abstract String toString();
    public abstract Value add(Value v);
    public abstract Value sub(Value v);
    public abstract Value mul(Value v);
    public abstract Value div(Value v);
    public abstract Value pow(Value v);
    public abstract boolean eq(Value v);
    public abstract boolean lte(Value v);
    public abstract boolean lt(Value v);
    public abstract boolean gte(Value v);
    public abstract boolean gt(Value v);
    public abstract boolean neq(Value v);
    public abstract boolean equals(Object other);
    public abstract int hashCode();
    public abstract Value create(String s);


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

}

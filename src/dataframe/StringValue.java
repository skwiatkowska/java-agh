package dataframe;

import java.util.Objects;

public class StringValue extends Value {
    protected String value;

    public StringValue() {
        value = "";
    }

    public StringValue(String value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return value;
    }

    public Value add(Value v) {
        return new StringValue(value+v.toString());
    }

    public Value sub(Value v) {
        throw new IllegalArgumentException("Invalid operation.");
    }

    public Value mul(Value v) {
        if (v instanceof IntegerValue) {
            StringValue result = new StringValue("");
            for (int i = 0; i < (int)v.value; i++) {
                result.value += value;
            }
            return result;
        }
        else {
            throw new IllegalArgumentException("Invalid operation.");
        }
    }


    public Value div(Value v) {
        throw new IllegalArgumentException("Invalid operation.");
    }

    public  Value pow(Value v)
    {
        throw new IllegalArgumentException("Invalid operation.");
    }

    public boolean eq(Value v)
    {
        return value.equals(v.toString());
    }

    public boolean lte(Value v)
    {
        throw new IllegalArgumentException("Invalid operation.");
    }

    public boolean lt(Value v)
    {
        throw new IllegalArgumentException("Invalid operation.");
    }

    public boolean gte(Value v)
    {
        throw new IllegalArgumentException("Invalid operation.");
    }

    public boolean gt(Value v)
    {
        throw new IllegalArgumentException("Invalid operation.");
    }

    public boolean neq(Value v)
    {
        return !this.eq(v);
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StringValue that = (StringValue) o;
        return Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value);
    }


    public Value create(String s) {
        value = s;
        return this;
    }
}

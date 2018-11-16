package dataframe;

import java.util.Objects;

public class StringValue extends Value implements Cloneable, Comparable<Value> {
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
        return new StringValue(value + v.toString());
    }


    public Value sub(Value v) {
        throw new IllegalArgumentException("Invalid operation for " + this.getClass().getName() + ".");
    }


    public Value mul(Value v) {
        throw new IllegalArgumentException("Invalid operation for " + this.getClass().getName() + ".");
    }


    public Value div(Value v) {
        throw new IllegalArgumentException("Invalid operation for " + this.getClass().getName() + ".");
    }


    public Value pow(Value v) {
        throw new IllegalArgumentException("Invalid operation for " + this.getClass().getName() + ".");
    }


    public boolean eq(Value v) {
        return value.equals(v.toString());
    }


    public boolean lte(Value v) {
        if (v instanceof StringValue)
            return (value.compareTo(((StringValue) v).value) <= 0);
        throw new IllegalArgumentException("Incompatible types given: " + this.getClass()
                + " and " + v.getClass().getName() + ". Cannot compare.");
    }


    public boolean lt(Value v) {
        if (v instanceof StringValue)
            return (value.compareTo(((StringValue) v).value) < 0);
        throw new IllegalArgumentException("Incompatible types given: " + this.getClass()
                + " and " + v.getClass().getName() + ". Cannot compare.");
    }


    public boolean gte(Value v) {
        if (v instanceof StringValue)
            return (value.compareTo(((StringValue) v).value) >= 0);
        throw new IllegalArgumentException("Incompatible types given: " + this.getClass()
                + " and " + v.getClass().getName() + ". Cannot compare.");
    }


    public boolean gt(Value v) {
        if (v instanceof StringValue)
            return (value.compareTo(((StringValue) v).value) > 0);
        throw new IllegalArgumentException("Incompatible types given: " + this.getClass()
                + " and " + v.getClass().getName() + ". Cannot compare.");
    }


    public boolean neq(Value v) {
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


    public StringValue clone() throws CloneNotSupportedException {
        return (StringValue) super.clone();
    }


    public int compareTo(Value v) {
        if (v instanceof StringValue) {
            return compareValuesOfTheSameInstance(v);
        }
        throw new IllegalArgumentException("Incompatible types given: " + this.getClass()
                + " and " + v.getClass().getName() + ". Cannot compare.");
    }
}

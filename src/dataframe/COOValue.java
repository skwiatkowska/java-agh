package dataframe;

import java.security.InvalidParameterException;
import java.util.Objects;

public class COOValue extends Value implements Cloneable, Comparable<Value> {
    protected int first;
    protected Value second;


    public COOValue(int _first, Value _second) {
        first = _first;
        second = _second;
    }


    public int getFirst() {
        return first;
    }


    public Value getSecond() {
        return second;
    }


    @Override
    public String toString() {
        return "(" + first + ", " + second + ")";
    }


    public Value add(Value v) {
        throw new IllegalArgumentException("Invalid operation for " + this.getClass().getName() + ".");
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
        return this.toString().equals(v.toString());
    }


    public boolean lte(Value v) {
        throw new IllegalArgumentException("Invalid operation for " + this.getClass().getName() + ".");
    }


    public boolean lt(Value v) {
        throw new IllegalArgumentException("Invalid operation for " + this.getClass().getName() + ".");
    }


    public boolean gte(Value v) {
        throw new IllegalArgumentException("Invalid operation for " + this.getClass().getName() + ".");
    }


    public boolean gt(Value v) {
        throw new IllegalArgumentException("Invalid operation for " + this.getClass().getName() + ".");
    }


    public boolean neq(Value v) {
        return !this.eq(v);
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        COOValue cooValue = (COOValue) o;
        return first == cooValue.first &&
                Objects.equals(second, cooValue.second);
    }


    @Override
    public int hashCode() {
        return Objects.hash(first, second);
    }


    public Value create(String s) {
        throw new InvalidParameterException("New COOValue object cannot be created straight from a string. " +
                "COOValue objects are accessible only after converting DataFrame to SparseDataFrame.");
    }


    public StringValue clone() throws CloneNotSupportedException {
        return (StringValue) super.clone();
    }


    public int compareTo(Value v) {
        if (v instanceof COOValue) {
            if (first == ((COOValue) v).first && second.eq(((COOValue) v).second))
                return 0;
            else if (first < ((COOValue) v).first || second.lt(((COOValue) v).second))
                return -1;
            else
                return 1;
        }
        throw new IllegalArgumentException("Incompatible types given: " + this.getClass()
                + " and " + v.getClass().getName() + ". Cannot compare.");
    }
}

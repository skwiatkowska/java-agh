package dataframe;

import java.util.Objects;

public class COOValue extends Value {
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
        throw new IllegalArgumentException("Invalid operation.");
    }


    public Value sub(Value v) {
        throw new IllegalArgumentException("Invalid operation.");
    }


    public Value mul(Value v) {
        throw new IllegalArgumentException("Invalid operation.");
    }


    public Value div(Value v) {
        throw new IllegalArgumentException("Invalid operation.");
    }


    public Value pow(Value v) {
        throw new IllegalArgumentException("Invalid operation.");
    }


    public boolean eq(Value v) {
        return this.toString().equals(v.toString());
    }


    public boolean lte(Value v) {
        throw new IllegalArgumentException("Invalid operation.");
    }


    public boolean lt(Value v) {
        throw new IllegalArgumentException("Invalid operation.");
    }


    public boolean gte(Value v) {
        throw new IllegalArgumentException("Invalid operation.");
    }


    public boolean gt(Value v) {
        throw new IllegalArgumentException("Invalid operation.");
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


        return this;
    }
}

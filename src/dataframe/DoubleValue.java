package dataframe;

import java.util.Objects;


public class DoubleValue extends Value implements Cloneable, Comparable<Value> {
    protected double value;


    public DoubleValue() {
        value = 0.0;
    }


    public DoubleValue(double _value) {
        value = _value;
    }


    public String toString() {
        return String.valueOf(value);
    }


    public Value add(Value v) {
        if (v instanceof IntegerValue || v instanceof FloatValue || v instanceof DoubleValue) {
            return new DoubleValue(value + Double.parseDouble(v.toString()));
        }
        else
            throw new IllegalArgumentException("Incompatible types. Cannot add " + this.getClass()
                    + " and " + v.getClass().getName());
    }


    public Value sub(Value v) {
        if (v instanceof IntegerValue || v instanceof FloatValue || v instanceof DoubleValue)
            return new DoubleValue(value - Double.parseDouble(v.toString()));
        else
            throw new IllegalArgumentException("Incompatible types. Cannot subtract " + this.getClass()
                    + " and " + v.getClass().getName());
    }


    public Value mul(Value v) {
        if (v instanceof IntegerValue || v instanceof FloatValue || v instanceof DoubleValue)
            return new DoubleValue(value * Double.parseDouble(v.toString()));
        else
            throw new IllegalArgumentException("Different objects' types.");
    }


    public Value div(Value v) {
        if (v instanceof IntegerValue || v instanceof FloatValue || v instanceof DoubleValue) {
            return new DoubleValue(value / Double.parseDouble(v.toString()));
        }
        throw new IllegalArgumentException("Incompatible types. Cannot divide " + this.getClass()
                + " and " + v.getClass().getName());
    }


    public Value pow(Value v) {
        if (v instanceof IntegerValue || v instanceof FloatValue || v instanceof DoubleValue)
            return new DoubleValue(Math.pow(value, Double.parseDouble(v.toString())));
        else
            throw new IllegalArgumentException("Incompatible types. Cannot do exponentiation on " + this.getClass()
                    + " and " + v.getClass().getName());
    }


    public boolean eq(Value v) {
        if (v instanceof IntegerValue || v instanceof FloatValue || v instanceof DoubleValue)
            return value == Double.parseDouble(v.toString());
        else
            throw new IllegalArgumentException("Incompatible types given: " + this.getClass()
                    + " and " + v.getClass().getName() + ". Cannot compare.");
    }


    public boolean lte(Value v) {
        if (v instanceof IntegerValue || v instanceof FloatValue || v instanceof DoubleValue)
            return value <= Double.parseDouble(v.toString());
        else
            throw new IllegalArgumentException("Incompatible types given: " + this.getClass()
                    + " and " + v.getClass().getName() + ". Cannot compare.");
    }


    public boolean lt(Value v) {
        if (v instanceof IntegerValue || v instanceof FloatValue || v instanceof DoubleValue)
            return value < Double.parseDouble(v.toString());
        else
            throw new IllegalArgumentException("Incompatible types given: " + this.getClass()
                    + " and " + v.getClass().getName() + ". Cannot compare.");
    }


    public boolean gte(Value v) {
        if (v instanceof IntegerValue || v instanceof FloatValue || v instanceof DoubleValue)
            return value >= Double.parseDouble(v.toString());
        else
            throw new IllegalArgumentException("Incompatible types given: " + this.getClass()
                    + " and " + v.getClass().getName() + ". Cannot compare.");
    }


    public boolean gt(Value v) {
        if (v instanceof IntegerValue || v instanceof FloatValue || v instanceof DoubleValue)
            return value > Double.parseDouble(v.toString());
        else
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
        DoubleValue that = (DoubleValue) o;
        return Double.compare(that.value, value) == 0;
    }


    @Override
    public int hashCode() {
        return Objects.hash(value);
    }


    public Value create(String s) {
        int i = 0;
        int len = s.length();
        boolean correct = false;
        while (i < len) {
            char digit = s.charAt(i);
            if (((int) digit >= 48 && (int) digit <= 57) || (int) digit == 45 || (int) digit == 46 || (int)digit == 101) {
                correct = true;
            }
            else
                throw new NumberFormatException();
            i++;
        }
        value = Double.parseDouble(s);
        return this;
    }


    public DoubleValue clone() throws CloneNotSupportedException {
        return (DoubleValue) super.clone();
    }


    public int compareTo(Value v) {
        if (v instanceof DoubleValue)
            return compareValuesOfTheSameInstance(v);
        throw new IllegalArgumentException("Incompatible types given: " + this.getClass()
                + " and " + v.getClass().getName() + ". Cannot compare.");
    }
}

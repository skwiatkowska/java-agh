package dataframe;

import java.util.Objects;


public class IntegerValue extends Value implements Cloneable, Comparable<Value> {
    protected int value;


    public IntegerValue() {
        value = 0;
    }


    public IntegerValue(int _value) {
        value = _value;
    }


    public String toString() {
        return Integer.toString(value);
    }


    public Value add(Value v) {
        if (v instanceof IntegerValue)
            return new IntegerValue(value + Integer.parseInt(v.toString()));
        else if (v instanceof DoubleValue)
            return new DoubleValue(value + Double.parseDouble(v.toString()));
        else if (v instanceof FloatValue)
            return new FloatValue(value + Float.parseFloat(v.toString()));
        throw new IllegalArgumentException("Incompatible types. Cannot add " + this.getClass()
                + " and " + v.getClass().getName());
    }


    public Value sub(Value v) {
        if (v instanceof IntegerValue)
            return new IntegerValue(value - Integer.parseInt(v.toString()));
        else if (v instanceof DoubleValue)
            return new DoubleValue(value - Double.parseDouble(v.toString()));
        else if (v instanceof FloatValue)
            return new FloatValue(value - Float.parseFloat(v.toString()));
        throw new IllegalArgumentException("Incompatible types. Cannot subtract " + this.getClass()
                + " and " + v.getClass().getName());
    }


    public Value mul(Value v) {
        if (v instanceof IntegerValue)
            return new IntegerValue(value * Integer.parseInt(v.toString()));
        else if (v instanceof DoubleValue)
            return new DoubleValue(value * Double.parseDouble(v.toString()));
        else if (v instanceof FloatValue)
            return new FloatValue(value * Float.parseFloat(v.toString()));
        else
            throw new IllegalArgumentException("Incompatible types. Cannot multiply " + this.getClass()
                    + " and " + v.getClass().getName());
    }


    public Value div(Value v) {
        if (v instanceof IntegerValue && ((IntegerValue) v).value == 0)
            throw new ArithmeticException("/ by zero.");
        else if (v instanceof FloatValue || v instanceof DoubleValue ||
                (v instanceof IntegerValue && ((IntegerValue) v).value != 0))
            return new DoubleValue(value / Double.parseDouble(v.toString()));
        throw new IllegalArgumentException("Incompatible types. Cannot divide " + this.getClass()
                + " and " + v.getClass().getName());
    }


    public Value pow(Value v) {
        if (v instanceof IntegerValue) {
            Double doublePow = Math.pow(value, Integer.parseInt(v.toString()));
            return new IntegerValue(doublePow.intValue());
        }
        else if (v instanceof DoubleValue)
            return new DoubleValue(Math.pow(value, Double.parseDouble(v.toString())));
        else if (v instanceof FloatValue)
            return new FloatValue((float) Math.pow(value, Double.parseDouble(v.toString())));
        else
            throw new IllegalArgumentException("Incompatible types. Cannot do exponentiation on " + this.getClass()
                    + " and " + v.getClass().getName());
    }


    public boolean eq(Value v) {
        if (v instanceof IntegerValue || v instanceof FloatValue || v instanceof DoubleValue)
            return value == Integer.parseInt(v.toString());
        else
            throw new IllegalArgumentException("Incompatible types given: " + this.getClass()
                    + " and " + v.getClass().getName() + ". Cannot compare.");
    }


    public boolean lte(Value v) {
        if (v instanceof IntegerValue || v instanceof FloatValue || v instanceof DoubleValue)
            return value <= Integer.parseInt(v.toString());
        else
            throw new IllegalArgumentException("Incompatible types given: " + this.getClass()
                    + " and " + v.getClass().getName() + ". Cannot compare.");
    }


    public boolean lt(Value v) {
        if (v instanceof IntegerValue || v instanceof FloatValue || v instanceof DoubleValue)
            return value < Integer.parseInt(v.toString());
        else
            throw new IllegalArgumentException("Incompatible types given: " + this.getClass()
                    + " and " + v.getClass().getName() + ". Cannot compare.");
    }


    public boolean gte(Value v) {
        if (v instanceof IntegerValue || v instanceof FloatValue || v instanceof DoubleValue)
            return value >= Integer.parseInt(v.toString());
        else
            throw new IllegalArgumentException("Incompatible types given: " + this.getClass()
                    + " and " + v.getClass().getName() + ". Cannot compare.");
    }


    public boolean gt(Value v) {
        if (v instanceof IntegerValue || v instanceof FloatValue || v instanceof DoubleValue)
            return value >= Integer.parseInt(v.toString());
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
        IntegerValue that = (IntegerValue) o;
        return value == that.value;
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
            if (((int) digit >= 48 && (int) digit <= 57) || (int) digit == 45) {
                correct = true;
            }
            else
                throw new NumberFormatException();
            i++;
        }
        value = Integer.parseInt(s);
        return this;
    }


    public IntegerValue clone() throws CloneNotSupportedException {
        return (IntegerValue) super.clone();
    }


    public int compareTo(Value v) {
        if (v instanceof IntegerValue)
            return compareValuesOfTheSameInstance(v);
        throw new IllegalArgumentException("Incompatible types given: " + this.getClass()
                + " and " + v.getClass().getName() + ". Cannot compare.");
    }
}

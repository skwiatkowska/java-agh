package dataframe;

import java.util.Objects;


public class DoubleValue extends Value {
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
        if (v instanceof IntegerValue || v instanceof FloatValue || v instanceof DoubleValue)
            return (new DoubleValue(value + (double) v.value));
        else
            throw new IllegalArgumentException("Different objects' types.");
    }


    public Value sub(Value v) {
        if (v instanceof IntegerValue || v instanceof FloatValue || v instanceof DoubleValue)
            return (new DoubleValue(value - (double) v.value));
        else
            throw new IllegalArgumentException("Different objects' types.");
    }


    public Value mul(Value v) {
        if (v instanceof IntegerValue || v instanceof FloatValue || v instanceof DoubleValue)
            return (new DoubleValue(value * (double) v.value));
        else
            throw new IllegalArgumentException("Different objects' types.");
    }


    public Value div(Value v) {
        if (v instanceof IntegerValue || v instanceof FloatValue || v instanceof DoubleValue) {
            try {
                return (new DoubleValue(value / (double) v.value));

            } catch (ArithmeticException e) {
                System.out.println("Cannot divide by 0.");
            }
        }
        throw new IllegalArgumentException("Different objects' types.");
    }


    public Value pow(Value v) {
        if (v instanceof IntegerValue || v instanceof FloatValue || v instanceof DoubleValue)
            return (new DoubleValue((double) Math.pow(value, (double) v.value)));
        else
            throw new IllegalArgumentException("Different objects' types.");
    }


    public boolean eq(Value v) {
        if (v instanceof IntegerValue || v instanceof FloatValue || v instanceof DoubleValue)
            return (double) v.value == value;
        else
            throw new IllegalArgumentException("Different objects' types. Cannot compare.");
    }


    public boolean lte(Value v) {
        if (v instanceof IntegerValue || v instanceof FloatValue || v instanceof DoubleValue)
            return value <= (double) v.value;
        else
            throw new IllegalArgumentException("Different objects' types. Cannot compare.");
    }


    public boolean lt(Value v) {
        if (v instanceof IntegerValue || v instanceof FloatValue || v instanceof DoubleValue)
            return value < (double) v.value;
        else
            throw new IllegalArgumentException("Different objects' types. Cannot compare.");
    }


    public boolean gte(Value v) {
        if (v instanceof IntegerValue || v instanceof FloatValue || v instanceof DoubleValue)
            return value >= (double) v.value;
        else
            throw new IllegalArgumentException("Different objects' types. Cannot compare.");
    }


    public boolean gt(Value v) {
        if (v instanceof IntegerValue || v instanceof FloatValue || v instanceof DoubleValue)
            return value > (double) v.value;
        else
            throw new IllegalArgumentException("Different objects' types. Cannot compare.");
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
        value = Double.parseDouble(s);
        return this;
    }

}

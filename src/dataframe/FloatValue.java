package dataframe;

import java.io.IOException;
import java.util.Objects;


public class FloatValue extends Value {
    protected float value;


    public FloatValue() {
        value = 0.0f;
    }


    public FloatValue(float _value) {
        value = _value;
    }


    public String toString() {
        return String.valueOf(value);
    }


    public Value add(Value v) {
        if (v instanceof IntegerValue || v instanceof FloatValue || v instanceof DoubleValue) {
            return (new FloatValue(value + (float) v.value));
        }
        else {
            throw new IllegalArgumentException("Different objects' types.");
        }
    }


    public Value sub(Value v) {
        if (v instanceof IntegerValue || v instanceof FloatValue || v instanceof DoubleValue) {
            return (new FloatValue(value - (float) v.value));
        }
        else {
            throw new IllegalArgumentException("Different objects' types.");
        }
    }


    public Value mul(Value v) {
        if (v instanceof IntegerValue || v instanceof FloatValue || v instanceof DoubleValue) {
            return (new FloatValue(value * (float) v.value));
        }
        else {
            throw new IllegalArgumentException("Different objects' types.");
        }
    }


    public Value div(Value v) {
        if (v instanceof IntegerValue || v instanceof FloatValue || v instanceof DoubleValue) {
            try {
                return (new FloatValue(value / (float) v.value));

            } catch (ArithmeticException e) {
                System.out.println("Cannot divide by 0.");
            }

        }
        throw new IllegalArgumentException("Different objects' types.");
    }


    public Value pow(Value v) {
        if (v instanceof IntegerValue || v instanceof FloatValue || v instanceof DoubleValue) {
            return (new IntegerValue((int) Math.pow(value, (int) v.value)));
        }
        else {
            throw new IllegalArgumentException("Different objects' types. Cannot compare.");
        }
    }


    public boolean eq(Value v) {
        if (v instanceof IntegerValue || v instanceof FloatValue || v instanceof DoubleValue)
            return (int) v.value == value;
        else
            throw new IllegalArgumentException("Different objects' types. Cannot compare.");
    }


    public boolean lte(Value v) {
        if (v instanceof IntegerValue || v instanceof FloatValue || v instanceof DoubleValue)
            return value <= (int) v.value;
        else
            throw new IllegalArgumentException("Different objects' types. Cannot compare.");
    }


    public boolean lt(Value v) {
        if (v instanceof IntegerValue || v instanceof FloatValue || v instanceof DoubleValue)
            return value < (int) v.value;
        else
            throw new IllegalArgumentException("Different objects' types. Cannot compare.");
    }


    public boolean gte(Value v) {
        if (v instanceof IntegerValue || v instanceof FloatValue || v instanceof DoubleValue)
            return value >= (int) v.value;
        else
            throw new IllegalArgumentException("Different objects' types. Cannot compare.");
    }


    public boolean gt(Value v) {
        if (v instanceof IntegerValue || v instanceof FloatValue || v instanceof DoubleValue)
            return value > (int) v.value;
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
        FloatValue that = (FloatValue) o;
        return Float.compare(that.value, value) == 0;
    }


    @Override
    public int hashCode() {
        return Objects.hash(value);
    }


    public Value create(String s) {
        value = Float.parseFloat(s);
        return this;
    }
}

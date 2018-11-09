package dataframe;

import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.Objects;


public class DateTimeValue extends Value implements Cloneable, Comparable<Value> {
    private LocalDate value;


    public DateTimeValue() {
        value = LocalDate.now();
    }


    public DateTimeValue(long _milisec) {
        Date date = new Date(_milisec);
        value = LocalDate.ofInstant(date.toInstant(), ZoneId.systemDefault());
    }


    public DateTimeValue(Date _date) {
        value = LocalDate.ofInstant(_date.toInstant(), ZoneId.systemDefault());
    }

    public DateTimeValue(LocalDate _date) {
        value = _date;
    }


    @Override
    public String toString() {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        return value.format(formatter);
    }


    public Value add(Value v) { //add v days to date
        if (v instanceof IntegerValue) {
            return new DateTimeValue(value.plusDays(((IntegerValue)v).value));
        }
        throw new IllegalArgumentException("Invalid operation.");
    }


    public Value sub(Value v) { //subtract v days from date
        if (v instanceof IntegerValue) {
            return new DateTimeValue(value.minusDays(((IntegerValue)v).value));
        }
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
        if (v instanceof DateTimeValue)
            return value.equals(((DateTimeValue) v).value);
        else
            throw new IllegalArgumentException("Different objects' types. Cannot compare.");

    }


    public boolean lte(Value v) {
        if (v instanceof DateTimeValue)
            return (value.isBefore(((DateTimeValue) v).value) || value.isEqual(((DateTimeValue) v).value));
        else
            throw new IllegalArgumentException("Different objects' types. Cannot compare.");
    }


    public boolean lt(Value v) {
        if (v instanceof DateTimeValue)
            return value.isBefore(((DateTimeValue) v).value);
        else throw new IllegalArgumentException("Different objects' types. Cannot compare.");
    }


    public boolean gte(Value v) {
        if (v instanceof DateTimeValue)
            return (value.isAfter(((DateTimeValue) v).value) || value.isEqual(((DateTimeValue) v).value));
        else throw new IllegalArgumentException("Different objects' types. Cannot compare.");
    }


    public boolean gt(Value v) {
        if (v instanceof DateTimeValue)
            return value.isAfter(((DateTimeValue) v).value);
        else throw new IllegalArgumentException("Different objects' types. Cannot compare.");
    }


    public boolean neq(Value v) {
        return !this.eq(v);
    }


    public Value create(String s) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        value = LocalDate.parse(s, formatter);
        return this;
    }


    public DateTimeValue clone() throws CloneNotSupportedException {
        return (DateTimeValue) super.clone();
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DateTimeValue that = (DateTimeValue) o;
        return Objects.equals(value, that.value);
    }


    @Override
    public int hashCode() {
        return Objects.hash(value);
    }

    public int compareTo(Value v){
        if (v instanceof DateTimeValue)
            return compareValuesOfTheSameInstance(v);
        throw new IllegalArgumentException("Different objects' types. Cannot compare.");
    }
}

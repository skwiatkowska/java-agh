package dataframe;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Objects;

public class DateTimeValue extends Value {
    protected int year;
    protected int month;
    protected int day;
    protected int hrs;
    protected int mins;


    public DateTimeValue(int _day, int _month, int _year) {
        if (!isValid(year, month, day)) throw new IllegalArgumentException();
        this.day = _day;
        this.month = _month;
        this.year = _year;
    }


    public DateTimeValue() {
       Calendar currentDate = Calendar.getInstance();
        java.util.Date x = currentDate.getTime();
        SimpleDateFormat formatdd = new SimpleDateFormat("dd");
        day = Integer.parseInt(formatdd.format(x));
        SimpleDateFormat formatmonth = new SimpleDateFormat("MM");
        month = Integer.parseInt(formatmonth.format(x));
        SimpleDateFormat formatyear = new SimpleDateFormat("yyyy");
        year = Integer.parseInt(formatyear.format(x));
    }


    public static boolean isLeap(int _year) {
        Calendar cal = Calendar.getInstance();
        cal.set(Calendar.YEAR, _year);
        return cal.getActualMaximum(Calendar.DAY_OF_YEAR) > 365;
    }


    public static boolean isValid(int _day, int _month, int _year) {
        if (_year < 0) return false;
        if ((_month < 1) || (_month > 12)) return false;
        if ((_day < 1) || (_day > 31)) return false;
        switch (_month) {
            case 1:
                return true;
            case 2:
                return (isLeap(_year) ? _day <= 29 : _day <= 28);
            case 3:
                return true;
            case 4:
                return _day < 31;
            case 5:
                return true;
            case 6:
                return _day < 31;
            case 7:
                return true;
            case 8:
                return true;
            case 9:
                return _day < 31;
            case 10:
                return true;
            case 11:
                return _day < 31;
            default:
                return true;
        }
    }


    @Override
    public String toString() {
        return (year + "-" + month + "-" + year);
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
        if (v instanceof DateTimeValue)
            return year == ((DateTimeValue) v).year && month == ((DateTimeValue) v).month && day == ((DateTimeValue) v).day;
        else throw new IllegalArgumentException("Different objects' types.");

    }


    public boolean lte(Value v) {
        if (v instanceof DateTimeValue)
            return (year < ((DateTimeValue) v).year ||
                    (year == ((DateTimeValue) v).year && month < ((DateTimeValue) v).month) ||
                    (year == ((DateTimeValue) v).year && month == ((DateTimeValue) v).month && day <= ((DateTimeValue) v).day));
        else throw new IllegalArgumentException("Different objects' types.");
    }


    public boolean lt(Value v) {
        if (v instanceof DateTimeValue) {
            return (year < ((DateTimeValue) v).year ||
                    (year == ((DateTimeValue) v).year && month < ((DateTimeValue) v).month) ||
                    (year == ((DateTimeValue) v).year && month == ((DateTimeValue) v).month && day < ((DateTimeValue) v).day));
        }
        else throw new IllegalArgumentException("Different objects' types.");
    }


    public boolean gte(Value v) {
        if (v instanceof DateTimeValue) {
            return (year > ((DateTimeValue) v).year ||
                    (year == ((DateTimeValue) v).year && month > ((DateTimeValue) v).month) ||
                    (year == ((DateTimeValue) v).year && month == ((DateTimeValue) v).month && day >= ((DateTimeValue) v).day));
        }
        else throw new IllegalArgumentException("Different objects' types.");
    }


    public boolean gt(Value v) {
        if (v instanceof DateTimeValue) {
            return (year > ((DateTimeValue) v).year ||
                    (year == ((DateTimeValue) v).year && month > ((DateTimeValue) v).month) ||
                    (year == ((DateTimeValue) v).year && month == ((DateTimeValue) v).month && day > ((DateTimeValue) v).day));
        }
        else throw new IllegalArgumentException("Different objects' types.");
    }


    public boolean neq(Value v) {
        return !this.eq(v);
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DateTimeValue that = (DateTimeValue) o;
        return year == that.year &&
                month == that.month &&
                day == that.day;
    }


    @Override
    public int hashCode() {
        return Objects.hash(year, month, day);
    }


    public Value create(String s) {
        String[] str = s.split("[.]");
        if (str.length != 3) throw new IllegalArgumentException();
        int dd = Integer.parseInt(str[0]);
        int mm = Integer.parseInt(str[1]);
        int yy = Integer.parseInt(str[2]);
        if (!isValid(dd, mm, yy)) throw new IllegalArgumentException();
        year = yy;
        month = mm;
        day = dd;

        return this;
    }
}

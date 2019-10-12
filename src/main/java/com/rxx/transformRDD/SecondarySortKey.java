package com.rxx.transformRDD;

import scala.Serializable;
import scala.math.Ordered;

public class SecondarySortKey implements Ordered<SecondarySortKey>, Serializable {

    //首先定义需要排序的类
    private Integer first;
    private Integer second;

    //提供getter和setter
    public Integer getFirst() {
        return first;
    }

    public void setFirst(Integer first) {
        this.first = first;
    }

    public Integer getSecond() {
        return second;
    }

    public void setSecond(Integer second) {
        this.second = second;
    }

    //提供hashcode和equal方法
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        SecondarySortKey that = (SecondarySortKey) o;

        if (!first.equals(that.first)) {
            return false;
        }
        return second.equals(that.second);
    }

    @Override
    public int hashCode() {
        int result = first.hashCode();
        result = 31 * result + second.hashCode();
        return result;
    }

    //提供构造方法
    public SecondarySortKey(Integer first, Integer second) {
        this.first = first;
        this.second = second;
    }

    public int compare(SecondarySortKey that) {
        if (this.first - that.getFirst() != 0) {
            return this.first - that.getFirst();
        } else {
            return this.second - that.getSecond();
        }
    }

    public boolean $less(SecondarySortKey that) {
        if (this.first < that.getFirst()) {
            return true;
        } else if (this.first.equals(that.getFirst()) && this.second < that.getSecond()) {
            return true;
        }
        return false;
    }

    public boolean $greater(SecondarySortKey that) {
        if (this.first > that.getFirst()) {
            return true;
        } else if (this.first.equals(that.getFirst()) && this.second > that.getSecond()) {
            return true;
        }
        return false;
    }

    public boolean $less$eq(SecondarySortKey that) {
        if (this.$less(that)) {
            return true;
        } else if (this.first.equals(that.getFirst()) && this.second.equals(that.getSecond())) {
            return true;
        }
        return false;
    }

    public boolean $greater$eq(SecondarySortKey that) {
        if (this.$greater(that)) {
            return true;
        } else if (this.first.equals(that.getFirst()) && this.second.equals(that.getSecond())) {
            return true;
        }
        return false;
    }

    public int compareTo(SecondarySortKey that) {
        return this.compare(that);
    }
}

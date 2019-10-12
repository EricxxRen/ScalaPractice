package com.rxx.transformRDD

class SecondarySortKeys (val first: Integer, val second: Integer)
  extends Ordered[SecondarySortKeys] with Serializable {

  override def compare(that: SecondarySortKeys): Int = {
    if (this.first - that.first != 0) {
      this.first - that.first
    } else {
      this.second - that.second
    }
  }

}

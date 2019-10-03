package com.rxx.Type

/**
 * 泛型类
 * @param localId
 * @tparam T
 */
class Student [T] (val localId: T) {
  def getStudentId (hukouId: T): Unit = {
    println("S-" + localId + "-" + hukouId)
  }
}

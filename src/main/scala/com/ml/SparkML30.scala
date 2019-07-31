package com.ml

import breeze.linalg._


/**
  * Created by yqq on 2019/7/28.
  */
object SparkML30 {
  def main(args: Array[String]): Unit = {

    /**
      * dense: 密集的
      */
    val mat1 = DenseMatrix.zeros[Double](2,3)
    println(mat1)

    val v1 = DenseVector.zeros[Double](3)
    println(v1)

    val v2 = DenseVector.ones[Double](3)
    println(v2)

    val v3 = DenseVector.range(1,12,2)
    println(v3)

    val diagMat = diag(DenseVector(1,5,6))
    println(diagMat)





  }

}

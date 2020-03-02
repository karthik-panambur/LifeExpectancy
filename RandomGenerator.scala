package com.bigdata.kafka

import scala.io

class RandomGenerator {

  def getRecord() : String = {
    val bufferedSource = io.Source.fromFile("input/Life Expectancy Data_Copy.csv")
    var matrix: Array[Array[String]] = Array.empty
    var i = 0
    var j = 0
    for (line <- bufferedSource.getLines.drop(1)) {
      var cols = line.split(",").map(_.trim.toString())
      if (cols.length < 22) {
        //println(cols.length)
        val s = cols.length + 1
        for ( x <- s to 22) {
          cols :+= ""
        }
        //println(cols.length)
      }
      matrix = matrix :+ cols
    }
    var rows = matrix.length
    var cols = matrix(0).length
    val max = rows - 1
    val min = 0
    //println("val:" + matrix(432)(20))
    var record = ""
    for (j <- 0 to cols - 1) {
      val random = new scala.util.Random
      val i = min + random.nextInt((max - min) + 1)
      val colEle = matrix(i)(j)
      while(colEle == 0){
        val i = min + random.nextInt((max - min) + 1)
        val colEle = matrix(i)(j)
      }
      record = record + colEle + ','
    }
    record = record.substring(0, record.length - 1)
    println(record)
    return record
  }
  getRecord()
}

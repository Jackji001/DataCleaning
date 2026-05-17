/*
练习读取csv文件

 */

package org.example

import org.apache.spark.{SparkConf, SparkContext}
import java.io.PrintWriter

object af {
  def main(args: Array[String]): Unit = {
    //配置spark
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("DataCleaning_JackJi")
      .set("spark.driver.memory", "1g")
      .set("hadoop.home.dir", "D:/")
      .set("spark.hadoop.dfs.permissions.enabled", "false")

    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN") // 减少日志干扰
    //读取文件
    val read =sc.textFile("src/main/java/org/example/Iris.csv")
    println("csv文件内容")
    val line = read.collect()
    line.foreach(println)
    val data =read.map(line => line.split(","))
    val DataWithoutHeader = read.filter(line => !line.startsWith("Id"))
    val Data3_average = DataWithoutHeader.map(line => {
      val fileds =line.split(",")
      fileds(3).toDouble
    })
    val count = Data3_average.count()
    //求和
    val sum = Data3_average.sum()

    val average = sum / count
    println("平均值：" + average)

    val Data4 = DataWithoutHeader.map(line => {
      val fileds = line.split(",")
      fileds(4).toDouble
    })
    val max = Data4.max()
    println("第五列最大值：" + max)
    //统计列Iris-setosa的平均值
    val setosa = DataWithoutHeader.filter(line => line.split(",")(5) == "Iris-setosa")
    val setosa_average = setosa.map(line => {
      val fileds = line.split(",")
      fileds(3).toDouble
    }).sum() / setosa.count()
    println("Iris-setosa的平均值：" + setosa_average)
    val versicolor = DataWithoutHeader.filter(line => line.split(",")(5) == "Iris-versicolor")
    val versicolor_average = versicolor.map(line => {
      val fileds = line.split(",")
      fileds(3).toDouble
    }).sum() / versicolor.count()
    println("Iris-versicolor的平均值：" + versicolor_average)
    val cha = setosa_average - versicolor_average
    println("Iris-setosa和Iris-versicolor的差值：" + cha)
    val virginica = DataWithoutHeader.filter(line => line.split(",")(5) == "Iris-virginica")
    //分类汇总最后一列的数量
    println("Iris-virginica的数量：" + count)
    //写入
    val OutPath =new PrintWriter("src/main/java/org/example/result/result.txt")
    val writer = new PrintWriter(OutPath)
    writer.println("第4列平均值," + average)
    writer.println("第5列最大值," + max)
    writer.println("Iris-setosa第4列平均值," + setosa_average)
    writer.println("Iris-versicolor第4列平均值," + versicolor_average)
    writer.println("两类平均值差值," + count )
    

    writer.close()

    sc.stop()
  }

}

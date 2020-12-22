package com.doubleriver

import org.apache.flink.api.scala.ExecutionEnvironment
import  org.apache.flink.api.scala._


//批处理的WordCount
object WordCount {
  def main(args: Array[String]): Unit = {
    //创建一个批处理的执行环境
    val env:ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    //从文件中读取数据
    val inptPath:String = "D:\\My\\Project\\Flink\\src\\main\\resources\\hello.txt"
    val inputDateSet:DataSet[String] = env.readTextFile(inptPath)

    //对数据进行转换处理统计，先分词，再按照word进行分组
    val resultDateSet:DataSet[(String,Int)]=inputDateSet
      .flatMap(_.split(" "))  //对读取的数据进行“ ”分割
      .map((_,1))  //转化为tuple的格式
      .groupBy(0)   //以第一个元素为key进行分组
      .sum(1)   //对所有第二个数据进行求和


    //打印输出
    resultDateSet.print()

  }

}

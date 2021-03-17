package com.doubleriver

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._

//流处理wordcount
object StreamWordCount {
  def main(args: Array[String]): Unit = {
    //创建流处理执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //设置并行度，不设置时，默认电脑核数
    //env.setParallelism(2)

    println(args)
    //从外部命令提取主机和端口参数  Run>>Editconfigurations>>program arg    --host localhost --port 7777
    val parameterTool = ParameterTool.fromArgs(args)
    val host = parameterTool.get("host")
    val port = parameterTool.getInt("port")

    println(host)
    println(port)

    //接受一个socket文本流
    val inputDateStream: DataStream[String] = env.socketTextStream(host,port)

    //进行转化处理

    val resultDataStream: DataStream[(String, Int)] = inputDateStream
      .flatMap(_.split(" "))
      .map((_, 1))
      .keyBy(0)
      .sum(1)

    //resultDataStream.print().setParallelism(1) //设置并行1
    resultDataStream.print()

    //启动一个进程，等待数据进入
    env.execute("stream word count")

    //netcat -lk 7777  或  nc  -lk 7777  该端口不断传入数据

  }

}

package org.apache.flink.examples.scala.jdbc

import org.apache.flink.examples.java.jdbc.JdbcReader
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

object ReadMysql {
  def main(args: Array[String]): Unit = {

    //scala代码
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val ds =env.addSource(new JdbcReader)
    ds.print()
    env.execute("flink read mysql")
  }
}

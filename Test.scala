package com.jiin

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SparkSession}

object Test {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("DataFrame").
      setMaster("local[4]")
    var sc = new SparkContext(conf)
    val spark = new SQLContext(sc)
    import spark.implicits._

    ///////////////////////////  데이터 파일 로딩  ////////////////////////////////////
    // 접속정보 설정 (1)
    var staticUrl = "jdbc:oracle:thin:@192.168.110.112:1521/orcl"

    var staticUser = "KOPO"
    var staticPw = "kopo"
    var selloutDb = "KOPO_CHANNEL_SEASONALITY_NEW"

    // 관계형 데이터베이스 Oracle 연결 (2)
    val dataOraclesql= spark.read.format("jdbc").
      option("url",staticUrl).
      option("dbtable",selloutDb).
      option("user",staticUser).
      option("password",staticPw).load

    print(dataOraclesql.show(5))

    var rawDataColumns = rawData.columns
    var
    }
}
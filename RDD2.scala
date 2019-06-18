package com.jiin

import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

object RDD {
  def main(args: Array[String]): Unit = {
    ///////////rddConvert///////////////////////
    val conf = new SparkConf().setAppName("DataFrame").
      setMaster("local[4]")
    var sc = new SparkContext(conf)
    val spark = new SQLContext(sc)

    ///데이터 파일 로딩
    // 파일명 설정 및 파일 읽기
    var rowFile = "kopo_product_volume.csv"

    // 절대경로 입력
    var rowData=
      spark.read.format("csv").
        option("header","true").
        option("Delimiter",",").
        load("c:/spark/bin/data/"+rowFile)

    // 데이터 확인
    println(rowData.show)


    // 데이터 프레임 컬럼별 인덱스 생성
    var rawDataColumns = rowData.columns
    var regionNo = rawDataColumns.indexOf("REGIONID")
    var productNo = rawDataColumns.indexOf("PRODUCTGROUP")
    var yearweekNo = rawDataColumns.indexOf("YEARWEEK")
    var qtyNo = rawDataColumns.indexOf("VOLUME")

    // rawData.columns
    // var rawDataColumns = rawData.columns
    // rawDataColumns.indexOf("REGINID")
    // var regionNo = rawDataColumns.indexOf("REGINID")
    // var productgNo = rawDataColumns.indexOf("PRODUCT")  -1이 나오면 그컬럼은 없다는 뜻
    // var productgNo = rawDataColumns.indexOf("PRODUCTGROUP")


    // 데이터프레임 -> RDD 변환 (1)
    var rawRdd = rowData.rdd

    // RDD 프린트 찍는 코드
    rawRdd.collect.foreach(println)
    filteredRdd1.collect.foreach(println)

    //디버깅 코드2
    //기본키를 뽑아내면 무조건 한 행만 가지고 디버깅을 펼칠 수 있는 것이다
    var x = rawRdd.filter(x=>{
      var checkValid = false
      if(x.getString(yearweekNo) == "201512") {
        checkValid = true
      }
        checkValid
    }).first

    //데이터 정제 regionid, productgroup, yearweek, qty


    //데이터 정제
    var filteredRdd1 = rawRdd.filter(x=>{
      //디버깅 코드
//      var x = rawRdd.first

      // 한줄씩 True or False 체크
      var checkValid = true

      //한행 내 특정 컬럼 추출
      var yearweek = x.getString(yearweekNo)
      var regionName = x.getString(regionNo)

      //조건 수행
      if (yearweek.length>6) {
        checkValid = false
        /////false = checkValid2로 빠짐///////
      }
      checkValid
    })
    filteredRdd1.first

 var paramWeek = "53"

    ///퀴즈/////
    var filteredRddQuiz = rawRdd.filter(x=>{
      //디버깅 코드
      // var x = rawRdd.first

      //한행씩
      var checkValid = true

      var yearweek = x.getString(yearweekNo)

      //조건 수행
      if(yearweek.substring(4,6) == paramWeek) {
        checkValid =false
      }
      checkValid

    })


    var MAXQTY = 700000
    var upperLimitSellout = filteredRddQuiz.map(row=>{
    //디버깅 코드
    //var row = filteredRddQuiz.first


      //Logic = if qty is upper than maxqty change to maxqty
      var org_qty = row.getString(qtyNo).toDouble ///캐스팅///
      var new_qty = 0.0d
      if(org_qty >= MAXQTY) {
        new_qty = MAXQTY
      }else{
        new_qty = org_qty
      }

      //Define output columns.
      Row(row.getString(regionNo),
        row.getString(productNo),
        row.getString(yearweekNo),
        new_qty)
//      Row(row.getString(regionNo),
//        row.getString(productNo),
//        row.getString(yearweekNo),
//        new_qty*1.2)
    })


    var MINQTY = 150000
    var lowerLimitSellout = filteredRdd1.map(row=>{
      // 디버깅 코드
      // var row = filteredRdd.first
      // Logic : if qty is upper than maxqty change to maxqty
      var org_qty = row.getString(qtyNo).toDouble
      var new_qty = 0.0d
      if(org_qty > MINQTY){
        new_qty = 150000
      }else{
        new_qty = org_qty
      }
      Row(row.getString(regionNo),
        row.getString(productNo),
        row.getString(yearweekNo),
        new_qty)
    })


    var MINQTY2 = 150000
    var lowerLimitSellout = filteredRdd1.map(row=>{
      // 디버깅 코드
      // var row = filteredRdd.first
      // Logic : if qty is upper than maxqty change to maxqty
      var org_qty = row.getString(qtyNo).toDouble
      var new_qty = 0.0d
      if(org_qty < MINQTY2){
        new_qty = 150000
      }else{
        new_qty = org_qty
      }
      Row(row.getString(yearweekNo),
        row.getString(productNo),
        row.getString(regionNo),
        row.getString(qtyNo))
//        new_qty*1.2)
    })

    lowerLimitSellout.collect.foreach(println)

    // 문제
    var replaceRdd = rawRdd.filter(x=>{
      var checkValid = true
      var yearweek = x.getString(productNo)
      if((yearweek.length < 6) &&
      (yearweek.substring(4,6).toInt <= 52)){
        checkValid = false
      }
      checkValid
    })

    // 문제
    var replaceRdd = rawRdd.filter(x=>{
      var checkValid = true
      var yearweek = x.getString(yearweekNo)
      if((yearweek.length < 6) ||
      (yearweek.substring(4,6).toInt <= 52)){
        checkValid = false
      }
      checkValid
    })


    // 다른 디버깅 방법
    var x = rawRdd.filter(x=>{
      var checkValid = false
      if(x.getString(yearweekNo) == "201512"){
        checkValid = true
      }
      checkValid
    }).first
    // 기본키만 조건을 넣으면 한행만 나온다.



    // 디버깅 방법
    //    var filteredRdd = rawRdd.filter(x = >{
    // 디버깅 코드
    // var x = rawRdd.first
    // 한줄씩 Ture or False 체크
    //      var checkValid = Ture
    //      var yearweek = x.getString(yearweekNo)
    //      var regionName = x.getString(0)
    // 한행 내 특정 컬럼 추출
    //      var yearweek = x.getString(yearweekNo)
    //
    // 조건 수행
    //      if((yearweek.length > 6) &&
    //        (regionName != "A01")){
    //        checkValid=False
    //      }
    //      checkValid
    //    })


    var MAX_VOLUME = 700000
    //전역변수로 설정해놓으면 지역에서는 var하면 안됨 에러남

    // 거래량이 최대거래량 (70만) 이상인 경우 최대 거래량 값으로 변경
    var filteredRdd = rawRdd.
      map(row=>{
        var orgColumns = row.toSeq.toList
        var volume = row.getString(qtyNo).toDouble
        if(volume >= MAX_VOLUME){
          volume = MAX_VOLUME
        }
        (orgColumns :+ volume)
      })

    // rdd 행 개수
    var rowCount = filteredRdd.count
    // rdd 첫번째 행 반환
    var firstRow = filteredRdd.first

/////그룹바이 /////////////////
    var groupRdd = lowerLimitSellout.groupBy(x=>{
        //define keys
        (x.getString(regionNo),
          x.getString(productNo))
    }).map(x=>{
      //code for debug

        // 키 값 저장
      var key = x._1
        // data 값 저장
      var data = x._2

      //calculate average for each group(region,product)
      var sumation = data.map(x=>{
        x.getDouble(qtyNo)}).sum
      var size = data.size

      var average = 0.0d
      if(size!=0){
        average = sumation/size
      }else {
        average = 0.0d
      }
      (key,average)
    })
/////그룹바이 많이 쓰는 이유가 key,value로 함수처럼 그룹Rdd로 쓸 수없을까 해서 나온거
    ////키를 던지면 어떤 밸류가 나올 수 있도록
    ///내가 필요할 때마다 키를 던지면 밸류를 줄 수 있도록 만들어보자.

    //확인
    groupRdd.collect.foreach(println)

    //Group Map 합수 생성
    var groupMap = groupRdd.collectAsMap()
    //collectAsMap을 해두면 키를 던지고 값을 바로 받아볼 수 있다//

    //Group Map 활용
    var targetRegion = "A01"
    var targetProduct = "ST0001"
    var testValue = 0.0d
    if(groupMap.contains(targetRegion,targetProduct)){ //.contains : 관계가 있는지 없는지
      var testValue = groupMap(targetRegion,targetProduct)
    }

    var nextRdd = upperLimitSellout.map(x=>{
      var org_qty = x.getDouble(qtyNo)
      ///디버깅 코드////
    })

    var filteredRdd = rawRdd.filter(x=>{
      var checkValid = true
      var yearweek = x.getString(yearweekNo)
      var regionName = x.getString(regionNo)
      var productName = x.getString(productNo)
      var qty = x.getString(qtyNo)
      if()
    })

  }
}

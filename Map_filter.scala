package com.jiin


object Map_filter {
  def main(args: Array[String]): Unit = {
    // 데이터 파일 로딩
    // 파일명 설정 및 파일 읽기 (2)
    // 기본 데이터 불러오기
    var rawFile = "kopo_product_volume.csv"

    // 절대경로 입력
    var rawData =
      spark.read.format("csv").
        option("header", "true").
        option("Delimiter", ",").
        load("c:/spark/bin/data/" + rawFile)

    // 파라미터 데이터 불러오기
    /// 데이터 파일 로딩
    // 접속정보 설정 (1)
    var staticUrl = "jdbc:oracle:thin:@192.168.110.112:1521/orcl"
    var staticUser = "kopo"
    var staticPw = "kopo"
    var selloutDb = "hk_parameter"

    // 관계형 데이터베이스 Oracle 연결 (2)
    val paramFromOracle = spark.read.format("jdbc").
      option("url", staticUrl).
      option("dbtable", selloutDb).
      option("user", staticUser).
      option("password", staticPw).load
    // 데이터프레임 -> RDD
    var paramRdd = paramFromOracle.rdd

    // 파라미터 테이블 인덱스컬럼생성
    var paramDataColumns = paramFromOracle.columns
    var paramctNo = paramDataColumns.indexOf("PARAM_CATEGORY")
    var paramnmNo = paramDataColumns.indexOf("PARAM_NAME")
    var paramvalueNo = paramDataColumns.indexOf("PARAM_VALUE")

    var testRdd = paramRdd.
      groupBy(x => {
        (x.getString(paramctNo), // category
          x.getString(paramnmNo))
      }). // parameter_name
      map(x => {
      // key 값 저장
      var key = x._1
      // data 값 저장
      var data = x._2
      var param_value = data.map(x => {
        x.getString(paramvalueNo) // parameterValue
      }).toArray

      (key, param_value)
    })

    var paramMap = testRdd.collectAsMap()

    // 데이터프레임 컬럼 인덱스
    var rawDataColumns = rawData.columns
    var regionNo = rawDataColumns.indexOf("REGIONID")
    var productNo = rawDataColumns.indexOf("PRODUCTGROUP")
    var yearweekNo = rawDataColumns.indexOf("YEARWEEK")
    var qtyNo = rawDataColumns.indexOf("VOLUME")

    // productGroupList
    var regionGroupListSplit = Array("A01", "A02") // default

    if (paramMap.contains("COMMON", "VALID_REGION")) {
      regionGroupListSplit = paramMap("COMMON", "VALID_REGION")
    }

    // 지역, 상품 필터링
    // VALID_REGION, VALID_PRODUCT
    var REGIONSET = if (paramMap.contains("COMMON", "VALID_REGION")) {
      paramMap("COMMON", "VALID_REGION").toSet
    } else {
      List("ALL").toSet
    }
    var PRODUCTSET = if (paramMap.contains("COMMON", "VALID_PRODUCT")) {
      paramMap("COMMON", "VALID_PRODUCT").toSet
    } else {
      List("ALL").toSet
    }

    var filterRdd1 = rawData.filter(x => {
      var checkValid = false
      var productInfo = x.getString(productNo)
      var regionInfo = x.getString(regionNo)
      if (
        (REGIONSET.contains(regionInfo)) ||
          (PRODUCTSET.contains(productInfo))) {
        checkValid = true
      }
      checkValid
    })


    //여기부터 내가 친 code
    // : groupBy in flatMap 안에서 내가 추가하고 싶은 계산값들을 새로운 컬럼으로 만들어서 데이터를 뽑아내자는 컨셉
    var flatGroup = filterRdd1.groupBy(x => {  //groupBY 시작
      ( x.getString(regionNo),  //지역
        x. getString(productNo) //상품
      )
    }).flatMap(x=>{ //flatMap : 각 입력 데이터에 대해 여러 개의 아웃풋 데이터를 생성하기 위해사용
      //code start
      var key = x._1
      var data = x._2
      var size = data.size //데이터의 사이즈를 구하기 위해서 전역변수로 설정
      var outputData = data.map(x=>{
        (
          x.getString(regionNo),
          x.getString(productNo),
          x.getString(yearweekNo),
          x.getString(qtyNo),
          size //기존에 있는 데이터를 유지하면서 데이터에 대한 size가 나올 수 있도록 size 컬럼을 추가
        )
      })
      outputData
    })

    //row 형태가 아닌 ._x 형태의 rdd를 데이터 프레임으로 변환하고 싶을 때는
    // flatGroup.toDF

    //단, 컬럼명이 자동으로 ._1 , ._2 ...등으로 표현되기때문에 컬럼명을 바꾸고 싶다면 아래처럼 컬럼명 지정
    // flatGroup.toDF("AA", "BB", "CC", "DD", "EE")



    //퀴즈문제
    var flatGroupAnswer = mapRdd.groupBy(x => {  //groupBY 시작
      ( x.getString(keyNo),
      )
    }).flatMap(x=>{

      //코드시작
      var key = x._1
      var data = x._2

      //size, average, stddev 구하기

      //step1: calculate size
      var size = data.size

      //step2 : average
      var qtyList = data.map(x=>{x.getDouble(qtyNo)})
      var qtyArray = qtyList.toArray //어떤 리스트든 double로 바꿀 수 있음
      var qtySum = qtyArray.sum
      var qtyMean = if(size!=0){
        qtySum/size
      }else{
        0.0
      }

      //step3 : 표준편차
      //qtyList에서 각각의 값에서 평균을 전부 빼야함
      var stdev = if( size!=0) {
        Math.sqrt(
          qtyList.map(x=>{ Math.pow(x-qtyMean,2 )}).sum/size)
      } else {
        0.0
      }

      / /output
      var outputData = data.map(x=>{
        var org_qty = x.getDouble(qtyNo)
        var seasonality = if(qtyMean!=0){
          org_qty/qtyMean
        }
        (
          x.getString(keyNo),
          x.getString(accountidNo),
          x.getString(productNo),
          x.getString(yeqrweekNo),
          x.getDouble(qtyNo),
          size,
          qtyMean,
          stdev,
          seasonality
        )
        outputData
      })


      //코드 끝
      })

  }
}
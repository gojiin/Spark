package com.jiin

import com.jiin.RDD.rawRdd
import org.apache.spark
import org.apache.spark.sql.Row

object RDD {


  var rawFile ="kopo_product_volume.csv"

  var rawData=
    spark.read.format("csv").
      option("header","true").
      load("c:/spark/bin/data/"+rawFile)

  var rawRdd = rawData.rdd

  //  데이터 불러온 후 컬럼 인덱싱 처리 한 번 해줘
  var rawDataColumns = rawData.columns
  var regionNo = rawDataColumns.indexOf("REGIONID")
  var productNo = rawDataColumns.indexOf("PRODUCTGROUP")
  var yearweekNo = rawDataColumns.indexOf("YEARWEEK")
  var qtyNo = rawDataColumns.indexOf("VOLUME")

  //  regionid, productgroup,yearweek, qty-> 몇번째인지 알려면 가독성이 떨어지니까
  //  위에처럼 컬럼의 정보를 담은 인덱스를 만든다.

  //  원하는 부분만 디버깅...기본키 컬럼을 담으면 원하는 행을 뽑아내서 볼 수 있다.
  var mapRdd = rawRdd.filter(x=>{
    var checkValid = false
    if(x.getString(yearweekNo) == "201502"){
      checkValid = true
    }
    checkValid
  })
  //  dataframe에 있는 애를 array에 다 담아
  var rawDataColumns = rawData.columns

  var regionNo = rawDataColumns.indexOf("REGIONID")
  //   _> 0번째 있다
  var productNo = rawDataColumns.indexOf("PRODUCT")
  //  -. -1은 없다


  //  데이터 정제
  var filteredRdd = rawRdd.filter(x=>{

    //   디버깅 코드
    //    var x = rawRdd.first  -> filter 하기전에 first해주면, 무작위로 하나 가져와서 아래 진행시킨걸 볼 수 있는
    //  한줄씩 True or False 체크
    var checkValid = true
    //    한 행 내 특정 컬럼 추출 : getString함수는 컬럼내 컬럼 값을 찾아와
    var yearweek = x.getString(2)
    //    조건 수행
    if(yearweek.length > 6) {
      checkValid = false
    }
    checkvalid
  })

    //    more
    var regionName = x.getString(0)

    if((yearweek.length > 6) &&
      (regionName !="A01")) {
      checkValid = false
    }


    //    디버깅
    var x = rawRdd.filter(x2=>{
      x2.getString(yearweekNo) = "201512"
    }).first



Row(row)

    //    실습
    var rawFile ="kopo_channel_seasonality.csv"

    var rawData=
      spark.read.format("csv").
        option("header","true").
        option("Delimiter",".").
        load("C:\\spark\\bin\\data\\"+rawFile)

    var rawRdd = rawData.rdd
  })


  //    그룹 데이타 확인하기
  var groupRdd = rawRdd.groupBy(x=>{
    //define keys
    (x.getString(regionNo), x.getString(qtyNo))
  }).map(x=>{
    //code for debug
    var key = x._1
    var data = x._2
    var size = data.size
    (key, size)
  })

//  데이터의 사이즈 알기
  data.size

//  group map 함수 생성
  var groupMap = groupRdd.collectAsMap()

//  Group MAP 활용
  var targetRegion = "A01"
  var targetProduct = "ST0001"
  if(groupMap.contains(targetRegion, targetProduct)){
    var testValue = groupMap(targetRegion, targetProduct)
  }
}

package cn.sks.util

import org.apache.spark.sql.{DataFrame, SparkSession}

object ExcelParseUtil {

  def parseExcel(spark:SparkSession,excelPath:String):DataFrame={

    val excelDf = spark.read.format("com.crealytics.spark.excel")
      .option("header", "true")// 是否将第一行作为表头
      .option("inferSchema", "false")//这是自动推断属性列的数据类型
      //      .option("dataAddress", "'Sheet2'!A1:G35") //指定 sheet 以及范围
      .option("treatEmptyValuesAsNulls", "true")//default: true
      .option("addColorColumns", "false") // Optional, default: false
      .option("workbookPassword", "None")// excel文件的打开密码
      .load(excelPath)

    excelDf.show(3)
    excelDf
  }


}

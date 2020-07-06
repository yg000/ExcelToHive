package cn.sks

import cn.sks.util.ExcelParseUtil
import org.apache.spark.sql.{DataFrame, SparkSession}

object ExcelToHive {
  def main(args: Array[String]): Unit = {
    val target_table= "ods.o_manual_excel_project"
    val excelPath= "C:\\Users\\admin\\Desktop\\学部委员\\test.xlsx"

    toHive(target_table,excelPath)
  }

  def toHive(target_table:String,excelPath:String)={
    val spark = SparkSession.builder()
      .master("local[12]")
      .appName("ExcelToHive")
      .config("hive.metastore.uris","thrift://10.0.82.132:9083")
      .enableHiveSupport()
      .getOrCreate()

    spark.sql(s"select * from ${target_table}").count()

    var origin: DataFrame = ExcelParseUtil.parseExcel(spark,excelPath)
    val origin_list = origin.schema.fieldNames.toList

    val start_num= spark.sql(s"select * from ${target_table}").count()
    println("hive库初始的数据量--------"+start_num)
    println("Excel中的数据量----------------"+origin.count())

    val target = spark.sql(s"select * from   ${target_table}")
    val target_list: List[String] = target.schema.fieldNames.toList

    val diff_list = target_list diff origin_list
    import org.apache.spark.sql._
    val udf_null = functions.udf(()=>null)
    diff_list.foreach(x=>{
      origin=origin.withColumn(x,udf_null())
    })

    val build = new StringBuilder
    build.append("select ")
    target.schema.fieldNames.foreach(x=>{
      build.append(x+",")
    })

    origin.createOrReplaceTempView("temp")
    val sql_str =s"insert into ${target_table}  "+ build.toString().stripSuffix(",") + "  from temp  where id is not null"

    spark.sql(sql_str)

    val end_num= spark.sql(s"select * from ${target_table}").count()
    println("Excel写入后hive库后的的数据量--------"+end_num)
    val num = end_num-start_num
    println("写入hive库中的数据量--------"+num)


    println(s"恭喜你 ====》 insert into ${target_table} success ！")

  }


}

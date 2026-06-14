package com.ecommerce

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object EcommerceSessionJob {

  val schema: StructType = StructType(Seq(
    StructField("event_time",    StringType,  nullable = true),
    StructField("event_type",    StringType,  nullable = true),
    StructField("product_id",    LongType,    nullable = true),
    StructField("category_id",   LongType,    nullable = true),
    StructField("category_code", StringType,  nullable = true),
    StructField("brand",         StringType,  nullable = true),
    StructField("price",         DoubleType,  nullable = true),
    StructField("user_id",       LongType,    nullable = true),
    StructField("user_session",  StringType,  nullable = true)
  ))

  def main(args: Array[String]): Unit = {
    val inputPath  = args(0)  // e.g. "data/*.csv"
    val outputPath = args(1)  // e.g. "hdfs:///data/ecommerce/sessions"
    val hiveTable  = args(2)  // e.g. "ecommerce.user_activity"

    val spark = SparkSession.builder()
      .appName("EcommerceSessionJob")
      .enableHiveSupport()
      .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
      .getOrCreate()

    // 1. CSV 읽기
    val raw = spark.read
      .option("header", "true")
      .schema(schema)
      .csv(inputPath)

    // 2. UTC → KST 변환, dt 파티션 컬럼 생성
    val parsed = raw
      .filter(col("user_id").isNotNull && col("event_time").isNotNull)
      .withColumn("event_time_kst",
        from_utc_timestamp(
          to_timestamp(
            regexp_replace(col("event_time"), " UTC$", ""),
            "yyyy-MM-dd HH:mm:ss"
          ),
          "Asia/Seoul"
        )
      )
      .filter(col("event_time_kst").isNotNull)
      .withColumn("dt", date_format(col("event_time_kst"), "yyyy-MM-dd"))
      .drop("event_time")

    // 3. 세션 ID 생성
    val withSessions = SessionGenerator.generate(parsed)

    // 4. parquet + snappy 로 dt 파티션별 저장
    withSessions
      .write
      .mode("overwrite")
      .partitionBy("dt")
      .option("compression", "snappy")
      .parquet(outputPath)

    // 5. Hive 메타스토어에 신규 파티션 등록
    spark.sql(s"MSCK REPAIR TABLE $hiveTable")

    spark.stop()
  }
}

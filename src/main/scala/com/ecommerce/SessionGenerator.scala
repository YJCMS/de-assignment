package com.ecommerce

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object SessionGenerator {

  private val SESSION_TIMEOUT_SECONDS = 5 * 60

  def generate(df: DataFrame): DataFrame = {
    val w = Window.partitionBy("user_id").orderBy("event_time_kst")

    df
      .withColumn("prev_event_time",
        lag("event_time_kst", 1).over(w))
      .withColumn("is_new_session",
        when(
          col("prev_event_time").isNull ||
          unix_timestamp(col("event_time_kst")) - unix_timestamp(col("prev_event_time")) >= SESSION_TIMEOUT_SECONDS,
          1
        ).otherwise(0))
      .withColumn("session_num",
        sum("is_new_session").over(w))
      .withColumn("session_id",
        concat(col("user_id").cast("string"), lit("_"), col("session_num").cast("string")))
      .drop("prev_event_time", "is_new_session", "session_num")
  }
}

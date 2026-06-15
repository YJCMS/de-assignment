#!/bin/bash
set -e

PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
JAR="$PROJECT_DIR/target/scala-2.13/ecommerce-spark-job.jar"
INPUT_PATH="$PROJECT_DIR/data/*.csv"
OUTPUT_PATH="$PROJECT_DIR/output/sessions"
HIVE_TABLE="ecommerce.user_activity"
WAREHOUSE_DIR="$PROJECT_DIR/spark-warehouse"

mkdir -p "$OUTPUT_PATH"
mkdir -p "$WAREHOUSE_DIR"

# Derby metastore_db는 실행 디렉토리에 생성됨 — 모든 spark 명령이 같은 메타스토어 공유
cd "$PROJECT_DIR"

echo "=== [1/3] Hive 테이블 생성 ==="
spark-sql \
  --conf "spark.sql.warehouse.dir=$WAREHOUSE_DIR" \
  -e "
CREATE DATABASE IF NOT EXISTS ecommerce;
CREATE EXTERNAL TABLE IF NOT EXISTS ecommerce.user_activity (
  event_time_kst  TIMESTAMP,
  event_type      STRING,
  product_id      BIGINT,
  category_id     BIGINT,
  category_code   STRING,
  brand           STRING,
  price           DOUBLE,
  user_id         BIGINT,
  user_session    STRING,
  session_id      STRING
)
PARTITIONED BY (dt STRING)
STORED AS PARQUET
LOCATION '$OUTPUT_PATH'
TBLPROPERTIES ('parquet.compress' = 'SNAPPY');
"

echo "=== [2/3] Spark 잡 실행 (CSV → 세션 생성 → Parquet 저장) ==="
spark-submit \
  --master "local[*]" \
  --driver-memory 8g \
  --conf "spark.sql.warehouse.dir=$WAREHOUSE_DIR" \
  --conf "spark.sql.shuffle.partitions=200" \
  --class com.ecommerce.EcommerceSessionJob \
  "$JAR" \
  "$INPUT_PATH" \
  "$OUTPUT_PATH" \
  "$HIVE_TABLE"

echo "=== [3/3] WAU 쿼리 실행 ==="
spark-sql \
  --conf "spark.sql.warehouse.dir=$WAREHOUSE_DIR" \
  -e "
-- 6-a: user_id 기준 WAU
SELECT year(dt) AS year, weekofyear(dt) AS week, COUNT(DISTINCT user_id) AS wau
FROM ecommerce.user_activity
GROUP BY year(dt), weekofyear(dt)
ORDER BY year, week;

-- 6-b: session_id 기준 WAU
SELECT year(dt) AS year, weekofyear(dt) AS week, COUNT(DISTINCT session_id) AS wau_by_session
FROM ecommerce.user_activity
GROUP BY year(dt), weekofyear(dt)
ORDER BY year, week;
"

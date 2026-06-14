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
LOCATION 'hdfs:///data/ecommerce/user_activity'
TBLPROPERTIES ('parquet.compress' = 'SNAPPY');

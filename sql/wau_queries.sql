-- 6-a. user_id 기준 WAU
-- 주(week) 내에 이벤트가 1건 이상 존재한 고유 user_id 수
SELECT
  year(dt)       AS year,
  weekofyear(dt) AS week,
  COUNT(DISTINCT user_id) AS wau
FROM ecommerce.user_activity
GROUP BY year(dt), weekofyear(dt)
ORDER BY year, week;

-- 결과 (데이터: 2019-Oct.csv + 2019-Nov.csv, KST 기준)
-- year  week  wau
-- 2019  40    818388
-- 2019  41    1057958
-- 2019  42    1090898
-- 2019  43    1093146
-- 2019  44    1054722
-- 2019  45    1321141
-- 2019  46    1543309
-- 2019  47    1376755
-- 2019  48    1176254


-- 6-b. session_id 기준 WAU
-- 주(week) 내에 발생한 고유 session_id 수
SELECT
  year(dt)       AS year,
  weekofyear(dt) AS week,
  COUNT(DISTINCT session_id) AS wau_by_session
FROM ecommerce.user_activity
GROUP BY year(dt), weekofyear(dt)
ORDER BY year, week;

-- 결과 (데이터: 2019-Oct.csv + 2019-Nov.csv, KST 기준)
-- year  week  wau_by_session
-- 2019  40    1570536
-- 2019  41    2154180
-- 2019  42    2257214
-- 2019  43    2153837
-- 2019  44    2115233
-- 2019  45    2751842
-- 2019  46    4754423
-- 2019  47    2876494
-- 2019  48    2376156

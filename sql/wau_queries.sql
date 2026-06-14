-- 6-a. user_id 기준 WAU
-- 주(week) 내에 이벤트가 1건 이상 존재한 고유 user_id 수
SELECT
  year(dt)       AS year,
  weekofyear(dt) AS week,
  COUNT(DISTINCT user_id) AS wau
FROM ecommerce.user_activity
GROUP BY year(dt), weekofyear(dt)
ORDER BY year, week;


-- 6-b. session_id 기준 WAU
-- 주(week) 내에 발생한 고유 session_id 수
SELECT
  year(dt)       AS year,
  weekofyear(dt) AS week,
  COUNT(DISTINCT session_id) AS wau_by_session
FROM ecommerce.user_activity
GROUP BY year(dt), weekofyear(dt)
ORDER BY year, week;

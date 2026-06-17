-- WAU 결과 검증 쿼리

-- ============================================================
-- 검증 1: 전체 unique user 수
-- WAU 단순합보다 작아야 정상 (동일 유저가 여러 주에 걸쳐 등장)
-- ============================================================
SELECT COUNT(DISTINCT user_id) AS total_unique_users
FROM ecommerce.user_activity;

-- 결과: 5,316,649명
-- WAU 단순합: 818388+1057958+1090898+1093146+1054722+1321141+1543309+1376755+1176254 = 9,532,571
-- 5,316,649 < 9,532,571 → 정상


-- ============================================================
-- 검증 2: 주별 WAU + session/user 비율
-- 정상 범위: 1.8~2.5 / 3.0 이상이면 이벤트성 트래픽 가능성
-- ============================================================
SELECT
  year(dt)                          AS year,
  weekofyear(dt)                    AS week,
  COUNT(DISTINCT user_id)           AS wau_user,
  COUNT(DISTINCT session_id)        AS wau_session,
  ROUND(COUNT(DISTINCT session_id) /
        COUNT(DISTINCT user_id), 2) AS session_per_user
FROM ecommerce.user_activity
GROUP BY year(dt), weekofyear(dt)
ORDER BY year, week;

-- 결과:
-- year  week  wau_user   wau_session  session_per_user
-- 2019  40    818388     1570536      1.92
-- 2019  41    1057958    2154180      2.04
-- 2019  42    1090898    2257214      2.07
-- 2019  43    1093146    2153837      1.97
-- 2019  44    1054722    2115233      2.01
-- 2019  45    1321141    2751842      2.08
-- 2019  46    1543309    4754423      3.08  ← session 수 급등
-- 2019  47    1376755    2876494      2.09
-- 2019  48    1176254    2376156      2.02
--
-- week 46(11/11~17)만 비율 3.08로 이상 → 검증 4에서 이벤트 수도 27M으로 2~3배 확인됨
-- 원인: 외부 자료로 특정 불가. 플랫폼 내부 프로모션 가능성 있음.


-- ============================================================
-- 검증 3: 이벤트 볼륨 비교
-- WAU와 같은 방향으로 증감해야 정상
-- ============================================================
SELECT
  year(dt)                   AS year,
  weekofyear(dt)             AS week,
  COUNT(*)                   AS total_events,
  COUNT(DISTINCT user_id)    AS unique_users,
  COUNT(DISTINCT session_id) AS unique_sessions
FROM ecommerce.user_activity
GROUP BY year(dt), weekofyear(dt)
ORDER BY year, week;

-- 결과:
-- year  week  total_events  unique_users  unique_sessions
-- 2019  40    7203287       818388        1570536
-- 2019  41    9749806       1057958       2154180
-- 2019  42    10375677      1090898       2257214
-- 2019  43    9718290       1093146       2153837
-- 2019  44    9484622       1054722       2115233
-- 2019  45    12548434      1321141       2751842
-- 2019  46    27263837      1543309       4754423  ← 이벤트 수 2~3배 급등
-- 2019  47    12923809      1376755       2876494
-- 2019  48    10682981      1176254       2376156
--
-- 이벤트 볼륨 ↑ ↔ WAU ↑ 방향 일치 → 로직 정합성 확인
-- week 46 급등 원인: 외부 자료로 특정 불가. 플랫폼 내부 프로모션 가능성 있음.


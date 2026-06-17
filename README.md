# Ecommerce Session Pipeline

Kaggle ecommerce-behavior-data를 Hive External Table로 제공하기 위한 Spark 배치 파이프라인입니다.

## 프로젝트 구조

```
de-assignment/
├── build.sbt                              # Spark 4.1.2, Scala 2.13.17
├── project/
│   ├── build.properties                   # sbt 1.12.12
│   └── plugins.sbt                        # sbt-assembly 2.1.1
├── src/main/scala/com/ecommerce/
│   ├── EcommerceSessionJob.scala          # main + 전체 파이프라인
│   └── SessionGenerator.scala             # 세션 ID 생성 로직
├── sql/
│   ├── create_hive_table.sql              # External Table DDL
│   ├── wau_queries.sql                    # WAU 쿼리 + 결과값
│   └── validation.sql                     # (추가) WAU 결과 검증 쿼리
├── scripts/
│   └── run_local.sh                       # 로컬 실행 스크립트
└── data/                                  # 실행 전 해당 폴더에 CSV 원본 추가 필요
    ├── 2019-Oct.csv
    └── 2019-Nov.csv
```

## 실행 방법

### 1. 데이터 준비

Kaggle에서 데이터셋 다운로드 후 `data/` 디렉토리에 위치시킵니다.

```
https://www.kaggle.com/mkechinov/ecommerce-behavior-data-from-multi-category-store
```

### 2. 빌드

```bash
sbt assembly
# 결과물: target/scala-2.13/ecommerce-spark-job.jar
```

빌드 성공 확인:
```bash
cat target/streams/_global/assembly/_global/streams/out | grep "Built:"
```

### 3. 실행

로컬 전체 실행 (Hive 테이블 생성 → Spark 잡 → WAU 쿼리):

```bash
bash scripts/run_local.sh
```

클러스터 환경에서는 직접 spark-submit:

```bash
spark-submit \
  --master yarn \
  --class com.ecommerce.EcommerceSessionJob \
  target/scala-2.13/ecommerce-spark-job.jar \
  "hdfs:///data/raw/*.csv" \
  "hdfs:///data/ecommerce/user_activity" \
  "ecommerce.user_activity"
```

인자 순서: `<inputPath> <outputPath> <hiveTable>`

---

## 설계 설명

### KST 변환 및 파티션

원본 `event_time` 컬럼은 `"2019-10-01 00:00:00 UTC"` 형식의 문자열입니다.

```scala
regexp_replace(col("event_time"), " UTC$", "")  // " UTC" 제거
→ to_timestamp(...)                              // Timestamp 변환
→ from_utc_timestamp(..., "Asia/Seoul")          // KST 변환
→ date_format(..., "yyyy-MM-dd")                 // dt 파티션 컬럼
```

날짜 변경 예시: UTC `2019-10-01 15:00:00` → KST `2019-10-02 00:00:00` → `dt=2019-10-02`

### 세션 ID 생성

```scala
val w = Window.partitionBy("user_id").orderBy("event_time_kst", "user_session")

lag("event_time_kst", 1).over(w)          // 이전 이벤트 시간
→ 차이 ≥ 300초 or 첫 이벤트 → is_new_session = 1
→ sum("is_new_session").over(w)           // 누적합 = session_num
→ session_id = "{user_id}_{session_num}"  // 예: "519107_0", "519107_1"
```

- 자정에 세션을 강제 종료하지 않음 (5분 규칙만 적용)
- `user_session`을 보조 정렬 키로 추가 → 동일 timestamp 내 순서 결정적(deterministic)

### Parquet + Snappy

```scala
withSessions.write
  .mode("overwrite")
  .partitionBy("dt")
  .option("compression", "snappy")
  .parquet(outputPath)
```

- Parquet: 컬럼형 포맷으로 WAU 같은 집계 쿼리에서 필요한 컬럼만 읽어 I/O 최소화
- Snappy: 압축률과 속도 균형이 좋은 Spark 표준 압축 코덱

### External Table + 추가 기간 대응

```sql
CREATE EXTERNAL TABLE ecommerce.user_activity (...)
PARTITIONED BY (dt STRING)
STORED AS PARQUET
LOCATION 'hdfs:///data/ecommerce/user_activity';
```

- EXTERNAL TABLE: 테이블 DROP 시 데이터 보존 (메타데이터·데이터 분리)
- 추가 기간(예: 2019-Dec) 처리 시: CSV 파일 추가 후 동일 잡 재실행 → `MSCK REPAIR TABLE`로 신규 파티션 자동 등록

### 배치 복구 장치

```scala
SparkSession.builder()
  .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
```

`dynamic partition overwrite` 모드를 사용합니다. 실패한 날짜만 재실행하면 해당 `dt` 파티션만 덮어씌워지고, 정상 완료된 다른 날짜의 파티션은 보존됩니다. 별도 마커 파일이나 복구 클래스 없이 Spark 기본 동작으로 멱등성을 보장합니다.

---

## WAU 결과

데이터: 2019-Oct.csv + 2019-Nov.csv, KST 기준 파티션

### 6-a. user_id 기준 WAU

```sql
SELECT year(dt) AS year, weekofyear(dt) AS week, COUNT(DISTINCT user_id) AS wau
FROM ecommerce.user_activity
GROUP BY year(dt), weekofyear(dt)
ORDER BY year, week;
```

| year | week | wau       |
|------|------|-----------|
| 2019 | 40   | 818,388   |
| 2019 | 41   | 1,057,958 |
| 2019 | 42   | 1,090,898 |
| 2019 | 43   | 1,093,146 |
| 2019 | 44   | 1,054,722 |
| 2019 | 45   | 1,321,141 |
| 2019 | 46   | 1,543,309 |
| 2019 | 47   | 1,376,755 |
| 2019 | 48   | 1,176,254 |

### 6-b. session_id 기준 WAU

```sql
SELECT year(dt) AS year, weekofyear(dt) AS week, COUNT(DISTINCT session_id) AS wau_by_session
FROM ecommerce.user_activity
GROUP BY year(dt), weekofyear(dt)
ORDER BY year, week;
```

| year | week | wau_by_session |
|------|------|----------------|
| 2019 | 40   | 1,570,536      |
| 2019 | 41   | 2,154,180      |
| 2019 | 42   | 2,257,214      |
| 2019 | 43   | 2,153,837      |
| 2019 | 44   | 2,115,233      |
| 2019 | 45   | 2,751,842      |
| 2019 | 46   | 4,754,423      |
| 2019 | 47   | 2,876,494      |
| 2019 | 48   | 2,376,156      |

---

## WAU 결과 검증 (추가)

`sql/validation.sql`에 WAU 결과의 정합성을 확인하기 위한 검증 쿼리를 추가로 작성했습니다.

- **검증 1**: 전체 unique user 수(5,316,649)가 WAU 단순합(9,532,571)보다 작은지 확인 → 동일 유저가 여러 주에 걸쳐 등장하는 정상 패턴 검증
- **검증 2**: 주별 session/user 비율(정상 범위 1.8~2.5) 확인 → week 46(11/11~17)만 3.08로 이상 징후 발견
- **검증 3**: 이벤트 볼륨과 WAU 증감 방향 일치 여부 확인 → week 46 이벤트 수 27M으로 2~3배 급등, 플랫폼 내부 프로모션 추정

---

## 언어 선택 이유: Scala

Scala와 Java 중 **Scala**를 선택했습니다.

- **Spark native 언어**: Spark 자체가 Scala로 작성되어 있어 DataFrame/Dataset API를 타입 안정성을 갖추며 가장 자연스럽게 사용할 수 있습니다.
- **간결함**: Java 대비 코드량이 적습니다. Window 함수 체이닝, 컬럼 표현식 등이 함수형 스타일로 직관적으로 표현됩니다.

---

## AI 도구 활용

**사용 도구**: Claude Code (claude-sonnet-4-6)

### 활용 범위

| 항목 | AI 활용 여부 | 직접 판단 |
|------|------------|---------|
| 프로젝트 구조 초안 | AI 활용 | AI가 제안한 과도한 서브패키지 제거, flat 구조로 결정 |
| 기술 선정 근거 초안 | AI 활용 | 면접 설명 가능 여부 기준으로 직접 검토 |
| 핵심 로직 구현 (SessionGenerator, EcommerceSessionJob) | AI 활용 | 코드 라인별 이해 후 보조 정렬 키 추가 등 수정 |
| SQL (DDL, WAU 쿼리) | AI 활용 | 결과값 이상 여부 직접 검증, week 46 session 급등 원인 분석 |
| 빌드 환경 설정 (build.sbt, run_local.sh) | AI 활용 | Spark 버전 호환성 문제 직접 조사·결정 |
| README | AI 활용 | AI로 초안 작성 후 각 섹션 내용 직접 검토 후 작성 |

### 프롬프트 전략

- **단계적 접근**: 전체를 한번에 구현 요청하지 않고 뼈대 → 설계 검토 → 구현 순서로 진행했습니다.
- **단순화 기준**: AI가 `RecoveryManager` 클래스, `scopt` 라이브러리, 서브패키지 구조를 제안할 때마다 "인터뷰에서 라인 단위로 설명할 수 있는가"를 기준으로 제거했습니다.
- **요구사항 단계별 대조**: 구현 단계마다 과제 요구사항과 직접 대조하여 각 항목의 완성도를 확인했습니다.
- **이해 우선**: 코드 작성 전에 설계를 충분히 논의하여 모든 결정의 이유를 인지한 상태에서 구현을 진행했습니다.

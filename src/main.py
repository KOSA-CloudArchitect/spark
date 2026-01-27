from pyspark.sql import SparkSession
import os
import pyspark.sql.functions as F
from pyspark.sql.types import *

# ==================== Kafka & Paths ====================
BOOTSTRAP = "my-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092"
INPUT_TOPIC = "realtime-review-collection-topic"
OUTPUT_TOPIC = "realtime-review-transform-topic"
CHECKPOINT_PATH = os.getenv("CHECKPOINT_PATH", "s3a://hihypipe-spark-checkpoints-baf915c6/realtime-chk/realtime-review-pipeline-v2")

# ==================== Processing Mode ====================
PER_RECORD_MODE = os.getenv("PER_RECORD_MODE", "true").lower() == "true"

spark = (SparkSession.builder
        .appName("realtime-review-pipeline")
        .getOrCreate())
print(f"[BOOT] CHECKPOINT_PATH= {CHECKPOINT_PATH}")

# 리뷰 데이터 JSON 스키마 정의
review_json_schema = StructType([
    StructField("job_id", StringType(), True),
    StructField("review_id", StringType(), True),
    StructField("product_code", StringType(), True),
    StructField("title", StringType(), True),
    StructField("tag", StringType(), True),
    StructField("star_rating", DoubleType(), True),
    StructField("review_count", IntegerType(), True),
    StructField("sales_price", IntegerType(), True),
    StructField("final_price", IntegerType(), True),
    StructField("review_rating", StringType(), True),
    StructField("review_date", StringType(), True),
    StructField("review_content", StringType(), True),
    StructField("review_keywords", MapType(StringType(), StringType()), True),
    StructField("review_help_cnt", StringType(), True),
    StructField("crawled_at", StringType(), True)
])

# 1) Kafka에서 실시간 스트림 읽기
raw = (spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP)
        .option("subscribe", INPUT_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load())

# 2) 데이터 파싱 및 필터링
parsed_df = raw.select(F.from_json(F.col("value").cast("string"), review_json_schema).alias("data"))

# 디버깅: 스트리밍 시작 로그
print(f"[DEBUG] Starting data processing with PER_RECORD_MODE: {PER_RECORD_MODE}")
print(f"[DEBUG] CHECKPOINT_PATH: {CHECKPOINT_PATH}")
print(f"[DEBUG] Input topic: {INPUT_TOPIC}, Output topic: {OUTPUT_TOPIC}")

# 처리량 모니터링을 위한 간단한 카운터
from pyspark.sql.functions import count


# 데이터 컬럼 이름 정규화
transformed_df = (parsed_df.select("data.*")
    .withColumnRenamed("product_code", "product_id")
    .withColumnRenamed("tag", "category")
    .withColumnRenamed("review_rating", "rating")
    .withColumnRenamed("review_content", "review_text")
    .withColumnRenamed("review_keywords", "keywords")
    .withColumnRenamed("review_help_cnt", "review_help_count")
    .withColumn("rating", F.col("rating").cast("double"))
    # 전체 평점(제품/모집단 기준) 보존: overall_star_rating
    .withColumn("overall_star_rating", F.col("star_rating").cast("double"))
    .withColumn("review_help_count", F.col("review_help_count").cast("int"))
)

# 분석 편의를 위한 플래그 추가 (행 제거 없음)
cleaned_df = (transformed_df
    .withColumn("is_coupang_trial", F.when(F.col("review_text").like("쿠팡체험단%"), F.lit(1)).otherwise(F.lit(0)))
    .withColumn("is_empty_review", F.when(
        F.col("review_text").isNull() | (F.length(F.trim(F.col("review_text"))) == 0), F.lit(1)
    ).otherwise(F.lit(0)))
)

# 날짜 정규화(yyyy.MM.dd -> yyyy-MM-dd)
normalized_df = cleaned_df.withColumn("review_date_norm", F.regexp_replace(F.col("review_date"), "[.]", "-"))

# 데이터 품질 검증 (정규화된 날짜 기준)
quality_df = (normalized_df
    .withColumn("is_valid_rating", F.col("rating").between(0.0, 5.0))
    .withColumn("is_valid_date", F.to_timestamp(F.col("review_date_norm"), "yyyy-MM-dd").isNotNull())
    .withColumn("has_content", F.col("is_empty_review") == 0)
)

# 유효성 플래그 및 사유 배열 추가 (삭제하지 않고 마킹)
quality_marked_df = (quality_df
    .withColumn("is_valid", F.col("is_valid_rating") & F.col("is_valid_date") & F.col("has_content"))
    .withColumn(
        "invalid_reason",
        F.flatten(F.array(
            F.when(~F.col("is_valid_rating"), F.array(F.lit("invalid_rating"))).otherwise(F.array()),
            F.when(~F.col("is_valid_date"), F.array(F.lit("invalid_date"))).otherwise(F.array()),
            F.when(~F.col("has_content"), F.array(F.lit("empty_review"))).otherwise(F.array())
        ))
    )
)

# 시간 파생 컬럼 (Redshift 파티셔닝/집계 보조) - 전체 레코드 대상
time_enhanced_df = (quality_marked_df
    .withColumn("review_timestamp", F.to_timestamp(F.col("review_date_norm"), "yyyy-MM-dd"))
    .withColumn("year", F.year(F.col("review_timestamp")))
    .withColumn("month", F.month(F.col("review_timestamp")))
    .withColumn("day", F.dayofmonth(F.col("review_timestamp")))
    .withColumn("week_of_year", F.weekofyear(F.col("review_timestamp")))
    .withColumn("quarter", F.quarter(F.col("review_timestamp")))
    .withColumn("yyyymm", F.date_format(F.col("review_timestamp"), "yyyyMM"))
    .withColumn("weekday", F.date_format(F.col("review_timestamp"), "E"))
    .withColumn("yyyymmdd", F.date_format(F.col("review_timestamp"), "yyyyMMdd"))
)

# 텍스트 분석 (전처리 전 원문 기준) - 유효 데이터만 대상으로 수행
text_analysis_df = (time_enhanced_df
    .withColumn("text_length", F.length(F.col("review_text")))
    .withColumn("word_count", F.size(F.split(F.col("review_text"), " ")))
    .withColumn("sentence_count", F.size(F.split(F.col("review_text"), "[.!?]")))
    .withColumn("avg_word_length",
        F.when(F.col("word_count") > 0, F.length(F.col("review_text")) / F.col("word_count")).otherwise(F.lit(0.0))
    )
)

# 전처리 (클린 텍스트) - 쿠팡체험단은 유지, 품질 미달은 앞 단계에서 제거됨
MAX_TEXT_LEN = 5000
MAX_ARRAY_LEN = 500
PER_RECORD_MODE = os.getenv("PER_RECORD_MODE", "true").lower() == "true"

preprocessed_df = (text_analysis_df
    .withColumn("clean_text", F.regexp_replace(F.col("review_text"), "[^가-힣a-zA-Z0-9\\s]", ""))
    .withColumn("clean_text", F.trim(F.col("clean_text")))
    .withColumn("review_text", F.expr(f"substring(review_text, 1, {MAX_TEXT_LEN})"))
    .withColumn("clean_text", F.expr(f"substring(clean_text, 1, {MAX_TEXT_LEN})"))
    .withColumn("keywords", F.when(F.size(F.map_values(F.col("keywords"))) > MAX_ARRAY_LEN,
                                    F.map_from_arrays(
                                        F.slice(F.map_keys(F.col("keywords")), 1, MAX_ARRAY_LEN),
                                        F.slice(F.map_values(F.col("keywords")), 1, MAX_ARRAY_LEN)
                                    )
                                ).otherwise(F.col("keywords")))
)

# (옵션) 키워드 평탄화는 Kafka Streams에서 수행: 변환 토픽의 reviews[*].keywords를 사용해 flatMap

# 중복 제거 비활성화: 모든 데이터를 그대로 처리
# 마이크로 배치 JSON 배열 작성 (확장된 필드 포함)
batched_df = preprocessed_df.select(
    "job_id", "review_id", "product_id", "title", "category",
    "review_count", "sales_price", "final_price", "rating", "overall_star_rating", "review_date_norm",
    "review_text", "clean_text", "keywords", "review_help_count", "crawled_at",
    # 분석 제외 플래그
    "is_coupang_trial", "is_empty_review",
    # 품질 플래그
    "is_valid_rating", "is_valid_date", "has_content", "is_valid", "invalid_reason",
    # 시간 파생 (요청: 년,월,일,분기,yyyymm, yyyymmdd, 요일)
    "year", "month", "day", "quarter", "yyyymm", "yyyymmdd", "weekday"
)

# JSON 직렬화 시 과도한 크기의 레코드 방지를 위해 안전 가드 적용
# - 텍스트는 위에서 길이 제한
# - 배열/맵 크기는 위에서 슬라이스 처리
# 모드별 출력 형태 분기
if PER_RECORD_MODE:
    # 리뷰 1건 = 메시지 1건 (배치 JSON 구조 유지)
    output_df = (batched_df.select(
        F.col("review_id").alias("key"),
        F.to_json(F.struct(
            F.col("job_id"),
            F.col("review_id"),
            F.col("product_id"),
            F.col("title"),
            F.col("category"),
            F.col("review_count"),
            F.col("sales_price"),
            F.col("final_price"),
            F.col("rating"),
            F.col("overall_star_rating"),
            F.col("review_date_norm").alias("review_date"),
            F.col("review_text"),
            F.col("clean_text"),
            F.col("keywords"),
            F.col("review_help_count"),
            F.col("crawled_at"),
            F.col("is_coupang_trial"),
            F.col("is_empty_review"),
            F.col("is_valid_rating"),
            F.col("is_valid_date"),
            F.col("has_content"),
            F.col("is_valid"),
            F.col("invalid_reason"),
            F.col("year"),
            F.col("month"),
            F.col("day"),
            F.col("quarter"),
            F.col("yyyymm"),
            F.col("yyyymmdd"),
            F.col("weekday")
        )).alias("value")
    ).selectExpr("CAST(key AS STRING) AS key", "CAST(value AS STRING) AS value"))
else:
    batched_json_df = (batched_df
        .groupBy("job_id")
        .agg(F.slice(F.collect_list(
            F.struct(
                F.col("review_id"),
                F.col("product_id"),
                F.col("title"),
                F.col("category"),
                F.col("review_count"),
                F.col("sales_price"),
                F.col("final_price"),
                F.col("rating"),
                F.col("overall_star_rating"),
                F.col("review_date_norm").alias("review_date"),
                F.col("review_text"),
                F.col("clean_text"),
                F.col("keywords"),
                F.col("review_help_count"),
                F.col("crawled_at"),
                # 분석 제외 플래그 포함
                F.col("is_coupang_trial"),
                F.col("is_empty_review"),
                # 품질 플래그 포함
                F.col("is_valid_rating"),
                F.col("is_valid_date"),
                F.col("has_content"),
                F.col("is_valid"),
                F.col("invalid_reason"),
                # 시간 파생 포함
                F.col("year"),
                F.col("month"),
                F.col("day"),
                F.col("quarter"),
                F.col("yyyymm"),
                F.col("yyyymmdd"),
                F.col("weekday")
            )
        ), 1, MAX_ARRAY_LEN).alias("reviews"))
        .withColumn("value", F.to_json(F.struct("job_id", "reviews")))
        .withColumn("key", F.col("job_id"))
        .selectExpr("CAST(key AS STRING) AS key", "CAST(value AS STRING) AS value"))

    output_df = batched_json_df

# Kafka로 쓰기 (집계용 배치 JSON)
print(f"[DEBUG] Starting Kafka writeStream with trigger: 0.5 seconds")
query = (output_df.writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP)
        .option("topic", OUTPUT_TOPIC)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .outputMode("update")
        .trigger(processingTime='0.5 seconds')
        .option("kafka.max.request.size", "1048576")  # 1MB
        .option("kafka.batch.size", "16384")          # 16KB - 개별 레코드에 최적화
        .option("kafka.compression.type", "none")     # 압축 제거로 속도 향상
        .option("kafka.linger.ms", "10")              # 10ms로 단축
        .option("kafka.enable.auto.commit", "true")   # 자동 커밋으로 처리량 확인 가능
        .start())

print(f"[DEBUG] Streaming query started successfully!")
print(f"[DEBUG] Waiting for data from topic: {INPUT_TOPIC}")
print(f"[DEBUG] Processing mode: {'PER_RECORD_MODE' if PER_RECORD_MODE else 'BATCH_MODE'}")

spark.streams.awaitAnyTermination()

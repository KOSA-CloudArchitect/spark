# Spark 리뷰 스트리밍 처리 명세

## 목적
Kafka로 수집된 리뷰 단건 스트림을 Spark Structured Streaming으로 정규화/검증/전처리하여 `job_id` 단위로 묶어 JSON 배열로 재발행합니다. Redshift 적재 및 월/일/요일 단위 분석에 필요한 파생 컬럼을 포함합니다.

## 입력
- 토픽: `realtime-review-collection-topic`
- 레코드(예시):
```json
{
  "product_code": "4548468621",
  "title": "삼성 제트 무선청소기 200W",
  "tag": "가전,청소기,무선청소기",
  "star_rating": 4.3,
  "review_count": 1250,
  "sales_price": 159000,
  "final_price": 129000,
  "review_id": "1234567890",
  "review_rating": 5,
  "review_date": "2025-09-10",
  "review_content": "흡입력이 강력하고 가볍습니다.",
  "review_keywords": {"사용 편의성": "만족", "소음": "조용함"},
  "review_help_cnt": 3,
  "job_id": "a123",
  "crawled_at": "2025-09-10T23:30:00Z"
}
```

## 처리 단계(요약)
1) JSON 파싱 및 컬럼 정규화
- `product_code → product_id`, `review_rating → rating`, `review_content → review_text`, `review_keywords → keywords`, `review_help_cnt → review_help_count`

2) 분석 플래그 생성(행 제거 없음)
- `is_coupang_trial`: `review_text`가 "쿠팡체험단%" 패턴이면 1, 아니면 0
- `is_empty_review`: `review_text` null 또는 trim 후 길이 0이면 1, 아니면 0

3) 데이터 품질 마킹(정규화 직후, 삭제하지 않음)
- `is_valid_rating`: 0.0 ≤ `rating` ≤ 5.0
- `is_valid_date`: `review_date`가 `yyyy-MM-dd` 형식
- `has_content`: `is_empty_review == 0`
- `is_valid`: 위 3개 모두 만족하면 1, 아니면 0
- `invalid_reason`: ["invalid_rating","invalid_date","empty_review"] 중 해당 사유 배열

4) 시간 파생 컬럼 생성(전체 행 대상)
- `review_timestamp`(ts), `year`(int), `month`(int), `day`(int), `week_of_year`(int), `quarter`(int)
- `yyyymm`(string: yyyyMM), `yyyymmdd`(string: yyyyMMdd), `weekday`(string: 요일 약어)

5) 텍스트 특성 추출(원문 기준)
- `text_length`, `word_count`, `sentence_count`, `avg_word_length`

6) 전처리(클린 텍스트 생성)
- 특수문자 제거/trim → `clean_text`
- 쿠팡체험단/유효성 미달 행도 삭제하지 않음(플래그로 표시)

7) `job_id` 기준 마이크로배치 JSON 배열 생성
- `job_id`로 그룹핑하여 `reviews` 배열 생성
- Kafka 메시지 key: `job_id`, value: 아래 스키마 JSON

8) 출력
- 토픽: `realtime-review-transform-topic`
- 체크포인트: S3 경로 사용(운영 코드 참조)
- 완료 판정/알림: Spark는 수행하지 않음. Kafka Streams가 `expected_count`와 transform 토픽의 distinct(review_id) 집계를 사용해 완료 이벤트 발행

## 출력 스키마(정의)
최종 Kafka value(JSON) 구조(리뷰 묶음):
```json
{
  "job_id": "a123",
  "reviews": [
    {
      "review_id": "1234567890",
      "product_id": "4548468621",
      "title": "삼성 제트 무선청소기 200W",
      "tag": "가전,청소기,무선청소기",
      "review_count": 1250,
      "sales_price": 159000,
      "final_price": 129000,
      "rating": 5.0,
      "review_date": "2025-09-10",
      "review_text": "흡입력이 강력하고 가볍습니다.",
      "clean_text": "흡입력이 강력하고 가볍습니다",
      "keywords": {"사용 편의성": "만족", "소음": "조용함"},
      "review_help_count": 3,
      "crawled_at": "2025-09-10T23:30:00Z",

      "is_coupang_trial": 0,
      "is_empty_review": 0,

      "is_valid_rating": 1,
      "is_valid_date": 1,
      "has_content": 1,
      "is_valid": 1,
      "invalid_reason": [],

      "year": 2025,
      "month": 9,
      "day": 10,
      "quarter": 3,
      "yyyymm": "202509",
      "yyyymmdd": "20250910",
      "weekday": "Wed"
    }
  ]
}
```

## 설계 노트
- 유효성 미달 레코드도 삭제하지 않고 플래그/사유로 표시해 downstream에서 제외/포함을 선택 가능하게 함
- Kafka Streams는 별도 control 토픽(`expected_count`)과 transform 토픽을 조합해 완료 판정, 집계는 `is_valid==1`만 사용
- 시간 파생 컬럼을 포함해 Redshift에서 월별/일별/요일별 집계가 쉬움
- Kafka 메시지 key를 `job_id`로 설정해 Kafka Streams 조인/집계 안정성 확보
- 여러 writeStream을 사용할 수 있어 코드에선 `spark.streams.awaitAnyTermination()`로 전체 쿼리 대기

## 다운스트림 연계
- LLM 분석 워커는 `reviews[*].clean_text`를 요약/감정분석 입력으로 사용(필요 시 `review_text`도 참조)
- Kafka Streams 집계 앱은 `review-rows`/`review-agg-by-job` 및 키워드 통계를 생성하며, 완료 이벤트는 `expected_count` 기준으로 발행

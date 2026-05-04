# Architecture Decision — Vietnamese Ride-Hailing CDC → Lakehouse (Decree 13 Compliant)

**Topic C · Author:** Track-2 Student  
**Date:** 2026-05-04

---

## 1. Problem Statement

Một công ty ride-hailing Việt Nam (quy mô tương đương Grab VN) đang chạy production
trên Oracle Database. Yêu cầu: xây dựng lakehouse analytics real-time, tuân thủ
**Nghị định 13/2023/NĐ-CP** về bảo vệ dữ liệu cá nhân.

**Scale:**
- 100 triệu chuyến/năm → ~274K chuyến/ngày, ~3 chuyến/giây trung bình
- Peak (giờ tan tầm HCMC + HN): **30.000 CDC events/giây** (gộp GPS pings + trip + payment)
- GPS ping mỗi 5 giây/xe → volume driver: ~10K events/giây sustained → **~86 GB/ngày raw**

**PII trong scope Decree 13:** số điện thoại, CCCD, tọa độ GPS liên tục, lịch sử chuyến đi.

**SLA bắt buộc:**
- Dashboard analyst refresh trong **60 giây** kể từ source commit
- Ad-hoc query p95 **< 1 giây**
- Late-arriving events thường xuyên (mất mạng vùng sâu, Tây Nguyên, ĐBSCL)

**Tại sao khó:** ba ràng buộc xung đột nhau — sub-60s freshness + PII phải mask trước
khi bất kỳ ai đọc + late events vẫn phải eventually consistent, không được drop.

---

## 2. Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────┐
│  SOURCE                                                             │
│  Oracle 19c (Production)                                            │
│  Tables: trips, drivers, riders, payments, gps_pings               │
└──────────────────────┬──────────────────────────────────────────────┘
                       │ Redo Log (LogMiner)
                       ▼
┌─────────────────────────────────────────────────────────────────────┐
│  INGESTION LAYER                                                    │
│  Debezium Connector → Kafka (3 brokers, replication=3)             │
│  Topic: vn.ridehailing.{table_name}  (1 topic per table)           │
│                                                                     │
│  ┌─────────────────────────────────┐                               │
│  │ PII Tokenization Service        │  ← HashiCorp Vault Transit    │
│  │ Runs BEFORE write to Delta      │    (deterministic encryption) │
│  │ phone → tok_phone_XXXX          │                               │
│  │ cccd  → tok_cccd_XXXX           │                               │
│  │ gps   → grid_cell_500m (k-anon) │                               │
│  └─────────────────────────────────┘                               │
└──────────────────────┬──────────────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────────────┐
│  BRONZE LAYER  (Delta Lake, append-only, immutable)                │
│  s3://lakehouse/bronze/{table}/                                     │
│  Partition: event_date / event_hour                                 │
│                                                                     │
│  • Raw CDC events (op: I/U/D, before/after images)                 │
│  • PII đã tokenized — KHÔNG có raw PII trên disk                   │
│  • Schema enforcement: reject malformed events                      │
│  • Retention: 1 năm (S3 Standard → IA sau 30 ngày)                │
│  • Delta CDF enabled → feed Silver incrementally                   │
└──────────────────────┬──────────────────────────────────────────────┘
                       │ Delta Change Data Feed
                       ▼
┌─────────────────────────────────────────────────────────────────────┐
│  SILVER LAYER  (Delta Lake, curated, SCD Type 2)                   │
│  s3://lakehouse/silver/{domain}/                                    │
│  Partition: event_date / city_id                                    │
│                                                                     │
│  silver.trips        — dedup + MERGE upsert, SCD2 for status       │
│  silver.drivers      — SCD Type 2 (track license changes)          │
│  silver.riders       — dedup, chỉ token PII                        │
│                                                                     │
│  Late-data handler:                                                 │
│  MERGE WHEN MATCHED AND src.updated_at > tgt.updated_at THEN UPDATE│
│                                                                     │
│  • OPTIMIZE + Z-ORDER by (city_id, trip_date) mỗi giờ             │
│  • Retention: 2 năm                                                │
└──────────────────────┬──────────────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────────────┐
│  GOLD LAYER  (Delta Lake, aggregated, dashboard-ready)              │
│  s3://lakehouse/gold/{metric}/                                      │
│                                                                     │
│  gold.trips_hourly   — count, revenue, avg_duration per city/hour  │
│  gold.driver_perf    — rating, completion_rate, earnings per driver │
│  gold.demand_heatmap — trip density per grid_cell per hour         │
│                                                                     │
│  • Refresh mỗi 5 phút (micro-batch Spark)                          │
│  • Z-ORDER by city_id cho dashboard filter hot path                │
│  • Retention: 3 năm                                                │
└──────────────────────┬──────────────────────────────────────────────┘
                       │
              ┌────────┴────────┐
              ▼                 ▼
       BI Dashboard        Ad-hoc Query
       (Metabase/          (DuckDB / Trino)
        Superset)          p95 < 1s

┌─────────────────────────────────────────────────────────────────────┐
│  COMPLIANCE LAYER  (tách biệt, Decree 13)                          │
│  delta.pii_access_audit — mọi query đọc token PII đều được log    │
│  delta.consent_registry — trạng thái đồng ý của từng rider/driver │
│  Unity Catalog: column-level mask trên token columns              │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 3. Quyết Định Chính & Alternatives Đã Loại

### D1 — Table Format: Delta Lake

**Chọn: Delta Lake**

| Alternative | Lý do loại |
|---|---|
| Apache Iceberg | CDF (Change Data Feed) kém mature hơn Delta — CDC incremental feed Bronze→Silver khó hơn. Hệ sinh thái VN ít Iceberg expertise. |
| Apache Hudi | Thiết kế tốt cho CDC nhưng DuckDB/Polars không đọc native Hudi; ad-hoc query SLA < 1s khó đạt nếu phải convert format. |
| Raw Parquet | Không có ACID, không time travel, không schema enforcement — không thể RESTORE khi có incident PII. |

**Lý do chọn Delta:** CDF native, RESTORE cho incident response, MERGE syntax clean cho
SCD2, DuckDB đọc trực tiếp cho ad-hoc query.

---

### D2 — PII Tokenization: Tại Bronze (Landing)

**Chọn: Tokenize tại Bronze, trước khi write xuống disk**

| Alternative | Lý do loại |
|---|---|
| Tokenize tại Silver | Raw PII nằm trên Bronze disk trong khoảng Bronze→Silver lag (~5–10 phút). Vi phạm Decree 13 Điều 9: "minimize exposure time". |
| Mask tại application layer (Oracle) | Yêu cầu schema change production Oracle — rủi ro cao. CDC backlog không được mask. |
| Encryption at rest (S3 SSE) | Mã hóa disk nhưng không giấu giá trị khi query — analyst vẫn thấy raw PII trong SELECT. |

**Lý do chọn:** Vault Transit cho phép deterministic tokenization — cùng số điện thoại
luôn ra cùng token, JOIN vẫn hoạt động qua các layer mà không cần giải mã.

---

### D3 — Ingestion: Debezium + Kafka

**Chọn: Debezium (Oracle LogMiner connector) → Kafka**

| Alternative | Lý do loại |
|---|---|
| Oracle GoldenGate | License Oracle GoldenGate ~$50K+/năm. Vendor lock-in. Quá đắt so với Debezium open-source. |
| Batch dump (sqoop / full export) | Full table scan mỗi 5 phút làm nặng production Oracle. SLA 60s không đạt được với batch. |
| Oracle Streams (AQ) | Deprecated từ Oracle 19c. Không có connector ecosystem. |

**Lý do chọn:** Debezium đọc Oracle redo log (LogMiner), không query tables — zero
production impact. Kafka buffer absorb peak 30K events/sec.

---

### D4 — Late-Data Handling: MERGE với Timestamp Guard

**Chọn: `MERGE WHEN MATCHED AND src.updated_at > tgt.updated_at`**

| Alternative | Lý do loại |
|---|---|
| Spark Structured Streaming watermark | Watermark **drop** events đến sau threshold — Decree 13 yêu cầu mọi chuyến đi cuối cùng phải được ghi nhận. Không thể drop. |
| Full Gold recompute khi có late event | 86GB/ngày Bronze → recompute Gold mỗi lần có late event = quá tốn compute, SLA 60s không đạt. |
| Ignore late events | Vi phạm data completeness — driver bị thiệt về earnings nếu trip không được tính. |

**Lý do chọn:** MERGE timestamp guard cho phép late event cập nhật Silver nếu
`src.updated_at` mới hơn — eventually consistent, không drop, không recompute toàn bộ.

---

### D5 — Partitioning Strategy

**Chọn: Bronze → `event_date/event_hour`, Silver → `event_date/city_id`**

| Alternative | Lý do loại |
|---|---|
| Partition by `city_id` only | Skewed: HCMC chiếm ~60% trips → một partition khổng lồ. Time-range query (7 ngày gần nhất) scan toàn bộ. |
| Không partition | Full table scan mọi query. Gold dashboard p95 >> 1s ở scale 31TB/năm. |
| Partition by `driver_id` | Cardinality quá cao (hàng triệu drivers) → hàng triệu tiny files. Small-file problem ngay từ đầu. |

**Lý do chọn:** `event_date` cho time-range queries, `city_id` cho geographic filter.
Z-ORDER by `(city_id, driver_id)` cho Silver để prune theo min/max stats.

---

### D6 — Catalog & Governance: Unity Catalog

**Chọn: Databricks Unity Catalog (hoặc Apache Polaris nếu vendor-neutral)**

| Alternative | Lý do loại |
|---|---|
| Hive Metastore | Không có column-level security — không thể mask token columns cho analyst. Không audit trail cho PII access. |
| Apache Polaris | Đang mature nhanh nhưng chưa production-proven tại VN enterprise. Ít support local. |
| Không có catalog | 20+ analyst teams, không governance → data swamp trong 6 tháng. |

**Lý do chọn:** Unity Catalog có column-level masking native — `tok_phone` column chỉ
Compliance team thấy raw token, analyst thấy `***`. Row-level security theo
`city_id` cho phép mỗi regional team chỉ đọc data của mình.

---

## 4. Failure Modes (3 AM Scenarios)

### F1 — Debezium Lag Spike (Oracle redo log tắc)

**Kịch bản:** DBA chạy bulk UPDATE 10 triệu rows → Oracle redo log overflow → Debezium
lag tăng từ 0 lên 2 triệu messages trong 5 phút → dashboard stale > 10 phút.

**Detection:**
- Alert: Kafka consumer group lag `silver-merger` > 100.000 messages → PagerDuty
- Alert: Dashboard timestamp `max(trip_start)` cũ hơn now() 5 phút

**Rollback / Fix:**
- Tăng Kafka retention từ 24h lên 72h (đã cấu hình sẵn) → không mất events
- Scale out Silver MERGE job (thêm Spark executors) để catch up
- Nếu Silver bị corrupt trong lúc catch up: `dt.restore(version_as_of=last_clean_version)` → replay từ Kafka offset tương ứng

**Tie với Day 18:** Delta time travel + CDF offset cho phép replay chính xác từ điểm trước incident.

---

### F2 — PII Tokenization Service Down → Raw PII Lands in Bronze

**Kịch bản:** Vault Transit service restart lúc 2:47 AM → 3 phút tokenizer trả về
plaintext → ~5.400 events với raw phone number ghi xuống Bronze.

**Detection:**
- Schema check job chạy mỗi phút: `SELECT COUNT(*) FROM bronze WHERE phone_raw RLIKE '^(0|84)[0-9]{8,9}$'`
- Nếu count > 0 → alert + **tự động pause Kafka consumer** (circuit breaker)

**Rollback:**
- `dt.restore(version_as_of=version_before_incident)` trên Bronze table — Delta ACID đảm bảo atomic rollback < 30 giây
- Tokenizer fix → re-process 3 phút events từ Kafka (offset còn lưu)
- Ghi incident vào `pii_access_audit` theo yêu cầu Decree 13 Điều 23 (thông báo vi phạm)

---

### F3 — SCD Type 2 MERGE Degradation (Silver table phình to)

**Kịch bản:** Sau 18 tháng, `silver.drivers` có 500 triệu rows SCD2 history.
MERGE job tăng từ 45 giây lên 8 phút → SLA 60s vi phạm liên tục.

**Detection:**
- Monitor MERGE execution time mỗi run → alert nếu > 90 giây
- `dt.history()` cho thấy operation duration tăng đều qua các version

**Rollback / Fix:**
- Short-term: Switch Silver MERGE sang APPEND mode + dedup view (không downtime)
- Fix: `OPTIMIZE silver.drivers ZORDER BY (driver_id, effective_date)` — compact + clustering
- Long-term: Partition `silver.drivers` thêm `effective_year` để prune SCD2 history cũ
- Xác nhận: sau OPTIMIZE, MERGE về < 60 giây, đo lại với `dt.history()` operation time

---

## 5. Ước Tính Chi Phí (Back-of-Envelope)

### Volume tính toán

```
GPS ping: 50.000 xe active × ping mỗi 5 giây = 10.000 events/giây
Avg event size (sau tokenize + compress): 200 bytes
→ 10.000 × 200B × 86.400 giây = ~172 GB/ngày raw Bronze
→ Parquet compress 5x → ~34 GB/ngày thực tế trên disk
→ 1 năm Bronze: 34 GB × 365 = ~12.4 TB
```

### Storage (AWS S3, ap-southeast-1)

| Layer | Size | Tier | $/TB-month | $/month |
|---|---|---|---|---|
| Bronze (1 năm) | 12.4 TB | S3 Standard (30 ngày) → IA | $18 avg | **$223** |
| Silver (2 năm, dedup ~40%) | 7.4 TB × 2 = 14.8 TB | S3 Standard | $23 | **$340** |
| Gold (aggregated, ~2% Bronze) | 0.25 TB × 3 năm | S3 Standard | $23 | **$17** |
| **Storage total** | | | | **~$580/tháng** |

### Compute

| Component | Spec | $/month |
|---|---|---|
| Kafka (3 brokers r5.large) | 24/7, replication=3 | $270 |
| Debezium + Tokenizer (2× t3.medium) | 24/7 | $60 |
| Spark Silver MERGE (spot r5.4xlarge) | 4 giờ/ngày catch-up | $95 |
| Spark Gold refresh (spot m5.2xlarge) | 5 phút/lần × 288 lần/ngày | $40 |
| DuckDB ad-hoc (serverless Athena) | ~50 queries/ngày × $5/TB | $30 |
| Unity Catalog + monitoring | managed | $50 |
| **Compute total** | | **~$545/tháng** |

### Tổng cộng

```
Storage:  $580/tháng
Compute:  $545/tháng
─────────────────────
TOTAL:  ~$1.125/tháng (~$13.500/năm)
```

So sánh: GoldenGate license alone ~$50K/năm → tiết kiệm ~$36K/năm so với Oracle stack.

---

## 6. MVP Một Tuần — Smallest Shippable Slice

**Mục tiêu:** Chứng minh CDC → Bronze (với PII mask) → Silver (MERGE) hoạt động
end-to-end. Không cần Gold, không cần dashboard.

**Ngày 1–2: Ingestion pipeline**
- Setup Debezium Oracle connector, connect tới Kafka local (Docker)
- Kafka topic `vn.ridehailing.trips` nhận CDC events từ Oracle test instance
- Viết Spark Structured Streaming job đọc Kafka → Bronze Delta table

**Ngày 3: PII Tokenization**
- Tích hợp Vault Transit (dev mode) vào Streaming job
- Kiểm tra: Bronze table không chứa raw phone/CCCD
- Schema enforcement: bad events bị reject, log vào dead-letter topic

**Ngày 4: Silver MERGE với late-data**
- Spark job đọc Bronze CDF → MERGE vào `silver.trips`
- Test case: inject late event (updated_at = 3 ngày trước) → kiểm tra Silver cập nhật đúng
- Đo MERGE latency → phải < 60s với 100K rows test

**Ngày 5: Kiểm tra & đo lường**
- End-to-end test: insert trip vào Oracle → kiểm tra Silver trong < 60 giây
- Smoke test: DuckDB query Silver → p95 < 1s
- Viết ADR (Architecture Decision Record) cho D1–D6 ở trên

**Definition of Done cho MVP:**
- [ ] Bronze không có raw PII (automated check pass)
- [ ] Late event (3 ngày trước) được Silver MERGE đúng, không drop
- [ ] End-to-end latency Oracle → Silver < 60 giây (đo 10 lần, p95)
- [ ] DuckDB ad-hoc `SELECT city_id, COUNT(*) FROM silver.trips GROUP BY 1` < 1s

Sau MVP tuần 1: Gold aggregation, Unity Catalog column masking, compliance audit table.

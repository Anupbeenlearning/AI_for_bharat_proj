# Design Document: DBT Leakage & Retention Monitoring Engine

## 1. Overview

### 1.1 System Purpose

The DBT Leakage & Retention Monitoring Engine is a production-grade AI infrastructure layer designed to transform unstructured beneficiary communication data into actionable retention analytics for India's Direct Benefit Transfer ecosystem. The system addresses a critical governance gap by enabling granular monitoring of beneficiary-level retention behavior, withdrawal patterns, and installment continuity across 500+ million beneficiaries and 300+ DBT schemes.

### 1.2 Design Principles

1. **Privacy-First Architecture**: Beneficiary data is hashed, anonymized, and processed with explicit consent
2. **Offline-First Operation**: Core functionality operates without continuous connectivity
3. **Horizontal Scalability**: Stateless microservices enable linear scaling to national deployment
4. **Fail-Safe Processing**: Message queuing and idempotent operations prevent data loss
5. **Explainable AI**: Model decisions are traceable and auditable for governance requirements
6. **Incremental Deployment**: Modular design supports phased rollout (pilot → state → national)

### 1.3 Technology Stack

**Core Processing**:
- Language: Python 3.11+ (ML/NLP workloads) and Go 1.21+ (high-throughput services)
- ML Framework: PyTorch 2.0+ with ONNX Runtime for inference
- NLP: Hugging Face Transformers, spaCy 3.7+

**Data Layer**:
- Primary Database: PostgreSQL 15+ with TimescaleDB extension (time-series optimization)
- Event Store: Apache Kafka 3.6+ (durable message queue)
- Cache: Redis 7.2+ (aggregation results, model predictions)
- Object Storage: MinIO or S3-compatible (audio files, model artifacts)

**ML/AI Services**:
- SMS Classification: IndicBERT fine-tuned on DBT corpus
- Entity Extraction: Hybrid regex + spaCy NER with custom DBT entity types
- ASR: Bhashini API or Whisper multilingual model
- Anomaly Detection: Statistical process control + isolation forest

**Infrastructure**:
- Container Orchestration: Kubernetes 1.28+
- Service Mesh: Istio 1.20+ (mTLS, observability)
- Monitoring: Prometheus + Grafana, OpenTelemetry
- API Gateway: Kong or Traefik


## 2. System Architecture

### 2.1 Layered Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                     Presentation Layer                          │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐         │
│  │   Web UI     │  │  Mobile UI   │  │  REST API    │         │
│  │  Dashboard   │  │  (Future)    │  │  Gateway     │         │
│  └──────────────┘  └──────────────┘  └──────────────┘         │
└─────────────────────────────────────────────────────────────────┘
                              │
┌─────────────────────────────────────────────────────────────────┐
│                    Application Layer                            │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐         │
│  │ Aggregation  │  │   Anomaly    │  │   Alert      │         │
│  │   Service    │  │   Detector   │  │   Manager    │         │
│  └──────────────┘  └──────────────┘  └──────────────┘         │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐         │
│  │  Consent     │  │   Access     │  │   Report     │         │
│  │  Manager     │  │   Control    │  │  Generator   │         │
│  └──────────────┘  └──────────────┘  └──────────────┘         │
└─────────────────────────────────────────────────────────────────┘
                              │
┌─────────────────────────────────────────────────────────────────┐
│                    Processing Layer                             │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐         │
│  │     SMS      │  │  Withdrawal  │  │ Installment  │         │
│  │  Processor   │  │  Correlator  │  │   Monitor    │         │
│  └──────────────┘  └──────────────┘  └──────────────┘         │
│  ┌──────────────┐  ┌──────────────┐                           │
│  │    Voice     │  │  Grievance   │                           │
│  │  Processor   │  │    Linker    │                           │
│  └──────────────┘  └──────────────┘                           │
└─────────────────────────────────────────────────────────────────┘
                              │
┌─────────────────────────────────────────────────────────────────┐
│                      AI/ML Layer                                │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐         │
│  │     SMS      │  │    Entity    │  │     ASR      │         │
│  │ Classifier   │  │  Extractor   │  │    Engine    │         │
│  └──────────────┘  └──────────────┘  └──────────────┘         │
│  ┌──────────────┐  ┌──────────────┐                           │
│  │   Intent     │  │   Anomaly    │                           │
│  │ Classifier   │  │    Model     │                           │
│  └──────────────┘  └──────────────┘                           │
└─────────────────────────────────────────────────────────────────┘
                              │
┌─────────────────────────────────────────────────────────────────┐
│                       Data Layer                                │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐         │
│  │  Event Store │  │  Aggregation │  │    Consent   │         │
│  │ (PostgreSQL) │  │     Cache    │  │   Database   │         │
│  └──────────────┘  └──────────────┘  └──────────────┘         │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐         │
│  │ Message Queue│  │    Object    │  │  Audit Log   │         │
│  │   (Kafka)    │  │   Storage    │  │   Storage    │         │
│  └──────────────┘  └──────────────┘  └──────────────┘         │
└─────────────────────────────────────────────────────────────────┘
```

### 2.2 Deployment Architecture Options

#### Option A: Hybrid Cloud-Edge (Recommended for Pilot)

```
┌─────────────────────────────────────────────────────────────────┐
│                      Central Cloud (State Level)                │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │  Aggregation Services │ ML Model Training │ Dashboards   │  │
│  │  State-level Analytics │ Model Registry │ API Gateway    │  │
│  └──────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
                              ▲
                              │ Sync (4G/WiFi)
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                    District Edge Nodes (3-5 per state)          │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │  SMS Processing │ Local Inference │ Event Storage        │  │
│  │  Offline Queue │ District Aggregation │ Local Cache      │  │
│  └──────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
                              ▲
                              │ Sync (2G/3G)
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│              Block/Village Edge Devices (Optional)              │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │  SMS Collection │ Offline Storage │ Basic Analytics      │  │
│  └──────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
```

**Rationale**: Balances processing power (district nodes) with offline capability (edge devices) and centralized analytics (cloud).

#### Option B: Fully Cloud-Native (Future Scale)

- All processing in centralized cloud (AWS/Azure/NIC Cloud)
- Edge devices only for data collection
- Requires reliable 4G/5G connectivity
- Lower operational complexity, higher bandwidth requirements

#### Option C: Fully On-Premise (High Security Scenarios)

- All infrastructure within government data centers
- No cloud dependencies
- Higher operational overhead, full data sovereignty
- Suitable for sensitive pilot phases


## 3. Data Flow

### 3.1 End-to-End SMS Processing Flow

```
┌──────────┐
│ Bank SMS │ (Credit: "Rs 2000 credited to A/c XX1234 under PM-KISAN")
└────┬─────┘
     │
     ▼
┌─────────────────┐
│ SMS Ingestion   │ → Store raw SMS with metadata (timestamp, sender, phone)
│ Module          │ → Deduplicate based on message hash
└────┬────────────┘
     │
     ▼
┌─────────────────┐
│ Normalization   │ → Unicode conversion (regional scripts)
│ Engine          │ → Remove special characters, standardize spacing
└────┬────────────┘
     │
     ▼
┌─────────────────┐
│ SMS Classifier  │ → ML model predicts scheme (PM-KISAN, MGNREGA, etc.)
│ (IndicBERT)     │ → Output: scheme_id, confidence_score
└────┬────────────┘
     │
     ├─ confidence < 0.7 ──→ Manual Review Queue
     │
     ▼
┌─────────────────┐
│ Entity          │ → Extract: amount, date, reference_number, account_digits
│ Extractor       │ → Hybrid: Regex patterns + NER model
└────┬────────────┘
     │
     ▼
┌─────────────────┐
│ Event Store     │ → Create Credit_Event record
│ (PostgreSQL)    │ → Link to beneficiary_hash (SHA-256 of phone)
└────┬────────────┘
     │
     ▼
┌─────────────────┐
│ Withdrawal      │ → Match debit SMS to recent credits
│ Correlator      │ → Calculate retention_window (hours/days)
└────┬────────────┘
     │
     ▼
┌─────────────────┐
│ Anomaly         │ → Check: retention < 2h, full withdrawal, pattern
│ Detector        │ → Generate alert if anomaly detected
└────┬────────────┘
     │
     ▼
┌─────────────────┐
│ Aggregation     │ → Daily batch: compute village/block/district metrics
│ Pipeline        │ → Store in cache for dashboard queries
└────┬────────────┘
     │
     ▼
┌─────────────────┐
│ Dashboard       │ → Display retention metrics, anomaly alerts
│ (Web UI)        │ → Role-based access (village/district/state)
└─────────────────┘
```

### 3.2 Voice Grievance Processing Flow

```
┌──────────────┐
│ Beneficiary  │ (Voice recording in regional language)
│ Voice Input  │
└──────┬───────┘
       │
       ▼
┌─────────────────┐
│ ASR Engine      │ → Transcribe audio to text
│ (Bhashini/      │ → Detect language (Hindi, Tamil, etc.)
│  Whisper)       │ → Output: transcript, language_code, confidence
└────┬────────────┘
     │
     ▼
┌─────────────────┐
│ Intent          │ → Classify: payment_delay, wrong_amount, coercion,
│ Classifier      │   account_issue, other
└────┬────────────┘
     │
     ▼
┌─────────────────┐
│ Grievance       │ → Extract: scheme_name, amount, date, location
│ Entity          │ → Use NER model trained on grievance corpus
│ Extractor       │
└────┬────────────┘
     │
     ▼
┌─────────────────┐
│ Grievance       │ → Match to Credit_Event in Event Store
│ Linker          │ → Link by: beneficiary_hash, scheme, date proximity
└────┬────────────┘
     │
     ▼
┌─────────────────┐
│ Grievance       │ → Store linked grievance record
│ Database        │ → Trigger alert to appropriate admin level
└─────────────────┘
```

### 3.3 Aggregation and Rollup Flow

```
┌─────────────────┐
│ Event Store     │ (Beneficiary-level credit/withdrawal events)
└────┬────────────┘
     │
     ▼ (Daily batch job at 2 AM)
┌─────────────────┐
│ Village         │ → Group by village_id
│ Aggregator      │ → Compute: total_credits, median_retention,
│                 │   anomaly_count, scheme_distribution
└────┬────────────┘
     │
     ▼
┌─────────────────┐
│ Block           │ → Group by block_id
│ Aggregator      │ → Rollup village metrics: sum, mean, percentiles
│                 │ → Compute inter-village variance
└────┬────────────┘
     │
     ▼
┌─────────────────┐
│ District        │ → Group by district_id
│ Aggregator      │ → Rollup block metrics
│                 │ → Flag high-variance blocks
└────┬────────────┘
     │
     ▼
┌─────────────────┐
│ State           │ → Rollup district metrics
│ Aggregator      │ → Compute state-wide trends
│                 │ → Inter-district comparisons
└────┬────────────┘
     │
     ▼
┌─────────────────┐
│ Aggregation     │ → Store results in Redis cache
│ Cache (Redis)   │ → TTL: 24 hours (refreshed daily)
└────┬────────────┘
     │
     ▼
┌─────────────────┐
│ Dashboard       │ → Query cache for fast response
│ Service         │ → Fallback to database if cache miss
└─────────────────┘
```


## 4. Components and Interfaces

### 4.1 SMS Ingestion Module

**Responsibility**: Receive, deduplicate, and queue SMS messages for processing.

**Interface**:
```python
class SMSIngestionModule:
    def ingest_sms(
        self,
        message_text: str,
        sender_id: str,
        recipient_phone: str,
        timestamp: datetime
    ) -> SMSRecord:
        """
        Ingest raw SMS and create record.
        
        Returns:
            SMSRecord with unique ID and metadata
        
        Raises:
            DuplicateSMSError if message hash already exists
        """
        pass
    
    def get_processing_queue(self, batch_size: int = 100) -> List[SMSRecord]:
        """Retrieve batch of unprocessed SMS for processing."""
        pass
```

**Implementation Details**:
- Message hash: SHA-256(sender_id + recipient_phone + message_text + timestamp_rounded_to_minute)
- Storage: Kafka topic `sms.raw` with 30-day retention
- Deduplication window: 24 hours (handles delayed delivery duplicates)

### 4.2 SMS Normalization Engine

**Responsibility**: Convert regional scripts to Unicode, standardize formatting.

**Interface**:
```python
class SMSNormalizationEngine:
    def normalize(self, raw_text: str) -> NormalizedSMS:
        """
        Normalize SMS text for downstream processing.
        
        Steps:
        1. Detect encoding and convert to UTF-8
        2. Normalize Unicode (NFC form)
        3. Remove excessive whitespace
        4. Preserve numerals and currency symbols
        
        Returns:
            NormalizedSMS with cleaned text and detected language
        """
        pass
```

**Implementation Details**:
- Use `ftfy` library for encoding fixes
- Preserve mixed-script content (e.g., Hindi + English numerals)
- Language detection: `langdetect` library with confidence threshold 0.8

### 4.3 SMS Classification Model

**Responsibility**: Predict DBT scheme from SMS text using fine-tuned transformer model.

**Model Architecture**:
- Base Model: IndicBERT (ai4bharat/indic-bert) or MuRIL (google/muril-base-cased)
- Fine-tuning: Classification head with 50+ scheme classes + "UNKNOWN"
- Input: Normalized SMS text (max 256 tokens)
- Output: Scheme ID, confidence score

**Training Data Requirements**:
- Minimum 500 examples per major scheme (PM-KISAN, MGNREGA, pensions)
- Minimum 100 examples per minor scheme
- Balanced across 10 supported languages
- Synthetic augmentation for low-resource schemes

**Interface**:
```python
class SMSClassifier:
    def predict_scheme(
        self,
        normalized_text: str,
        language_code: str
    ) -> SchemeClassification:
        """
        Predict DBT scheme from SMS text.
        
        Returns:
            SchemeClassification(
                scheme_id: str,
                confidence: float,
                top_k_predictions: List[Tuple[str, float]]
            )
        """
        pass
    
    def batch_predict(
        self,
        texts: List[str],
        language_codes: List[str]
    ) -> List[SchemeClassification]:
        """Batch inference for throughput optimization."""
        pass
```

**Performance Targets**:
- Inference latency: <100ms per SMS (p95)
- Throughput: 1000 SMS/second on single GPU (T4)
- Accuracy: >85% across all languages (weighted F1)

**Model Deployment**:
- Format: ONNX for cross-platform compatibility
- Quantization: INT8 for edge deployment (4x smaller, 2-3x faster)
- Serving: Triton Inference Server or TorchServe
- Versioning: MLflow model registry with A/B testing support

### 4.4 Entity Extraction Engine

**Responsibility**: Extract structured fields (amount, date, reference, account) from SMS.

**Hybrid Approach**:

1. **Regex Patterns** (Fast path for common formats):
   - Amount: `Rs\.?\s*(\d{1,3}(?:,\d{3})*(?:\.\d{2})?)` or `₹\s*(\d+)`
   - Date: `(\d{1,2}[-/]\d{1,2}[-/]\d{2,4})` or `(\d{2}-[A-Z]{3}-\d{4})`
   - Reference: `Ref(?:erence)?[:\s]+([A-Z0-9]{10,20})`
   - Account: `A/?c[:\s]+(?:XX)?(\d{4})`

2. **NER Model** (Fallback for complex cases):
   - Base: spaCy 3.7 with custom DBT entity types
   - Entities: SCHEME_NAME, AMOUNT, DATE, REFERENCE, ACCOUNT
   - Training: 10K+ annotated SMS across languages

**Interface**:
```python
class EntityExtractor:
    def extract(
        self,
        normalized_text: str,
        scheme_id: str
    ) -> ExtractedEntities:
        """
        Extract structured fields from SMS.
        
        Returns:
            ExtractedEntities(
                amount: Optional[Decimal],
                date: Optional[datetime],
                reference: Optional[str],
                account_last4: Optional[str],
                confidence_scores: Dict[str, float]
            )
        """
        pass
```

**Confidence Scoring**:
- Regex match: confidence = 0.95
- NER extraction: confidence = model probability
- Multiple extractions: use highest confidence, flag conflict if >1 with confidence >0.8

### 4.5 Event Store

**Responsibility**: Persist beneficiary-level credit and withdrawal events with immutability.

**Schema**:
```sql
CREATE TABLE credit_events (
    event_id UUID PRIMARY KEY,
    beneficiary_hash VARCHAR(64) NOT NULL,  -- SHA-256 of phone
    scheme_id VARCHAR(50) NOT NULL,
    amount DECIMAL(12, 2) NOT NULL,
    transaction_date DATE NOT NULL,
    transaction_timestamp TIMESTAMP,
    reference_number VARCHAR(50),
    account_last4 VARCHAR(4),
    sms_id UUID REFERENCES sms_records(id),
    extraction_confidence JSONB,  -- {amount: 0.95, date: 0.98, ...}
    created_at TIMESTAMP DEFAULT NOW(),
    INDEX idx_beneficiary_date (beneficiary_hash, transaction_date DESC),
    INDEX idx_scheme_date (scheme_id, transaction_date DESC)
);

CREATE TABLE withdrawal_events (
    event_id UUID PRIMARY KEY,
    beneficiary_hash VARCHAR(64) NOT NULL,
    amount DECIMAL(12, 2) NOT NULL,
    transaction_date DATE NOT NULL,
    transaction_timestamp TIMESTAMP,
    matched_credit_id UUID REFERENCES credit_events(event_id),
    retention_window_hours INT,  -- NULL if unmatched
    sms_id UUID REFERENCES sms_records(id),
    created_at TIMESTAMP DEFAULT NOW(),
    INDEX idx_beneficiary_date (beneficiary_hash, transaction_date DESC)
);

CREATE TABLE beneficiary_metadata (
    beneficiary_hash VARCHAR(64) PRIMARY KEY,
    village_id VARCHAR(20) NOT NULL,
    block_id VARCHAR(20) NOT NULL,
    district_id VARCHAR(20) NOT NULL,
    state_id VARCHAR(20) NOT NULL,
    consent_status VARCHAR(20) NOT NULL,  -- ACTIVE, WITHDRAWN, EXPIRED
    consent_date TIMESTAMP,
    consent_expiry TIMESTAMP,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    INDEX idx_village (village_id),
    INDEX idx_district (district_id)
);
```

**Immutability Enforcement**:
- No UPDATE or DELETE operations on event tables
- Corrections handled via new event with `corrects_event_id` field
- Soft deletion for GDPR compliance: set `anonymized_at` timestamp, hash beneficiary_hash again


### 4.6 Withdrawal Correlation Engine

**Responsibility**: Match withdrawal events to credit events and calculate retention windows.

**Correlation Algorithm**:
```python
class WithdrawalCorrelator:
    def correlate_withdrawal(
        self,
        withdrawal: WithdrawalEvent
    ) -> Optional[CorrelationResult]:
        """
        Match withdrawal to most recent unmatched credit.
        
        Algorithm:
        1. Query credit_events for same beneficiary_hash
        2. Filter: transaction_date <= withdrawal.date
        3. Filter: matched_credit_id IS NULL (unmatched)
        4. Order by transaction_timestamp DESC
        5. Take first match (most recent)
        6. Calculate retention_window = withdrawal.timestamp - credit.timestamp
        7. Update withdrawal.matched_credit_id
        
        Returns:
            CorrelationResult(
                credit_event_id: UUID,
                retention_window_hours: int,
                match_confidence: float
            )
        """
        pass
```

**Edge Cases**:
- Multiple credits same day: Match to closest timestamp
- Partial withdrawals: Match to largest unmatched credit
- Withdrawal > credit: Flag as anomaly, still correlate
- No matching credit: Store as unmatched, investigate later

**Performance Optimization**:
- Maintain in-memory cache of recent unmatched credits (last 30 days)
- Batch correlation: process withdrawals in groups of 1000
- Index on (beneficiary_hash, transaction_timestamp DESC, matched_credit_id)

### 4.7 Anomaly Detection Engine

**Responsibility**: Identify suspicious withdrawal patterns using statistical and rule-based methods.

**Detection Rules**:

1. **Immediate Withdrawal Rule**:
   - Trigger: retention_window < 2 hours
   - Severity: HIGH
   - Rationale: Suggests assisted/coerced withdrawal at bank branch

2. **Pattern Anomaly Rule**:
   - Trigger: 3+ consecutive credits with retention_window < 6 hours
   - Severity: HIGH
   - Rationale: Systematic leakage pattern

3. **Full Withdrawal Rule**:
   - Trigger: |withdrawal_amount - credit_amount| < ₹10
   - Severity: MEDIUM
   - Rationale: Complete fund extraction (vs. partial use)

4. **Statistical Outlier Detection**:
   - Method: Isolation Forest on features [retention_window, withdrawal_ratio, transaction_hour]
   - Training: Village-level baseline (minimum 100 beneficiaries)
   - Trigger: Anomaly score > 0.7
   - Severity: MEDIUM

5. **Installment Gap Rule**:
   - Trigger: Expected installment missing for >7 days (grace period)
   - Severity: MEDIUM (escalates to HIGH after 2+ missed)
   - Rationale: Payment system failure or beneficiary exclusion

**Interface**:
```python
class AnomalyDetector:
    def detect_anomalies(
        self,
        correlation_result: CorrelationResult,
        beneficiary_history: List[CreditEvent]
    ) -> List[Anomaly]:
        """
        Run all detection rules and return flagged anomalies.
        
        Returns:
            List of Anomaly objects with type, severity, evidence
        """
        pass
    
    def update_baseline(
        self,
        village_id: str,
        recent_events: List[CorrelationResult]
    ) -> None:
        """Update village-level statistical baseline (weekly job)."""
        pass
```

**False Positive Mitigation**:
- Whitelist: Beneficiaries with consistent immediate withdrawal (e.g., elderly with caregiver)
- Temporal adjustment: Higher thresholds during festival seasons (expected spending)
- Feedback loop: Administrators mark false positives, retrain isolation forest

### 4.8 Installment Continuity Monitor

**Responsibility**: Track expected periodic payments and flag missing installments.

**Scheme Periodicity Registry**:
```python
SCHEME_PERIODICITY = {
    "PM_KISAN": {"period_days": 120, "grace_days": 7},  # Quarterly
    "OLD_AGE_PENSION": {"period_days": 30, "grace_days": 7},  # Monthly
    "WIDOW_PENSION": {"period_days": 30, "grace_days": 7},
    "SCHOLARSHIP": {"period_days": 90, "grace_days": 14},  # Quarterly
    "LPG_SUBSIDY": {"period_days": 60, "grace_days": 10},  # Bi-monthly
}
```

**Monitoring Algorithm**:
```python
class InstallmentMonitor:
    def check_continuity(
        self,
        beneficiary_hash: str,
        scheme_id: str
    ) -> Optional[ContinuityGap]:
        """
        Check if expected installment is missing.
        
        Algorithm:
        1. Get last credit_event for beneficiary + scheme
        2. Calculate expected_next_date = last_date + period_days
        3. If current_date > expected_next_date + grace_days:
               Flag as MISSING_INSTALLMENT
        4. Count consecutive missing installments
        5. Escalate severity if count >= 2
        
        Returns:
            ContinuityGap(
                expected_date: date,
                days_overdue: int,
                consecutive_missing: int,
                severity: str
            )
        """
        pass
```

**Scheduled Job**:
- Frequency: Daily at 3 AM (after aggregation pipeline)
- Scope: All beneficiaries with active consent and recent scheme activity
- Output: Continuity gap records inserted into `installment_gaps` table

### 4.9 Voice Processing Module (Optional)

**Components**:

1. **ASR Engine**:
   - Option A: Bhashini API (government-provided, 12 Indian languages)
   - Option B: OpenAI Whisper large-v3 (multilingual, self-hosted)
   - Input: Audio file (WAV/MP3, max 5 minutes)
   - Output: Transcript, language code, word-level timestamps

2. **Intent Classifier**:
   - Model: DistilBERT fine-tuned on grievance corpus
   - Classes: payment_delay, wrong_amount, coercion, account_issue, scheme_query, other
   - Training data: 5K+ labeled grievances (synthetic + real)

3. **Grievance Entity Extractor**:
   - Same NER model as SMS extraction, retrained on voice transcript style
   - Entities: SCHEME_NAME, AMOUNT, DATE, LOCATION, ISSUE_TYPE

**Interface**:
```python
class VoiceProcessor:
    def process_grievance(
        self,
        audio_file_path: str,
        beneficiary_hash: str
    ) -> GrievanceRecord:
        """
        Process voice grievance end-to-end.
        
        Steps:
        1. Transcribe audio using ASR
        2. Classify intent
        3. Extract entities
        4. Link to credit events if possible
        5. Store grievance record
        6. Trigger alert to appropriate admin
        
        Returns:
            GrievanceRecord with transcript, intent, entities, linked_events
        """
        pass
```

**Privacy Considerations**:
- Audio files stored encrypted in object storage
- Retention: 90 days, then deleted (transcripts retained)
- Access: Only authorized administrators with audit logging


### 4.10 Aggregation Pipeline

**Responsibility**: Compute hierarchical statistics from beneficiary-level events.

**Pipeline Stages**:

```python
class AggregationPipeline:
    def run_daily_aggregation(self, target_date: date) -> None:
        """
        Execute full aggregation pipeline for target date.
        
        Stages:
        1. Village-level aggregation
        2. Block-level rollup
        3. District-level rollup
        4. State-level rollup
        5. Cache population
        6. Anomaly summary generation
        """
        pass
    
    def aggregate_village(
        self,
        village_id: str,
        date_range: Tuple[date, date]
    ) -> VillageMetrics:
        """
        Compute village-level metrics.
        
        Metrics:
        - total_credits: count of credit events
        - total_amount_credited: sum of amounts
        - total_withdrawals: count of withdrawal events
        - median_retention_window: median hours
        - retention_distribution: histogram [0-2h, 2-6h, 6-24h, 1-7d, 7-30d, 30d+]
        - anomaly_count: by severity (HIGH, MEDIUM, LOW)
        - scheme_distribution: percentage per scheme
        - unique_beneficiaries: count of distinct beneficiary_hash
        - installment_gaps: count of missing installments
        """
        pass
```

**Aggregation Schema**:
```sql
CREATE TABLE village_metrics (
    metric_id UUID PRIMARY KEY,
    village_id VARCHAR(20) NOT NULL,
    date DATE NOT NULL,
    total_credits INT,
    total_amount_credited DECIMAL(15, 2),
    total_withdrawals INT,
    median_retention_hours INT,
    retention_distribution JSONB,  -- {0-2h: 15, 2-6h: 30, ...}
    anomaly_count JSONB,  -- {HIGH: 5, MEDIUM: 12, LOW: 8}
    scheme_distribution JSONB,  -- {PM_KISAN: 45%, MGNREGA: 30%, ...}
    unique_beneficiaries INT,
    installment_gaps INT,
    created_at TIMESTAMP DEFAULT NOW(),
    UNIQUE (village_id, date),
    INDEX idx_village_date (village_id, date DESC)
);

-- Similar tables for block_metrics, district_metrics, state_metrics
```

**Rollup Logic**:
- Sum: total_credits, total_amount_credited, total_withdrawals, unique_beneficiaries
- Weighted average: median_retention_hours (weighted by beneficiary count)
- Merge: retention_distribution (sum counts across bins)
- Merge: anomaly_count (sum by severity)
- Recalculate: scheme_distribution (aggregate percentages)

**Performance Optimization**:
- Materialized views for common queries
- Partitioning: village_metrics partitioned by date (monthly partitions)
- Parallel processing: Aggregate villages in parallel (100 workers)
- Incremental updates: Only recompute changed villages (detect via event timestamps)

### 4.11 Alert Management System

**Responsibility**: Deliver anomaly alerts to appropriate administrators based on severity and jurisdiction.

**Alert Routing Logic**:
```python
class AlertManager:
    def route_alert(self, anomaly: Anomaly) -> List[AlertRecipient]:
        """
        Determine recipients based on anomaly location and severity.
        
        Routing rules:
        - HIGH severity: Village + Block + District (immediate)
        - MEDIUM severity: Village + Block (within 1 hour)
        - LOW severity: Village only (daily digest)
        
        Returns:
            List of AlertRecipient with user_id, role, delivery_channel
        """
        pass
    
    def deliver_alert(
        self,
        alert: Alert,
        recipients: List[AlertRecipient]
    ) -> DeliveryStatus:
        """
        Send alert via configured channels (SMS, email, push, dashboard).
        
        Delivery channels:
        - SMS: For HIGH severity, immediate delivery
        - Email: For MEDIUM/HIGH, with detailed context
        - Dashboard: Real-time notification badge
        - Mobile push: If mobile app installed
        """
        pass
    
    def track_acknowledgment(
        self,
        alert_id: UUID,
        user_id: str,
        action: str  # ACKNOWLEDGED, INVESTIGATING, RESOLVED, FALSE_POSITIVE
    ) -> None:
        """Track administrator response to alert."""
        pass
```

**Escalation Policy**:
- HIGH severity unacknowledged for 4 hours → Escalate to district
- MEDIUM severity unacknowledged for 24 hours → Escalate to block
- Resolved alerts: Close with resolution notes for audit trail

**Alert Schema**:
```sql
CREATE TABLE alerts (
    alert_id UUID PRIMARY KEY,
    anomaly_id UUID REFERENCES anomalies(id),
    severity VARCHAR(10) NOT NULL,
    alert_type VARCHAR(50) NOT NULL,
    beneficiary_hash VARCHAR(64),
    village_id VARCHAR(20),
    scheme_id VARCHAR(50),
    context JSONB,  -- Anomaly details
    created_at TIMESTAMP DEFAULT NOW(),
    acknowledged_at TIMESTAMP,
    acknowledged_by VARCHAR(50),
    resolved_at TIMESTAMP,
    resolution_notes TEXT,
    INDEX idx_village_severity (village_id, severity, created_at DESC)
);
```

### 4.12 Consent Management Module

**Responsibility**: Manage beneficiary consent lifecycle per DPDP Act 2023.

**Consent States**:
- PENDING: Consent requested, awaiting response
- ACTIVE: Consent granted, data processing allowed
- WITHDRAWN: Consent revoked, anonymize data
- EXPIRED: Consent expired (24 months), request renewal

**Interface**:
```python
class ConsentManager:
    def request_consent(
        self,
        beneficiary_phone: str,
        consent_mode: str  # SMS, VOICE, WRITTEN, DIGITAL
    ) -> ConsentRequest:
        """
        Initiate consent request.
        
        Sends:
        - SMS with consent text in beneficiary's language
        - Unique consent code for response
        - Link to detailed privacy policy
        """
        pass
    
    def record_consent(
        self,
        beneficiary_phone: str,
        consent_code: str,
        granted: bool,
        evidence: Optional[bytes]  # Audio recording, signed form scan
    ) -> ConsentRecord:
        """
        Record consent decision with audit trail.
        
        If granted:
        - Set consent_status = ACTIVE
        - Set consent_expiry = now + 24 months
        - Store evidence in encrypted storage
        
        If denied:
        - Set consent_status = DENIED
        - Do not process SMS data for this beneficiary
        """
        pass
    
    def withdraw_consent(
        self,
        beneficiary_hash: str
    ) -> None:
        """
        Process consent withdrawal.
        
        Actions:
        1. Set consent_status = WITHDRAWN
        2. Anonymize beneficiary_hash in all event tables (re-hash)
        3. Preserve aggregated statistics (no PII)
        4. Stop processing future SMS
        5. Log withdrawal in audit trail
        """
        pass
    
    def check_expiry(self) -> List[str]:
        """
        Daily job to identify expired consents.
        
        Returns:
        List of beneficiary_hash with expired consent (>24 months)
        Triggers renewal request SMS
        """
        pass
```

**Consent Schema**:
```sql
CREATE TABLE consent_records (
    consent_id UUID PRIMARY KEY,
    beneficiary_hash VARCHAR(64) UNIQUE NOT NULL,
    consent_status VARCHAR(20) NOT NULL,
    consent_mode VARCHAR(20),
    consent_date TIMESTAMP,
    consent_expiry TIMESTAMP,
    withdrawal_date TIMESTAMP,
    evidence_storage_path VARCHAR(255),  -- Encrypted file path
    language_preference VARCHAR(10),
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    INDEX idx_status_expiry (consent_status, consent_expiry)
);
```


### 4.13 Access Control Module

**Responsibility**: Enforce role-based access control (RBAC) for dashboards and APIs.

**Role Hierarchy**:
```
ADMIN (System Administrator)
  └─ STATE_OFFICER (State Welfare Monitoring Unit)
      └─ DISTRICT_OFFICER (District Welfare Administrator)
          └─ BLOCK_OFFICER (Block Development Officer)
              └─ VILLAGE_OFFICER (Gram Panchayat Official)
```

**Permission Model**:
```python
class AccessControlModule:
    def check_permission(
        self,
        user_id: str,
        resource_type: str,  # VILLAGE_METRICS, BENEFICIARY_DETAIL, ALERT, etc.
        resource_id: str,  # village_id, beneficiary_hash, alert_id
        action: str  # READ, WRITE, DELETE
    ) -> bool:
        """
        Check if user has permission for action on resource.
        
        Rules:
        - VILLAGE_OFFICER: READ village_metrics for assigned villages only
        - BLOCK_OFFICER: READ all villages in assigned block
        - DISTRICT_OFFICER: READ all blocks/villages in assigned district
        - STATE_OFFICER: READ all districts in state
        - ADMIN: Full access (READ, WRITE, DELETE)
        
        Beneficiary-level data:
        - Only DISTRICT_OFFICER and above can view beneficiary_hash
        - VILLAGE_OFFICER sees only aggregated metrics
        """
        pass
    
    def get_accessible_resources(
        self,
        user_id: str,
        resource_type: str
    ) -> List[str]:
        """
        Get list of resource IDs user can access.
        
        Example:
        - VILLAGE_OFFICER → [village_001, village_002]
        - BLOCK_OFFICER → [block_01] (implies all villages in block)
        """
        pass
```

**User Schema**:
```sql
CREATE TABLE users (
    user_id VARCHAR(50) PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL,
    phone VARCHAR(15),
    role VARCHAR(20) NOT NULL,
    jurisdiction_type VARCHAR(20),  -- VILLAGE, BLOCK, DISTRICT, STATE
    jurisdiction_ids TEXT[],  -- Array of assigned IDs
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT NOW(),
    last_login TIMESTAMP,
    INDEX idx_role_jurisdiction (role, jurisdiction_type)
);

CREATE TABLE access_logs (
    log_id UUID PRIMARY KEY,
    user_id VARCHAR(50) NOT NULL,
    resource_type VARCHAR(50) NOT NULL,
    resource_id VARCHAR(100),
    action VARCHAR(20) NOT NULL,
    granted BOOLEAN NOT NULL,
    ip_address INET,
    user_agent TEXT,
    timestamp TIMESTAMP DEFAULT NOW(),
    INDEX idx_user_timestamp (user_id, timestamp DESC)
);
```

### 4.14 Dashboard Service

**Responsibility**: Serve aggregated metrics and visualizations to web UI.

**API Endpoints**:

```python
class DashboardService:
    @route("/api/v1/metrics/village/{village_id}")
    @requires_permission("VILLAGE_METRICS", "READ")
    def get_village_metrics(
        self,
        village_id: str,
        start_date: date,
        end_date: date
    ) -> VillageMetricsResponse:
        """
        Retrieve village-level metrics for date range.
        
        Response includes:
        - Time series: daily metrics
        - Retention distribution: histogram
        - Scheme breakdown: pie chart data
        - Anomaly summary: count by severity
        - Top anomalies: list of recent HIGH severity
        """
        pass
    
    @route("/api/v1/metrics/district/{district_id}/compare")
    @requires_permission("DISTRICT_METRICS", "READ")
    def compare_blocks(
        self,
        district_id: str,
        metric: str,  # median_retention, anomaly_rate, etc.
        date: date
    ) -> ComparisonResponse:
        """
        Compare blocks within district on specific metric.
        
        Returns:
        - Ranked list of blocks
        - Statistical distribution (mean, median, std dev)
        - Outlier blocks flagged
        """
        pass
    
    @route("/api/v1/alerts")
    @requires_permission("ALERT", "READ")
    def get_alerts(
        self,
        user_id: str,
        severity: Optional[str] = None,
        status: Optional[str] = None,  # OPEN, ACKNOWLEDGED, RESOLVED
        limit: int = 50
    ) -> AlertListResponse:
        """
        Get alerts for user's jurisdiction.
        
        Filters by:
        - User's accessible villages/blocks/districts
        - Severity (if specified)
        - Status (if specified)
        
        Returns paginated list with alert details
        """
        pass
```

**Caching Strategy**:
- Village/block/district metrics: Cache in Redis with 24-hour TTL
- Real-time alerts: No caching (query database directly)
- User permissions: Cache for 1 hour (invalidate on role change)

**Response Format**:
```json
{
  "status": "success",
  "data": {
    "village_id": "VIL001",
    "village_name": "Rampur",
    "date_range": {"start": "2024-01-01", "end": "2024-01-31"},
    "metrics": {
      "total_credits": 1250,
      "total_amount_credited": 2500000.00,
      "unique_beneficiaries": 450,
      "median_retention_hours": 72,
      "retention_distribution": {
        "0-2h": 45,
        "2-6h": 78,
        "6-24h": 120,
        "1-7d": 350,
        "7-30d": 520,
        "30d+": 137
      },
      "anomaly_count": {"HIGH": 5, "MEDIUM": 12, "LOW": 8},
      "scheme_distribution": {
        "PM_KISAN": 45.2,
        "MGNREGA": 28.7,
        "OLD_AGE_PENSION": 18.3,
        "OTHER": 7.8
      }
    }
  },
  "metadata": {
    "data_freshness": "2024-02-01T02:30:00Z",
    "cache_hit": true
  }
}
```


## 5. Data Models

### 5.1 Core Entity Relationships

```
┌─────────────────────┐
│ Beneficiary         │
│ (hashed identity)   │
└──────┬──────────────┘
       │ 1
       │
       │ N
┌──────▼──────────────┐         ┌─────────────────────┐
│ Credit Event        │ 1     1 │ Withdrawal Event    │
│                     ├─────────┤                     │
│ - event_id          │ matched │ - event_id          │
│ - beneficiary_hash  │         │ - beneficiary_hash  │
│ - scheme_id         │         │ - amount            │
│ - amount            │         │ - timestamp         │
│ - timestamp         │         │ - matched_credit_id │
│ - reference         │         │ - retention_window  │
└──────┬──────────────┘         └─────────────────────┘
       │
       │ 1
       │
       │ N
┌──────▼──────────────┐
│ Anomaly             │
│                     │
│ - anomaly_id        │
│ - credit_event_id   │
│ - anomaly_type      │
│ - severity          │
│ - detected_at       │
└──────┬──────────────┘
       │
       │ 1
       │
       │ 1
┌──────▼──────────────┐
│ Alert               │
│                     │
│ - alert_id          │
│ - anomaly_id        │
│ - recipients        │
│ - status            │
└─────────────────────┘
```

### 5.2 Aggregation Hierarchy

```
┌─────────────────────┐
│ State Metrics       │
│                     │
│ - state_id          │
│ - date              │
│ - aggregated_stats  │
└──────┬──────────────┘
       │ 1
       │
       │ N
┌──────▼──────────────┐
│ District Metrics    │
│                     │
│ - district_id       │
│ - date              │
│ - aggregated_stats  │
└──────┬──────────────┘
       │ 1
       │
       │ N
┌──────▼──────────────┐
│ Block Metrics       │
│                     │
│ - block_id          │
│ - date              │
│ - aggregated_stats  │
└──────┬──────────────┘
       │ 1
       │
       │ N
┌──────▼──────────────┐
│ Village Metrics     │
│                     │
│ - village_id        │
│ - date              │
│ - aggregated_stats  │
└─────────────────────┘
```

### 5.3 Complete Schema Definitions

**SMS and Events**:
```sql
CREATE TABLE sms_records (
    id UUID PRIMARY KEY,
    message_text TEXT NOT NULL,
    message_hash VARCHAR(64) UNIQUE NOT NULL,
    sender_id VARCHAR(20) NOT NULL,
    recipient_phone_hash VARCHAR(64) NOT NULL,
    received_at TIMESTAMP NOT NULL,
    processed_at TIMESTAMP,
    processing_status VARCHAR(20),  -- PENDING, PROCESSED, FAILED
    error_message TEXT,
    created_at TIMESTAMP DEFAULT NOW(),
    INDEX idx_status (processing_status, received_at)
);

CREATE TABLE credit_events (
    event_id UUID PRIMARY KEY,
    beneficiary_hash VARCHAR(64) NOT NULL,
    scheme_id VARCHAR(50) NOT NULL,
    amount DECIMAL(12, 2) NOT NULL,
    transaction_date DATE NOT NULL,
    transaction_timestamp TIMESTAMP,
    reference_number VARCHAR(50),
    account_last4 VARCHAR(4),
    sms_id UUID REFERENCES sms_records(id),
    extraction_confidence JSONB,
    created_at TIMESTAMP DEFAULT NOW(),
    INDEX idx_beneficiary_date (beneficiary_hash, transaction_date DESC),
    INDEX idx_scheme_date (scheme_id, transaction_date DESC),
    INDEX idx_reference (reference_number)
);

CREATE TABLE withdrawal_events (
    event_id UUID PRIMARY KEY,
    beneficiary_hash VARCHAR(64) NOT NULL,
    amount DECIMAL(12, 2) NOT NULL,
    transaction_date DATE NOT NULL,
    transaction_timestamp TIMESTAMP,
    matched_credit_id UUID REFERENCES credit_events(event_id),
    retention_window_hours INT,
    sms_id UUID REFERENCES sms_records(id),
    created_at TIMESTAMP DEFAULT NOW(),
    INDEX idx_beneficiary_date (beneficiary_hash, transaction_date DESC),
    INDEX idx_matched_credit (matched_credit_id)
);
```

**Anomalies and Alerts**:
```sql
CREATE TABLE anomalies (
    anomaly_id UUID PRIMARY KEY,
    credit_event_id UUID REFERENCES credit_events(event_id),
    withdrawal_event_id UUID REFERENCES withdrawal_events(event_id),
    anomaly_type VARCHAR(50) NOT NULL,  -- IMMEDIATE_WITHDRAWAL, PATTERN_ANOMALY, etc.
    severity VARCHAR(10) NOT NULL,  -- HIGH, MEDIUM, LOW
    detection_method VARCHAR(50),  -- RULE_BASED, STATISTICAL, ML_MODEL
    evidence JSONB,  -- Context for detection
    detected_at TIMESTAMP DEFAULT NOW(),
    false_positive BOOLEAN DEFAULT FALSE,
    false_positive_marked_by VARCHAR(50),
    false_positive_marked_at TIMESTAMP,
    INDEX idx_severity_date (severity, detected_at DESC),
    INDEX idx_credit_event (credit_event_id)
);

CREATE TABLE alerts (
    alert_id UUID PRIMARY KEY,
    anomaly_id UUID REFERENCES anomalies(anomaly_id),
    severity VARCHAR(10) NOT NULL,
    alert_type VARCHAR(50) NOT NULL,
    beneficiary_hash VARCHAR(64),
    village_id VARCHAR(20),
    block_id VARCHAR(20),
    district_id VARCHAR(20),
    scheme_id VARCHAR(50),
    context JSONB,
    created_at TIMESTAMP DEFAULT NOW(),
    acknowledged_at TIMESTAMP,
    acknowledged_by VARCHAR(50),
    resolved_at TIMESTAMP,
    resolved_by VARCHAR(50),
    resolution_notes TEXT,
    escalated BOOLEAN DEFAULT FALSE,
    escalated_at TIMESTAMP,
    INDEX idx_village_severity (village_id, severity, created_at DESC),
    INDEX idx_status (acknowledged_at, resolved_at)
);
```

**Aggregated Metrics**:
```sql
CREATE TABLE village_metrics (
    metric_id UUID PRIMARY KEY,
    village_id VARCHAR(20) NOT NULL,
    date DATE NOT NULL,
    total_credits INT,
    total_amount_credited DECIMAL(15, 2),
    total_withdrawals INT,
    total_amount_withdrawn DECIMAL(15, 2),
    median_retention_hours INT,
    mean_retention_hours DECIMAL(10, 2),
    retention_distribution JSONB,
    anomaly_count JSONB,
    scheme_distribution JSONB,
    unique_beneficiaries INT,
    installment_gaps INT,
    data_quality_score DECIMAL(3, 2),  -- 0.00 to 1.00
    created_at TIMESTAMP DEFAULT NOW(),
    UNIQUE (village_id, date),
    INDEX idx_village_date (village_id, date DESC)
) PARTITION BY RANGE (date);

-- Similar structure for block_metrics, district_metrics, state_metrics
-- with appropriate aggregation fields
```

**Consent and Users**:
```sql
CREATE TABLE consent_records (
    consent_id UUID PRIMARY KEY,
    beneficiary_hash VARCHAR(64) UNIQUE NOT NULL,
    consent_status VARCHAR(20) NOT NULL,
    consent_mode VARCHAR(20),
    consent_date TIMESTAMP,
    consent_expiry TIMESTAMP,
    withdrawal_date TIMESTAMP,
    evidence_storage_path VARCHAR(255),
    language_preference VARCHAR(10),
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    INDEX idx_status_expiry (consent_status, consent_expiry)
);

CREATE TABLE users (
    user_id VARCHAR(50) PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL,
    phone VARCHAR(15),
    role VARCHAR(20) NOT NULL,
    jurisdiction_type VARCHAR(20),
    jurisdiction_ids TEXT[],
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT NOW(),
    last_login TIMESTAMP,
    INDEX idx_role_jurisdiction (role, jurisdiction_type)
);
```

**Voice Grievances** (Optional):
```sql
CREATE TABLE grievances (
    grievance_id UUID PRIMARY KEY,
    beneficiary_hash VARCHAR(64) NOT NULL,
    audio_file_path VARCHAR(255),
    transcript TEXT,
    language_code VARCHAR(10),
    intent VARCHAR(50),
    extracted_entities JSONB,
    linked_credit_events UUID[],
    submitted_at TIMESTAMP DEFAULT NOW(),
    processed_at TIMESTAMP,
    resolution_status VARCHAR(20),  -- OPEN, IN_PROGRESS, RESOLVED, CLOSED
    resolution_notes TEXT,
    INDEX idx_beneficiary (beneficiary_hash),
    INDEX idx_status (resolution_status, submitted_at DESC)
);
```


## 6. Orchestration and Scheduling

### 6.1 Event-Driven Processing

**Kafka Topics**:
```
sms.raw              → Raw SMS ingestion (retention: 30 days)
sms.normalized       → Normalized SMS (retention: 30 days)
sms.classified       → Classified SMS with scheme (retention: 30 days)
events.credit        → Credit events (retention: 90 days)
events.withdrawal    → Withdrawal events (retention: 90 days)
anomalies.detected   → Detected anomalies (retention: 90 days)
alerts.triggered     → Triggered alerts (retention: 90 days)
```

**Consumer Groups**:
- `sms-normalizer`: Consumes `sms.raw`, produces `sms.normalized`
- `sms-classifier`: Consumes `sms.normalized`, produces `sms.classified`
- `entity-extractor`: Consumes `sms.classified`, produces `events.credit` or `events.withdrawal`
- `withdrawal-correlator`: Consumes `events.withdrawal`, updates database
- `anomaly-detector`: Consumes correlation results, produces `anomalies.detected`
- `alert-manager`: Consumes `anomalies.detected`, produces `alerts.triggered`

**Processing Guarantees**:
- At-least-once delivery (idempotent consumers handle duplicates)
- Offset management: Commit after successful database write
- Dead letter queue: Failed messages after 3 retries → manual review

### 6.2 Batch Jobs

**Daily Aggregation Pipeline** (Scheduled: 2:00 AM):
```python
def daily_aggregation_job(target_date: date):
    """
    Compute hierarchical aggregations for previous day.
    
    Steps:
    1. Aggregate village metrics (parallel: 100 workers)
    2. Rollup to block metrics (parallel: 20 workers)
    3. Rollup to district metrics (parallel: 5 workers)
    4. Rollup to state metrics (single worker)
    5. Populate Redis cache
    6. Generate anomaly summary reports
    
    Duration: ~2-4 hours for 10M beneficiaries
    """
    pass
```

**Installment Continuity Check** (Scheduled: 3:00 AM):
```python
def installment_continuity_job():
    """
    Check for missing installments across all active beneficiaries.
    
    Steps:
    1. Query beneficiaries with active consent and recent scheme activity
    2. For each beneficiary-scheme pair, check expected vs actual installments
    3. Flag missing installments with grace period consideration
    4. Insert continuity gap records
    5. Trigger alerts for HIGH severity gaps (2+ consecutive missing)
    
    Duration: ~1 hour for 10M beneficiaries
    """
    pass
```

**Consent Expiry Check** (Scheduled: Daily 4:00 AM):
```python
def consent_expiry_job():
    """
    Identify expired consents and trigger renewal requests.
    
    Steps:
    1. Query consent_records where consent_expiry < current_date
    2. Set consent_status = EXPIRED
    3. Send renewal request SMS to beneficiaries
    4. Log renewal requests for tracking
    
    Duration: ~30 minutes
    """
    pass
```

**Model Performance Monitoring** (Scheduled: Weekly Sunday 1:00 AM):
```python
def model_monitoring_job():
    """
    Evaluate model performance on recent data.
    
    Steps:
    1. Sample 1000 manually reviewed SMS from past week
    2. Run classification model and compare predictions
    3. Calculate accuracy, precision, recall, F1
    4. Compare to baseline metrics
    5. Alert if degradation > 5%
    6. Trigger retraining pipeline if degradation > 10%
    
    Duration: ~1 hour
    """
    pass
```

**Data Retention Cleanup** (Scheduled: Monthly 1st 5:00 AM):
```python
def data_retention_job():
    """
    Archive or delete data exceeding retention periods.
    
    Steps:
    1. Archive beneficiary events older than 36 months to cold storage
    2. Delete SMS records older than 90 days
    3. Delete audio files older than 90 days (keep transcripts)
    4. Purge audit logs older than 7 years
    5. Vacuum database tables
    
    Duration: ~4 hours
    """
    pass
```

### 6.3 Workflow Orchestration

**Tool**: Apache Airflow or Temporal

**DAG Structure** (Daily Aggregation):
```python
from airflow import DAG
from airflow.operators.python import PythonOperator

with DAG('daily_aggregation', schedule_interval='0 2 * * *') as dag:
    
    aggregate_villages = PythonOperator(
        task_id='aggregate_villages',
        python_callable=aggregate_all_villages,
        op_kwargs={'target_date': '{{ ds }}'}
    )
    
    aggregate_blocks = PythonOperator(
        task_id='aggregate_blocks',
        python_callable=aggregate_all_blocks,
        op_kwargs={'target_date': '{{ ds }}'}
    )
    
    aggregate_districts = PythonOperator(
        task_id='aggregate_districts',
        python_callable=aggregate_all_districts,
        op_kwargs={'target_date': '{{ ds }}'}
    )
    
    aggregate_state = PythonOperator(
        task_id='aggregate_state',
        python_callable=aggregate_state_level,
        op_kwargs={'target_date': '{{ ds }}'}
    )
    
    populate_cache = PythonOperator(
        task_id='populate_cache',
        python_callable=populate_redis_cache,
        op_kwargs={'target_date': '{{ ds }}'}
    )
    
    # Define dependencies
    aggregate_villages >> aggregate_blocks >> aggregate_districts >> aggregate_state >> populate_cache
```


## 7. Privacy-Preserving Architecture

### 7.1 Data Minimization

**Principle**: Collect and retain only data necessary for retention monitoring.

**Implementation**:
- Store beneficiary phone numbers as SHA-256 hashes (irreversible)
- Do not store beneficiary names, Aadhaar numbers, or full account numbers
- SMS text retained for 90 days only (sufficient for model retraining)
- Aggregated metrics retained indefinitely (no PII)

### 7.2 Anonymization Techniques

**Beneficiary Hashing**:
```python
def hash_beneficiary_id(phone_number: str, salt: bytes) -> str:
    """
    Generate irreversible hash of beneficiary identifier.
    
    Algorithm:
    1. Normalize phone number (remove +91, spaces, dashes)
    2. Concatenate with system-wide salt (stored in HSM)
    3. Apply SHA-256 hash
    4. Return hex digest
    
    Properties:
    - Deterministic: Same phone → same hash (enables correlation)
    - Irreversible: Cannot recover phone from hash
    - Collision-resistant: Different phones → different hashes
    """
    normalized = normalize_phone(phone_number)
    salted = normalized.encode() + salt
    return hashlib.sha256(salted).hexdigest()
```

**K-Anonymity for Exports**:
```python
def apply_k_anonymity(data: pd.DataFrame, k: int = 5) -> pd.DataFrame:
    """
    Ensure exported data satisfies k-anonymity (k=5).
    
    Steps:
    1. Identify quasi-identifiers (village_id, scheme_id, age_group, gender)
    2. Group records by quasi-identifier combinations
    3. If group size < k, suppress or generalize
    4. Remove direct identifiers (beneficiary_hash)
    
    Example:
    - Village "VIL001" + Scheme "PM_KISAN" + Age "60-65" → 3 records
    - Suppress: Generalize age to "60-70" or suppress village to "BLOCK_01"
    """
    pass
```

**Differential Privacy for Aggregations**:
```python
def add_laplace_noise(
    true_value: float,
    sensitivity: float,
    epsilon: float = 1.0
) -> float:
    """
    Add calibrated noise to aggregated metrics.
    
    Parameters:
    - true_value: Actual aggregated metric
    - sensitivity: Maximum change from adding/removing one record
    - epsilon: Privacy budget (lower = more privacy, more noise)
    
    Use cases:
    - Village-level median retention: sensitivity = 24 hours, epsilon = 1.0
    - Anomaly counts: sensitivity = 1, epsilon = 0.5
    
    Returns:
    Noisy value satisfying epsilon-differential privacy
    """
    noise = np.random.laplace(0, sensitivity / epsilon)
    return true_value + noise
```

### 7.3 Consent-Based Processing

**Consent Workflow**:
```
1. Beneficiary receives DBT credit SMS
2. System sends consent request SMS:
   "Govt of [State] is monitoring DBT retention to prevent leakage.
    Reply YES to allow analysis of your bank SMS (anonymous).
    Your data will be protected per DPDP Act 2023.
    Reply NO to opt out. More info: [URL]"
3. Beneficiary replies YES or NO
4. System records consent with timestamp
5. If YES: Process SMS data, hash phone number
6. If NO: Do not process, delete any existing data
```

**Consent Renewal** (Every 24 months):
```
"Your consent for DBT monitoring expires on [DATE].
 Reply RENEW to continue or STOP to withdraw.
 Your data remains protected. Info: [URL]"
```

### 7.4 Data Access Controls

**Principle**: Least privilege access, audit all queries.

**Implementation**:
- Village officers: Cannot view beneficiary_hash (only aggregated metrics)
- District officers: Can view beneficiary_hash for investigation (logged)
- State officers: Can view beneficiary_hash for policy analysis (logged)
- Admins: Full access (logged)

**Query Auditing**:
```python
def audit_query(
    user_id: str,
    query: str,
    result_count: int,
    contains_pii: bool
) -> None:
    """
    Log all database queries accessing beneficiary data.
    
    Logged fields:
    - user_id, role, jurisdiction
    - query text (sanitized)
    - timestamp
    - result_count
    - contains_pii flag
    - IP address
    
    Alerts:
    - Bulk exports (>1000 records with PII) → Alert admin
    - Unusual access patterns → Alert security team
    """
    pass
```

### 7.5 Secure Data Storage

**Encryption at Rest**:
- Database: PostgreSQL with Transparent Data Encryption (TDE)
- Object storage: Server-side encryption (SSE-KMS)
- Backups: Encrypted with separate key

**Encryption in Transit**:
- All API calls: TLS 1.3 with certificate pinning
- Internal services: mTLS via Istio service mesh
- Database connections: SSL/TLS required

**Key Management**:
- Master keys: Hardware Security Module (HSM) or AWS KMS
- Key rotation: Every 90 days with zero-downtime migration
- Key access: Logged and restricted to authorized services

### 7.6 Right to be Forgotten

**Implementation**:
```python
def process_deletion_request(beneficiary_hash: str) -> None:
    """
    Implement Right to be Forgotten per DPDP Act.
    
    Steps:
    1. Verify deletion request authenticity (OTP verification)
    2. Re-hash beneficiary_hash with deletion salt (makes it unlinkable)
    3. Update all event tables with new hash
    4. Delete SMS records containing phone number
    5. Delete audio files (if any)
    6. Preserve aggregated statistics (no PII)
    7. Log deletion in audit trail (with original hash for compliance)
    8. Confirm deletion to beneficiary via SMS
    
    Timeline: Complete within 30 days per DPDP Act
    """
    pass
```

**Aggregated Data Preservation**:
- Village/block/district metrics remain unchanged (no PII)
- Beneficiary count decremented
- Historical trends preserved for policy analysis


## 8. Scalability Considerations

### 8.1 Horizontal Scaling Strategy

**Stateless Microservices**:
- All processing services are stateless (no local state)
- State stored in PostgreSQL, Redis, or Kafka
- Services can be scaled independently based on load

**Scaling Targets**:
```
Component                 | Pilot (1M)  | State (10M) | National (100M)
--------------------------|-------------|-------------|------------------
SMS Processors            | 5 pods      | 20 pods     | 100 pods
Classification Inference  | 2 GPU pods  | 10 GPU pods | 50 GPU pods
Aggregation Workers       | 10 pods     | 50 pods     | 200 pods
API Gateway               | 3 pods      | 10 pods     | 50 pods
PostgreSQL                | 1 primary   | 1 primary   | 1 primary
                          | 2 replicas  | 5 replicas  | 20 replicas
Kafka Brokers             | 3 brokers   | 5 brokers   | 15 brokers
Redis Cache               | 1 cluster   | 3 clusters  | 10 clusters
                          | (3 nodes)   | (3 nodes ea)| (3 nodes ea)
```

### 8.2 Database Sharding

**Sharding Strategy**: Shard by `district_id` (geographic partitioning)

**Rationale**:
- Queries are typically scoped to district or below
- Balanced distribution (districts have similar beneficiary counts)
- Simplifies jurisdiction-based access control

**Implementation**:
```python
def get_shard_for_district(district_id: str) -> str:
    """
    Determine database shard for district.
    
    Sharding scheme:
    - Hash district_id to determine shard
    - 10 shards for state-level deployment
    - 100 shards for national deployment
    
    Returns:
    Connection string for appropriate shard
    """
    shard_count = get_shard_count()
    shard_id = hash(district_id) % shard_count
    return f"postgresql://shard-{shard_id}.db.internal:5432/dbt_monitoring"
```

**Cross-Shard Queries**:
- State-level aggregations: Query all shards in parallel, merge results
- Use materialized views for common cross-shard queries
- Cache state-level metrics in Redis (refreshed daily)

### 8.3 Caching Architecture

**Multi-Tier Caching**:

1. **Application Cache** (In-memory, per pod):
   - Scheme metadata (periodicity, names)
   - User permissions
   - TTL: 1 hour

2. **Distributed Cache** (Redis):
   - Aggregated metrics (village/block/district/state)
   - Recent anomaly summaries
   - TTL: 24 hours (refreshed by aggregation pipeline)

3. **CDN Cache** (CloudFront/Cloudflare):
   - Static dashboard assets
   - Public documentation
   - TTL: 7 days

**Cache Invalidation**:
- Time-based: TTL expiration
- Event-based: Invalidate on data updates (rare for aggregated metrics)
- Manual: Admin can force cache refresh

### 8.4 Load Balancing

**API Gateway Load Balancing**:
- Algorithm: Least connections (for long-lived dashboard connections)
- Health checks: HTTP GET /health every 10 seconds
- Circuit breaker: Remove unhealthy pods after 3 failed checks

**Database Load Balancing**:
- Writes: Primary only
- Reads: Round-robin across replicas
- Read-heavy queries (dashboards): Dedicated read replicas
- Write-heavy queries (event ingestion): Primary with connection pooling

### 8.5 Asynchronous Processing

**Queue-Based Architecture**:
- SMS ingestion: Synchronous (fast acknowledgment)
- SMS processing: Asynchronous (Kafka consumers)
- Aggregation: Batch (scheduled jobs)
- Alerts: Asynchronous (Kafka consumers)

**Benefits**:
- Decouples ingestion from processing (prevents backpressure)
- Enables retry logic for transient failures
- Allows independent scaling of components

### 8.6 Performance Optimization

**Database Indexing**:
```sql
-- Critical indexes for query performance
CREATE INDEX CONCURRENTLY idx_credit_events_beneficiary_date 
    ON credit_events (beneficiary_hash, transaction_date DESC);

CREATE INDEX CONCURRENTLY idx_withdrawal_events_beneficiary_date 
    ON withdrawal_events (beneficiary_hash, transaction_date DESC);

CREATE INDEX CONCURRENTLY idx_anomalies_severity_date 
    ON anomalies (severity, detected_at DESC);

CREATE INDEX CONCURRENTLY idx_village_metrics_date 
    ON village_metrics (village_id, date DESC);

-- Partial indexes for common filters
CREATE INDEX CONCURRENTLY idx_alerts_open 
    ON alerts (created_at DESC) 
    WHERE resolved_at IS NULL;
```

**Query Optimization**:
- Use EXPLAIN ANALYZE for slow queries
- Materialized views for complex aggregations
- Partition large tables by date (monthly partitions)
- Connection pooling (PgBouncer): 100 connections per pod

**Model Inference Optimization**:
- Batch inference: Process 32 SMS per batch (GPU utilization)
- ONNX Runtime: 2-3x faster than PyTorch
- INT8 quantization: 4x smaller models, 2x faster inference
- Model caching: Keep model in GPU memory (no reload per request)


## 9. Error Handling and Resilience

### 9.1 SMS Processing Failures

**Failure Modes**:

1. **Unparseable SMS Format**:
   - Cause: Bank changes SMS template, non-standard format
   - Handling: Log to manual review queue, extract what's possible, flag low confidence
   - Recovery: Update regex patterns, retrain NER model

2. **Low Classification Confidence**:
   - Cause: Ambiguous SMS text, new scheme not in training data
   - Handling: Flag for manual review (confidence < 0.7), store with "UNKNOWN_SCHEME"
   - Recovery: Manual labeling → add to retraining dataset

3. **Entity Extraction Failure**:
   - Cause: Missing fields in SMS (no amount, no date)
   - Handling: Store partial event with NULL fields, flag for investigation
   - Recovery: Attempt extraction with alternative patterns, manual review

**Error Recovery**:
```python
class SMSProcessingPipeline:
    def process_with_fallback(self, sms: SMSRecord) -> ProcessingResult:
        """
        Process SMS with multiple fallback strategies.
        
        Strategy:
        1. Try primary classification model
        2. If confidence < 0.7, try secondary model (different architecture)
        3. If still low, try rule-based classification (keyword matching)
        4. If all fail, queue for manual review
        
        Entity extraction:
        1. Try regex patterns (fast path)
        2. If fails, try NER model
        3. If fails, try fuzzy matching against known schemes
        4. If all fail, store with NULL fields and flag
        """
        try:
            classification = self.primary_classifier.predict(sms.text)
            if classification.confidence < 0.7:
                classification = self.secondary_classifier.predict(sms.text)
            if classification.confidence < 0.5:
                classification = self.rule_based_classifier.predict(sms.text)
            
            entities = self.extract_entities(sms.text, classification.scheme_id)
            
            return ProcessingResult(
                classification=classification,
                entities=entities,
                status="SUCCESS" if classification.confidence >= 0.7 else "LOW_CONFIDENCE"
            )
        except Exception as e:
            self.log_error(sms.id, e)
            self.queue_for_manual_review(sms)
            return ProcessingResult(status="FAILED", error=str(e))
```

### 9.2 ASR and Voice Processing Failures

**Failure Modes**:

1. **Poor Audio Quality**:
   - Cause: Background noise, low volume, poor recording
   - Handling: Return low confidence score, flag for manual transcription
   - Mitigation: Audio preprocessing (noise reduction, normalization)

2. **Unsupported Language/Dialect**:
   - Cause: Regional dialect not in ASR training data
   - Handling: Attempt transcription, flag low confidence, offer manual transcription
   - Recovery: Collect dialect samples, fine-tune ASR model

3. **ASR Service Unavailable**:
   - Cause: Bhashini API downtime, network issues
   - Handling: Queue audio file, retry with exponential backoff (1m, 5m, 15m, 1h)
   - Fallback: Switch to backup ASR service (Whisper self-hosted)

**Circuit Breaker Pattern**:
```python
class ASRServiceClient:
    def __init__(self):
        self.failure_count = 0
        self.circuit_open = False
        self.last_failure_time = None
    
    def transcribe(self, audio_path: str) -> Transcript:
        """
        Call ASR service with circuit breaker.
        
        Circuit breaker states:
        - CLOSED: Normal operation
        - OPEN: Service unavailable, fail fast
        - HALF_OPEN: Test if service recovered
        
        Thresholds:
        - Open circuit after 5 consecutive failures
        - Keep open for 5 minutes
        - Test recovery with single request
        """
        if self.circuit_open:
            if time.time() - self.last_failure_time > 300:  # 5 minutes
                self.circuit_open = False  # Try half-open
            else:
                raise CircuitBreakerOpenError("ASR service unavailable")
        
        try:
            result = self._call_asr_api(audio_path)
            self.failure_count = 0
            return result
        except Exception as e:
            self.failure_count += 1
            self.last_failure_time = time.time()
            if self.failure_count >= 5:
                self.circuit_open = True
            raise
```

### 9.3 Database Failures

**Failure Modes**:

1. **Primary Database Unavailable**:
   - Detection: Health check fails, connection timeout
   - Handling: Automatic failover to replica (promoted to primary)
   - Recovery time: <60 seconds (Patroni/Stolon orchestration)

2. **Replica Lag**:
   - Detection: Replication lag > 10 seconds
   - Handling: Remove lagging replica from read pool
   - Recovery: Replica catches up, re-added to pool

3. **Disk Full**:
   - Detection: Disk usage > 90%
   - Handling: Alert admin, trigger cleanup job (delete old SMS records)
   - Prevention: Monitor disk usage, auto-scale storage

**Connection Pooling**:
```python
# PgBouncer configuration
[databases]
dbt_monitoring = host=db-primary.internal port=5432 dbname=dbt_monitoring

[pgbouncer]
pool_mode = transaction
max_client_conn = 1000
default_pool_size = 25
reserve_pool_size = 5
reserve_pool_timeout = 3
```

### 9.4 Kafka Failures

**Failure Modes**:

1. **Broker Unavailable**:
   - Detection: Producer cannot connect to broker
   - Handling: Kafka client automatically retries with other brokers
   - Recovery: Broker restarts, rejoins cluster

2. **Consumer Lag**:
   - Detection: Consumer lag > 10,000 messages
   - Handling: Scale up consumer group (add more pods)
   - Alert: Notify admin if lag persists > 1 hour

3. **Message Processing Failure**:
   - Handling: Retry 3 times with exponential backoff
   - Dead letter queue: Move to DLQ after 3 failures
   - Manual review: Admin investigates DLQ messages

**Idempotent Consumers**:
```python
class IdempotentSMSProcessor:
    def process_message(self, message: KafkaMessage) -> None:
        """
        Process Kafka message idempotently.
        
        Idempotency key: message.key (SMS hash)
        
        Algorithm:
        1. Check if message already processed (query by SMS hash)
        2. If yes, skip processing (acknowledge message)
        3. If no, process and insert to database
        4. Commit Kafka offset after successful insert
        
        Handles:
        - Duplicate messages (Kafka at-least-once delivery)
        - Consumer restarts (offset committed after processing)
        """
        sms_hash = message.key
        if self.already_processed(sms_hash):
            logger.info(f"Skipping duplicate SMS: {sms_hash}")
            return
        
        result = self.process_sms(message.value)
        self.store_result(result)
        # Offset committed automatically after return
```

### 9.5 Model Inference Failures

**Failure Modes**:

1. **Model Server Unavailable**:
   - Detection: HTTP 503 from Triton/TorchServe
   - Handling: Retry with exponential backoff, fallback to rule-based classification
   - Alert: Notify ML team if unavailable > 5 minutes

2. **Out of Memory (GPU)**:
   - Detection: CUDA out of memory error
   - Handling: Reduce batch size, retry with smaller batch
   - Prevention: Monitor GPU memory, scale horizontally

3. **Model Accuracy Degradation**:
   - Detection: Weekly monitoring job detects >5% accuracy drop
   - Handling: Alert ML team, trigger retraining pipeline
   - Rollback: Revert to previous model version if degradation > 10%

### 9.6 Offline Synchronization Failures

**Failure Modes**:

1. **Network Unavailable During Sync**:
   - Handling: Queue data locally, retry when network available
   - Conflict resolution: Server timestamp wins

2. **Sync Conflict (Same Event Modified)**:
   - Detection: Event hash mismatch
   - Handling: Log conflict, use server version, flag for manual review

3. **Large Sync Backlog**:
   - Detection: >10,000 queued events
   - Handling: Compress data, batch upload, prioritize recent events
   - Alert: Notify admin if backlog > 24 hours


## 10. Monitoring and Observability

### 10.1 Metrics Collection

**System Metrics** (Prometheus):
```yaml
# SMS Processing Metrics
sms_ingestion_rate: Counter - SMS received per second
sms_processing_latency: Histogram - Time to process SMS (p50, p95, p99)
sms_classification_accuracy: Gauge - Real-time accuracy estimate
sms_extraction_confidence: Histogram - Distribution of confidence scores

# Event Metrics
credit_events_created: Counter - Credit events per minute
withdrawal_events_created: Counter - Withdrawal events per minute
correlation_success_rate: Gauge - % of withdrawals successfully correlated
anomaly_detection_rate: Counter - Anomalies detected per hour

# Model Metrics
model_inference_latency: Histogram - ML model inference time
model_batch_size: Histogram - Batch sizes processed
model_gpu_utilization: Gauge - GPU memory and compute usage
model_error_rate: Counter - Model inference failures

# Database Metrics
db_connection_pool_usage: Gauge - Active connections / pool size
db_query_latency: Histogram - Query execution time
db_replication_lag: Gauge - Replica lag in seconds
db_disk_usage: Gauge - Disk usage percentage

# Kafka Metrics
kafka_consumer_lag: Gauge - Messages behind per consumer group
kafka_message_rate: Counter - Messages produced/consumed per second
kafka_error_rate: Counter - Producer/consumer errors

# API Metrics
api_request_rate: Counter - Requests per second by endpoint
api_response_latency: Histogram - Response time by endpoint
api_error_rate: Counter - 4xx and 5xx errors by endpoint
api_active_users: Gauge - Concurrent dashboard users
```

**Business Metrics** (Custom):
```yaml
# Retention Monitoring
daily_beneficiaries_monitored: Gauge - Unique beneficiaries with events
daily_credits_processed: Counter - Total credit events
daily_anomalies_detected: Counter - Anomalies by severity
median_retention_window: Gauge - State-wide median retention

# Consent Metrics
consent_rate: Gauge - % of beneficiaries with active consent
consent_withdrawals: Counter - Consent withdrawals per day
consent_renewals: Counter - Consent renewals per day

# Alert Metrics
alerts_triggered: Counter - Alerts by severity
alert_acknowledgment_time: Histogram - Time to acknowledge
alert_resolution_time: Histogram - Time to resolve
false_positive_rate: Gauge - % of alerts marked false positive

# Data Quality
sms_parsing_success_rate: Gauge - % of SMS successfully parsed
extraction_confidence_avg: Gauge - Average extraction confidence
manual_review_queue_size: Gauge - SMS awaiting manual review
```

### 10.2 Logging Strategy

**Log Levels**:
- ERROR: System failures, unrecoverable errors
- WARN: Degraded performance, low confidence predictions, retries
- INFO: Normal operations, event milestones (aggregation complete)
- DEBUG: Detailed processing steps (disabled in production)

**Structured Logging** (JSON format):
```json
{
  "timestamp": "2024-02-01T10:30:45.123Z",
  "level": "INFO",
  "service": "sms-classifier",
  "trace_id": "abc123",
  "message": "SMS classified successfully",
  "context": {
    "sms_id": "uuid-123",
    "scheme_id": "PM_KISAN",
    "confidence": 0.92,
    "processing_time_ms": 87
  }
}
```

**Log Aggregation**:
- Tool: ELK Stack (Elasticsearch, Logstash, Kibana) or Loki
- Retention: 90 days for application logs, 7 years for audit logs
- Indexing: By service, level, timestamp, trace_id

### 10.3 Distributed Tracing

**Tool**: OpenTelemetry + Jaeger

**Trace Spans**:
```
SMS Processing Trace:
├─ sms-ingestion (5ms)
├─ sms-normalization (12ms)
├─ sms-classification (87ms)
│  └─ model-inference (82ms)
├─ entity-extraction (45ms)
│  ├─ regex-extraction (8ms)
│  └─ ner-extraction (37ms)
├─ event-storage (23ms)
│  └─ database-insert (18ms)
└─ kafka-produce (6ms)

Total: 178ms
```

**Trace Context Propagation**:
- HTTP headers: `traceparent`, `tracestate`
- Kafka message headers: `trace_id`, `span_id`
- Database queries: Tagged with trace_id in comments

### 10.4 Alerting Rules

**Critical Alerts** (PagerDuty/Opsgenie):
```yaml
# System Health
- alert: DatabasePrimaryDown
  expr: up{job="postgresql-primary"} == 0
  for: 1m
  severity: critical
  
- alert: KafkaBrokerDown
  expr: kafka_broker_count < 3
  for: 5m
  severity: critical

- alert: HighErrorRate
  expr: rate(http_requests_total{status=~"5.."}[5m]) > 0.05
  for: 5m
  severity: critical

# Model Performance
- alert: ModelInferenceFailure
  expr: rate(model_inference_errors[5m]) > 0.1
  for: 5m
  severity: critical

- alert: ModelAccuracyDegradation
  expr: model_accuracy < 0.80
  for: 1h
  severity: critical
```

**Warning Alerts** (Email/Slack):
```yaml
# Performance Degradation
- alert: HighLatency
  expr: histogram_quantile(0.95, sms_processing_latency) > 2
  for: 10m
  severity: warning

- alert: ConsumerLag
  expr: kafka_consumer_lag > 10000
  for: 15m
  severity: warning

# Data Quality
- alert: LowConfidenceRate
  expr: rate(sms_low_confidence[1h]) > 0.2
  for: 1h
  severity: warning

- alert: ManualReviewBacklog
  expr: manual_review_queue_size > 1000
  for: 1h
  severity: warning
```

### 10.5 Dashboards

**Operational Dashboard** (Grafana):
- SMS processing throughput (messages/sec)
- Processing latency (p50, p95, p99)
- Error rates by component
- Database connection pool usage
- Kafka consumer lag
- Model inference latency
- API response times

**Business Dashboard** (Custom Web UI):
- Daily beneficiaries monitored
- Credit events processed (by scheme)
- Anomalies detected (by severity)
- State-wide retention distribution
- Top 10 districts by anomaly rate
- Consent rate trends
- Alert resolution metrics

**ML Model Dashboard** (MLflow):
- Model versions deployed
- Accuracy/precision/recall trends
- Inference latency by model version
- Feature importance
- Confusion matrix
- Drift detection metrics

### 10.6 Health Checks

**Liveness Probe** (Kubernetes):
```python
@app.route('/health/live')
def liveness():
    """
    Check if service is alive (process running).
    
    Returns:
    - 200 OK: Service is alive
    - 500 Error: Service should be restarted
    """
    return {"status": "alive"}, 200
```

**Readiness Probe** (Kubernetes):
```python
@app.route('/health/ready')
def readiness():
    """
    Check if service is ready to accept traffic.
    
    Checks:
    - Database connection
    - Kafka connection
    - Model loaded
    - Redis connection
    
    Returns:
    - 200 OK: Service is ready
    - 503 Unavailable: Service not ready (remove from load balancer)
    """
    checks = {
        "database": check_database_connection(),
        "kafka": check_kafka_connection(),
        "model": check_model_loaded(),
        "redis": check_redis_connection()
    }
    
    if all(checks.values()):
        return {"status": "ready", "checks": checks}, 200
    else:
        return {"status": "not_ready", "checks": checks}, 503
```

**Startup Probe** (Kubernetes):
```python
@app.route('/health/startup')
def startup():
    """
    Check if service has completed initialization.
    
    Checks:
    - Model downloaded and loaded
    - Database migrations applied
    - Configuration loaded
    
    Returns:
    - 200 OK: Service initialized
    - 503 Unavailable: Still initializing
    """
    if model_loaded and db_migrated and config_loaded:
        return {"status": "started"}, 200
    else:
        return {"status": "starting"}, 503
```


## 11. Correctness Properties

### 11.1 Introduction to Correctness Properties

A property is a characteristic or behavior that should hold true across all valid executions of a system—essentially, a formal statement about what the system should do. Properties serve as the bridge between human-readable specifications and machine-verifiable correctness guarantees.

In this system, correctness properties validate that the DBT retention monitoring engine correctly processes SMS data, detects anomalies, maintains privacy, and enforces access controls across all possible inputs and scenarios. Each property is universally quantified (applies to all valid inputs) and directly traceable to specific requirements.

### 11.2 SMS Processing Properties

**Property 1: SMS Ingestion Completeness**
*For any* SMS received from a registered banking source, the SMS_Ingestion_Module should parse and store the message with all required metadata (timestamp, sender ID, recipient phone hash), and the stored record should be retrievable by its unique ID.
**Validates: Requirements 1.1, 1.4**

**Property 2: Unicode Normalization Preservation**
*For any* regional language SMS text, normalizing to Unicode should produce valid UTF-8 output, and for mixed-language content, all language segments should be present in the normalized output (no data loss).
**Validates: Requirements 1.2, 1.3**

**Property 3: Processing Resilience**
*For any* batch of SMS messages containing both valid and malformed messages, processing should continue for all messages, and malformed messages should be logged with original content while valid messages are processed successfully.
**Validates: Requirements 1.5**

**Property 4: Classification Completeness**
*For any* normalized SMS, the Classification_Model should return a scheme prediction (including "UNKNOWN_SCHEME" for unrecognized patterns) with a confidence score between 0.0 and 1.0.
**Validates: Requirements 2.1, 2.5**

**Property 5: Low Confidence Flagging**
*For any* SMS classification with confidence score below 0.7, the message should be flagged for manual review and appear in the manual review queue.
**Validates: Requirements 2.3**

**Property 6: Entity Extraction Completeness**
*For any* classified SMS, the Extraction_Engine should attempt to extract all specified fields (scheme name, amount, date, reference number, account digits), and each extracted field should have an associated confidence score.
**Validates: Requirements 3.1**

**Property 7: Currency and Date Normalization**
*For any* extracted amount, it should be normalized to standard decimal format (two decimal places), and for any extracted date regardless of input format, it should be converted to ISO 8601 format.
**Validates: Requirements 3.2, 3.3**

**Property 8: Low Confidence Field Handling**
*For any* entity extraction where field confidence is below threshold, the field should be flagged as "LOW_CONFIDENCE" and the original text should be retained in the extraction result.
**Validates: Requirements 3.5**


### 11.3 Event Storage and Correlation Properties

**Property 9: Credit Event Creation**
*For any* successful entity extraction, a Credit_Event record should be created in the Event_Store with all extracted fields (scheme, amount, date, reference) and confidence scores, linked to the beneficiary hash.
**Validates: Requirements 4.1, 4.2**

**Property 10: Event Immutability**
*For any* Credit_Event or Withdrawal_Event stored in the Event_Store, attempting to modify the record should fail, maintaining an immutable audit trail (corrections handled via new events with corrects_event_id).
**Validates: Requirements 4.3**

**Property 11: SMS Deduplication Idempotency**
*For any* SMS with the same reference number processed multiple times, only one Credit_Event should exist in the Event_Store (deduplication based on reference number).
**Validates: Requirements 4.4**

**Property 12: Withdrawal Detection and Correlation**
*For any* debit SMS, it should be identified as a Withdrawal_Event, and if a matching unmatched credit exists for the same beneficiary, the withdrawal should be correlated to the most recent unmatched credit with retention_window calculated.
**Validates: Requirements 5.1, 5.2**

**Property 13: Retention Window Calculation**
*For any* correlated withdrawal, if the time difference is ≤24 hours, retention_window should be expressed in hours, and if >24 hours, it should be expressed in days.
**Validates: Requirements 5.3, 5.4**

**Property 14: Unmatched Withdrawal Handling**
*For any* withdrawal event with no matching credit event for the same beneficiary, the withdrawal should be stored with matched_credit_id as NULL and flagged for investigation.
**Validates: Requirements 5.5**

### 11.4 Anomaly Detection Properties

**Property 15: Immediate Withdrawal Detection**
*For any* correlated credit-withdrawal pair with retention_window < 2 hours, the Anomaly_Detector should flag the event as "IMMEDIATE_WITHDRAWAL" with HIGH severity.
**Validates: Requirements 6.1**

**Property 16: Pattern Anomaly Detection**
*For any* beneficiary with 3 or more consecutive credit events where each has retention_window < 6 hours, the Anomaly_Detector should flag a "PATTERN_ANOMALY" with HIGH severity.
**Validates: Requirements 6.2**

**Property 17: Full Withdrawal Detection**
*For any* correlated credit-withdrawal pair where |withdrawal_amount - credit_amount| ≤ ₹10, the Anomaly_Detector should flag the event as "FULL_WITHDRAWAL" with MEDIUM severity.
**Validates: Requirements 6.3**

**Property 18: Statistical Outlier Detection**
*For any* village with sufficient baseline data (≥100 beneficiaries), retention windows that are outliers (>2 standard deviations from village median) should be flagged by the Anomaly_Detector.
**Validates: Requirements 6.4**

**Property 19: Anomaly Alert Creation**
*For any* detected anomaly, an alert record should be created with the anomaly type, severity level (LOW/MEDIUM/HIGH), and relevant context (beneficiary hash, scheme, village).
**Validates: Requirements 6.5**

### 11.5 Installment Continuity Properties

**Property 20: Missing Installment Detection**
*For any* beneficiary enrolled in a periodic scheme (with defined periodicity), if the expected installment date + grace period (7 days) has passed without a credit event, the Installment_Monitor should flag "MISSING_INSTALLMENT".
**Validates: Requirements 7.2**

**Property 21: Consecutive Missing Installment Escalation**
*For any* beneficiary with 2 or more consecutive missing installments, the severity should escalate from MEDIUM to HIGH.
**Validates: Requirements 7.3**

**Property 22: Delayed Installment Recording**
*For any* installment that arrives after the grace period, the Installment_Monitor should record the delay duration (days overdue) and update the beneficiary record.
**Validates: Requirements 7.4**

**Property 23: Scheme Exit Exclusion**
*For any* beneficiary with a scheme exit record, the Installment_Monitor should not perform continuity checks for that scheme.
**Validates: Requirements 7.5**

### 11.6 Voice Grievance Processing Properties

**Property 24: Voice Transcription and Language Detection**
*For any* submitted voice audio file, the ASR_Module should produce a transcript and detect the language, returning both with confidence scores.
**Validates: Requirements 8.1**

**Property 25: Grievance Intent Classification**
*For any* completed transcription, the Intent_Classifier should categorize the grievance into one of the defined categories (payment_delay, wrong_amount, coercion, account_issue, scheme_query, other).
**Validates: Requirements 8.3**

**Property 26: Grievance Entity Extraction**
*For any* grievance transcript, the Grievance_Extractor should attempt to extract relevant entities (scheme name, amount disputed, date of issue, location) with confidence scores.
**Validates: Requirements 8.4**

**Property 27: Grievance-Event Correlation**
*For any* grievance that mentions a specific credit event (by scheme, amount, or date), the Grievance_Linker should attempt to correlate it with matching Credit_Event records in the Event_Store.
**Validates: Requirements 8.5**


### 11.7 Aggregation and Rollup Properties

**Property 28: Village Metrics Calculation**
*For any* village and date range, the Aggregation_Pipeline should compute complete metrics including scheme-wise distribution (percentages summing to 100%), retention window distribution (counts across all bins), and top anomaly patterns by frequency.
**Validates: Requirements 9.2, 9.3, 9.4**

**Property 29: Aggregation Metadata Storage**
*For any* computed aggregation (village/block/district/state level), the stored metrics should include a timestamp and data freshness indicator showing when the aggregation was computed.
**Validates: Requirements 9.5**

**Property 30: Hierarchical Rollup Correctness**
*For any* set of village metrics, rolling up to block level should correctly compute sum (for counts), mean, median, and percentiles, and the same rollup logic should apply from block→district and district→state.
**Validates: Requirements 10.1, 10.2, 10.3**

**Property 31: Inter-Region Variance Detection**
*For any* completed district-level aggregation, the Aggregation_Pipeline should compute inter-block variance, and blocks with variance >2 standard deviations from district mean should be flagged as high-variance.
**Validates: Requirements 10.4**

### 11.8 Consent and Privacy Properties

**Property 32: Consent-Gated Processing**
*For any* beneficiary, SMS data processing should only occur if an active consent record exists (consent_status = ACTIVE and consent_expiry > current_date).
**Validates: Requirements 11.1**

**Property 33: Consent Withdrawal Effect**
*For any* beneficiary who withdraws consent, future SMS processing should immediately stop, and existing beneficiary-level data should be anonymized (beneficiary_hash re-hashed) while preserving aggregated statistics.
**Validates: Requirements 11.2, 11.3**

**Property 34: Consent Audit Trail**
*For any* consent action (grant, withdrawal, renewal), an audit record should be created with timestamp, consent mode (SMS/voice/written/digital), and stored evidence reference.
**Validates: Requirements 11.4**

**Property 35: Consent Expiry and Renewal**
*For any* consent record where consent_expiry < current_date, the consent_status should be set to EXPIRED, and a renewal request should be sent to the beneficiary.
**Validates: Requirements 11.5**

**Property 36: Beneficiary Identifier Hashing**
*For any* beneficiary phone number, before storage it should be hashed using salted SHA-256, and the hash should be deterministic (same phone → same hash) but irreversible (cannot recover phone from hash).
**Validates: Requirements 12.1**

**Property 37: Pseudonymization in Reports**
*For any* aggregated report or export, beneficiary names should be replaced with pseudonymous identifiers, and no direct identifiers (phone numbers, Aadhaar) should appear.
**Validates: Requirements 12.2**

**Property 38: K-Anonymity for Research Exports**
*For any* data exported for research purposes, k-anonymity with k≥5 should be applied, ensuring each combination of quasi-identifiers appears at least 5 times or is suppressed/generalized.
**Validates: Requirements 12.3**

**Property 39: Quasi-Identifier Removal**
*For any* anonymization operation, quasi-identifiers (exact GPS coordinates, precise timestamps beyond day granularity) should be removed or generalized to prevent re-identification.
**Validates: Requirements 12.4**

**Property 40: Small Cell Suppression**
*For any* aggregation cell (village-scheme combination) with fewer than 5 beneficiaries, the metric should be suppressed (not displayed) to prevent inference attacks.
**Validates: Requirements 12.5**

### 11.9 Access Control and Security Properties

**Property 41: Role-Based Access Enforcement**
*For any* data access request, the Access_Control_Module should enforce permissions based on user role and jurisdiction, denying access to resources outside the user's assigned scope.
**Validates: Requirements 13.1**

**Property 42: Village-Level Access Restriction**
*For any* user with role VILLAGE_OFFICER, data access should be restricted to only the assigned village(s), and attempts to access other villages should be denied.
**Validates: Requirements 13.2**

**Property 43: Hierarchical Access Inheritance**
*For any* user with role DISTRICT_OFFICER, data access should be granted to all blocks and villages within the assigned district (hierarchical access).
**Validates: Requirements 13.3**

**Property 44: Access Audit Logging**
*For any* data access attempt (successful or denied), an audit log entry should be created with user_id, timestamp, resource_type, resource_id, action, and granted status.
**Validates: Requirements 13.4**

**Property 45: Unauthorized Access Denial and Alerting**
*For any* unauthorized access attempt (user accessing resource outside jurisdiction), the request should be denied (403 Forbidden), and an alert should be sent to the administrator.
**Validates: Requirements 13.5**

**Property 46: Alert Content Completeness**
*For any* generated alert, it should include all required anomaly details (beneficiary pseudonym, scheme, retention window, village, severity, context).
**Validates: Requirements 14.3**

**Property 47: Alert Acknowledgment Tracking and Escalation**
*For any* delivered alert, the Alert_System should track acknowledgment status, and if a HIGH severity alert remains unacknowledged for 4 hours, it should be escalated to the next administrative level.
**Validates: Requirements 14.4**

**Property 48: Low Severity Alert Batching**
*For any* LOW severity alert, it should be batched into a daily digest report rather than sent immediately.
**Validates: Requirements 14.5**


### 11.10 Offline Synchronization Properties

**Property 49: Offline Queueing**
*For any* SMS data collected when network is unavailable, the Offline_Module should queue the data locally in persistent storage for later synchronization.
**Validates: Requirements 16.1**

**Property 50: Automatic Synchronization on Reconnection**
*For any* queued data when network connectivity is restored, the Offline_Module should automatically synchronize all queued data to the central server.
**Validates: Requirements 16.2**

**Property 51: Conflict Resolution Consistency**
*For any* synchronization conflict (same event modified locally and on server), the Offline_Module should resolve using server-side timestamp precedence (server version wins).
**Validates: Requirements 16.3**

**Property 52: Data Compression Before Transmission**
*For any* data transmitted during synchronization, it should be compressed before transmission to minimize bandwidth usage.
**Validates: Requirements 16.4**

**Property 53: Sync Failure Handling**
*For any* synchronization attempt that fails 3 times with exponential backoff, the Offline_Module should alert the user and preserve the data locally for manual intervention.
**Validates: Requirements 16.5**

### 11.11 Audit and Compliance Properties

**Property 54: Deletion Audit Trail**
*For any* data deletion operation (beneficiary data, consent withdrawal, retention policy), an audit log entry should be created and maintained for compliance verification.
**Validates: Requirements 17.5**

**Property 55: API Authentication Enforcement**
*For any* API request to the API_Gateway, OAuth 2.0 authentication with valid JWT token should be required, and requests without valid authentication should be rejected (401 Unauthorized).
**Validates: Requirements 18.2**

**Property 56: API Rate Limiting**
*For any* API client, the API_Gateway should enforce rate limiting of 1000 requests per hour, and requests exceeding this limit should be rejected (429 Too Many Requests).
**Validates: Requirements 18.3**

**Property 57: API Response Format**
*For any* successful API request, the response should be in JSON format with pagination support (page number, page size, total count) for list endpoints.
**Validates: Requirements 18.4**

**Property 58: API Date Range Validation**
*For any* API request including a date range parameter, the API_Gateway should validate that the range does not exceed 12 months, and reject requests with larger ranges (400 Bad Request).
**Validates: Requirements 18.5**

**Property 59: Comprehensive Access Logging**
*For any* data access event, the Audit_Logger should create a log entry with user_id, timestamp, resource_type, resource_id, and action performed.
**Validates: Requirements 19.1**

**Property 60: Data Modification Logging**
*For any* data modification operation (INSERT, UPDATE, DELETE), the Audit_Logger should create a log entry with entity type, operation, before/after values (for UPDATE), and user_id.
**Validates: Requirements 19.2**

**Property 61: Authentication Event Logging**
*For any* authentication event (login, logout, failed login attempt), the Audit_Logger should create a log entry with user_id, timestamp, event type, and IP address.
**Validates: Requirements 19.3**

**Property 62: Anomaly Detection Logging**
*For any* detected anomaly, the Audit_Logger should create a log entry with full context (beneficiary hash, scheme, metrics, detection method, evidence).
**Validates: Requirements 19.4**

**Property 63: Tamper-Evident Log Storage**
*For any* audit log entry, it should be stored in tamper-evident format with cryptographic integrity verification (hash chain or digital signature), and tampering attempts should be detectable.
**Validates: Requirements 19.5**

### 11.12 System Resilience Properties

**Property 64: Threshold-Based Health Alerting**
*For any* monitored metric (latency, error rate, throughput) that exceeds its configured threshold, the Health_Monitor should trigger an alert to the operations team.
**Validates: Requirements 20.4**

**Property 65: Durable SMS Persistence**
*For any* incoming SMS, it should be persisted to a durable queue (Kafka) before processing begins, ensuring no data loss even if processing fails.
**Validates: Requirements 22.4**

**Property 66: Database Unavailability Resilience**
*For any* write operation when the database is unavailable, the DBT_System should queue the write locally and retry with exponential backoff until the database is available or maximum retries are reached.
**Validates: Requirements 22.5**

### 11.13 Encryption and Security Properties

**Property 67: Data-at-Rest Encryption**
*For any* data stored in the database or object storage, it should be encrypted using AES-256 encryption, and unencrypted data should not be accessible from storage media.
**Validates: Requirements 23.1**

**Property 68: Data-in-Transit Encryption**
*For any* network communication (API calls, database connections, inter-service communication), TLS 1.3 should be used, and unencrypted connections should be rejected.
**Validates: Requirements 23.2**

**Property 69: Certificate Pinning for API Clients**
*For any* API client connection, certificate pinning should be implemented, and connections with non-pinned certificates should be rejected to prevent man-in-the-middle attacks.
**Validates: Requirements 23.4**

### 11.14 Multi-Lingual and Accessibility Properties

**Property 70: Mixed-Script SMS Handling**
*For any* SMS containing mixed scripts (e.g., Hindi text with English numerals, Tamil with Arabic numerals), the Extraction_Engine should correctly extract entities from all script segments.
**Validates: Requirements 24.3**

**Property 71: UI Language Selection**
*For any* user selecting a language preference in the dashboard, the UI should display in the selected language (minimum Hindi and English supported).
**Validates: Requirements 24.4**

**Property 72: Intermittent Connectivity Operation**
*For any* period of intermittent network connectivity, the DBT_System should continue core operations (SMS collection, local processing, queueing) and synchronize when connectivity is available.
**Validates: Requirements 25.3**

### 11.15 Explainability and Transparency Properties

**Property 73: Anomaly Explanation Availability**
*For any* flagged anomaly, the system should provide a model decision explanation (detection method, evidence, contributing factors) that can be retrieved by authorized users.
**Validates: Requirements 27.4**


## 12. Error Handling Strategy

### 12.1 Error Classification

**Transient Errors** (Retry with exponential backoff):
- Network timeouts
- Database connection failures
- External service unavailability (ASR API, Kafka broker)
- Temporary resource exhaustion (connection pool full)

**Permanent Errors** (Log and move to dead letter queue):
- Malformed SMS that cannot be parsed
- Invalid data format (non-numeric amount, unparseable date)
- Authentication failures (invalid credentials)
- Authorization failures (insufficient permissions)

**Degraded Mode Errors** (Fallback to alternative method):
- Low classification confidence → Rule-based classification
- NER extraction failure → Regex-only extraction
- Primary ASR service down → Fallback to secondary ASR
- Cache miss → Query database directly

### 12.2 Error Recovery Patterns

**Circuit Breaker Pattern**:
- Applied to: ASR service, external APIs, database connections
- Threshold: 5 consecutive failures
- Open duration: 5 minutes
- Half-open test: Single request to check recovery

**Retry with Exponential Backoff**:
- Applied to: Network requests, database writes, Kafka produces
- Initial delay: 1 second
- Backoff multiplier: 2x
- Maximum delay: 60 seconds
- Maximum retries: 3 attempts

**Dead Letter Queue**:
- Applied to: Kafka message processing failures
- Retention: 30 days
- Manual review: Admin dashboard for DLQ inspection
- Reprocessing: Manual trigger after issue resolution

### 12.3 Graceful Degradation

**SMS Processing**:
- Primary: ML classification → NER extraction
- Fallback 1: Rule-based classification → Regex extraction
- Fallback 2: Store as UNKNOWN_SCHEME with raw text
- Fallback 3: Manual review queue

**Voice Processing**:
- Primary: Bhashini ASR → Intent classification
- Fallback 1: Whisper ASR → Intent classification
- Fallback 2: Store audio with "TRANSCRIPTION_FAILED" status
- Fallback 3: Manual transcription queue

**Dashboard Queries**:
- Primary: Redis cache
- Fallback 1: Database read replica
- Fallback 2: Database primary
- Fallback 3: Return cached stale data with warning

### 12.4 Error Monitoring and Alerting

**Error Rate Thresholds**:
- SMS parsing errors >5% → Alert ML team
- Classification confidence <0.7 for >20% → Alert ML team
- Database connection failures >10/minute → Alert DevOps
- API error rate >5% → Alert DevOps
- Kafka consumer lag >10,000 messages → Alert DevOps

**Error Dashboards**:
- Real-time error rate by component
- Error distribution by type (transient, permanent, degraded)
- Dead letter queue size and age
- Manual review queue size and age
- Circuit breaker status (open/closed/half-open)


## 13. Testing Strategy

### 13.1 Testing Approach Overview

The DBT Retention Monitoring Engine requires a comprehensive testing strategy that combines unit tests, property-based tests, integration tests, and end-to-end tests. Given the critical nature of government infrastructure and the complexity of ML-based processing, testing must ensure both functional correctness and operational reliability.

**Testing Pyramid**:
```
                    ▲
                   / \
                  /   \
                 /  E2E \
                /  Tests \
               /-----------\
              /             \
             /  Integration  \
            /     Tests       \
           /-------------------\
          /                     \
         /   Property-Based      \
        /        Tests            \
       /---------------------------\
      /                             \
     /          Unit Tests           \
    /_______________________________\
```

### 13.2 Unit Testing

**Scope**: Individual functions, classes, and modules in isolation.

**Focus Areas**:
- SMS parsing logic (regex patterns, format handling)
- Entity extraction functions (amount, date, reference parsing)
- Hashing and anonymization functions
- Access control permission checks
- Aggregation calculation functions
- Error handling edge cases

**Example Unit Tests**:
```python
def test_parse_amount_standard_format():
    """Test amount parsing with standard format."""
    text = "Rs. 2,000.00 credited"
    result = parse_amount(text)
    assert result == Decimal("2000.00")

def test_parse_amount_no_decimals():
    """Test amount parsing without decimal places."""
    text = "₹2000 credited"
    result = parse_amount(text)
    assert result == Decimal("2000.00")

def test_hash_beneficiary_deterministic():
    """Test that hashing is deterministic."""
    phone = "9876543210"
    salt = b"test_salt"
    hash1 = hash_beneficiary_id(phone, salt)
    hash2 = hash_beneficiary_id(phone, salt)
    assert hash1 == hash2

def test_hash_beneficiary_different_phones():
    """Test that different phones produce different hashes."""
    salt = b"test_salt"
    hash1 = hash_beneficiary_id("9876543210", salt)
    hash2 = hash_beneficiary_id("9876543211", salt)
    assert hash1 != hash2

def test_access_control_village_officer_restriction():
    """Test village officer can only access assigned villages."""
    user = User(role="VILLAGE_OFFICER", jurisdiction_ids=["VIL001"])
    assert check_permission(user, "VILLAGE_METRICS", "VIL001", "READ") == True
    assert check_permission(user, "VILLAGE_METRICS", "VIL002", "READ") == False
```

**Unit Test Configuration**:
- Framework: pytest (Python), Jest (TypeScript/JavaScript), JUnit (Java)
- Coverage target: >80% line coverage for core logic
- Mocking: Use mocks for external dependencies (database, Kafka, APIs)
- Execution: Run on every commit (CI/CD pipeline)

### 13.3 Property-Based Testing

**Scope**: Universal properties that should hold for all valid inputs.

**Framework**: Hypothesis (Python), fast-check (TypeScript), QuickCheck (Haskell-style)

**Configuration**:
- Minimum iterations: 100 per property test
- Shrinking: Enabled (find minimal failing example)
- Seed: Randomized (different inputs each run)
- Timeout: 60 seconds per property

**Property Test Examples**:

```python
from hypothesis import given, strategies as st

@given(st.text(min_size=10, max_size=500))
def test_property_1_sms_ingestion_completeness(sms_text):
    """
    Property 1: SMS Ingestion Completeness
    Feature: dbt-retention-monitoring, Property 1
    
    For any SMS text, ingestion should create a record with all metadata.
    """
    sender_id = "BANK01"
    recipient = "9876543210"
    timestamp = datetime.now()
    
    record = sms_ingestion_module.ingest_sms(
        message_text=sms_text,
        sender_id=sender_id,
        recipient_phone=recipient,
        timestamp=timestamp
    )
    
    assert record.id is not None
    assert record.message_text == sms_text
    assert record.sender_id == sender_id
    assert record.recipient_phone_hash == hash_beneficiary_id(recipient, SALT)
    assert record.received_at == timestamp

@given(st.text(alphabet=st.characters(categories=['L', 'N', 'P'])))
def test_property_2_unicode_normalization_preservation(regional_text):
    """
    Property 2: Unicode Normalization Preservation
    Feature: dbt-retention-monitoring, Property 2
    
    For any regional language text, normalization should preserve all content.
    """
    normalized = sms_normalization_engine.normalize(regional_text)
    
    # Check valid UTF-8
    assert normalized.text.encode('utf-8')
    
    # Check no data loss (all characters present in some form)
    # This is a simplified check; real implementation would be more sophisticated
    assert len(normalized.text) > 0 if len(regional_text) > 0 else True

@given(
    st.decimals(min_value=1, max_value=100000, places=2),
    st.datetimes(min_value=datetime(2020, 1, 1), max_value=datetime(2025, 12, 31))
)
def test_property_11_sms_deduplication_idempotency(amount, date):
    """
    Property 11: SMS Deduplication Idempotency
    Feature: dbt-retention-monitoring, Property 11
    
    For any SMS with same reference number, only one credit event should exist.
    """
    reference = f"REF{random.randint(1000000, 9999999)}"
    beneficiary = hash_beneficiary_id("9876543210", SALT)
    
    # Process same SMS twice
    sms_text = f"Rs {amount} credited on {date.strftime('%d-%m-%Y')} Ref: {reference}"
    
    event1 = process_sms_to_credit_event(sms_text, beneficiary)
    event2 = process_sms_to_credit_event(sms_text, beneficiary)
    
    # Query database for events with this reference
    events = query_credit_events_by_reference(reference)
    
    assert len(events) == 1, "Duplicate SMS should result in single event"

@given(
    st.integers(min_value=0, max_value=23),  # retention in hours
    st.booleans()  # whether it's within 24 hours
)
def test_property_13_retention_window_calculation(hours, within_24h):
    """
    Property 13: Retention Window Calculation
    Feature: dbt-retention-monitoring, Property 13
    
    For any correlated withdrawal, retention window should be in correct units.
    """
    credit_time = datetime.now()
    
    if within_24h:
        withdrawal_time = credit_time + timedelta(hours=hours)
    else:
        withdrawal_time = credit_time + timedelta(days=2, hours=hours)
    
    retention = calculate_retention_window(credit_time, withdrawal_time)
    
    if within_24h:
        assert retention.unit == "hours"
        assert retention.value == hours
    else:
        assert retention.unit == "days"
        assert retention.value >= 2

@given(st.integers(min_value=0, max_value=10))
def test_property_15_immediate_withdrawal_detection(retention_hours):
    """
    Property 15: Immediate Withdrawal Detection
    Feature: dbt-retention-monitoring, Property 15
    
    For any retention window < 2 hours, immediate withdrawal should be flagged.
    """
    credit_event = create_test_credit_event()
    withdrawal_event = create_test_withdrawal_event(
        credit_event,
        retention_hours=retention_hours
    )
    
    anomalies = anomaly_detector.detect_anomalies(
        correlation_result=CorrelationResult(
            credit_event_id=credit_event.id,
            retention_window_hours=retention_hours
        ),
        beneficiary_history=[credit_event]
    )
    
    if retention_hours < 2:
        assert any(a.anomaly_type == "IMMEDIATE_WITHDRAWAL" for a in anomalies)
        assert any(a.severity == "HIGH" for a in anomalies)
    else:
        assert not any(a.anomaly_type == "IMMEDIATE_WITHDRAWAL" for a in anomalies)

@given(st.lists(st.integers(min_value=0, max_value=10), min_size=3, max_size=10))
def test_property_16_pattern_anomaly_detection(retention_windows):
    """
    Property 16: Pattern Anomaly Detection
    Feature: dbt-retention-monitoring, Property 16
    
    For any beneficiary with 3+ consecutive short-retention credits, pattern anomaly flagged.
    """
    beneficiary_hash = hash_beneficiary_id("9876543210", SALT)
    credit_events = []
    
    for i, retention in enumerate(retention_windows):
        credit = create_test_credit_event(beneficiary_hash, event_id=f"credit_{i}")
        withdrawal = create_test_withdrawal_event(credit, retention_hours=retention)
        credit_events.append((credit, retention))
    
    # Check if there are 3+ consecutive events with retention < 6 hours
    consecutive_short = 0
    max_consecutive = 0
    for _, retention in credit_events:
        if retention < 6:
            consecutive_short += 1
            max_consecutive = max(max_consecutive, consecutive_short)
        else:
            consecutive_short = 0
    
    anomalies = anomaly_detector.detect_pattern_anomalies(beneficiary_hash)
    
    if max_consecutive >= 3:
        assert any(a.anomaly_type == "PATTERN_ANOMALY" for a in anomalies)
        assert any(a.severity == "HIGH" for a in anomalies)
    else:
        assert not any(a.anomaly_type == "PATTERN_ANOMALY" for a in anomalies)
```

**Property Test Coverage**:
- All 73 correctness properties from Section 11 should have corresponding property tests
- Each test should reference its property number in a comment
- Tests should use realistic data generators (Indian phone numbers, DBT schemes, regional languages)


### 13.4 Integration Testing

**Scope**: Interactions between components, database operations, message queue processing.

**Focus Areas**:
- SMS ingestion → Kafka → Processing pipeline → Database storage
- Classification model → Entity extraction → Event creation
- Withdrawal correlation → Anomaly detection → Alert generation
- Aggregation pipeline → Cache population → Dashboard queries
- Consent management → Data anonymization → Access control
- Offline module → Synchronization → Conflict resolution

**Example Integration Tests**:
```python
def test_integration_sms_to_credit_event():
    """Test complete flow from SMS ingestion to credit event creation."""
    # Setup
    sms_text = "Rs 2000 credited to A/c XX1234 under PM-KISAN on 15-01-2024 Ref: REF1234567"
    sender_id = "SBIINB"
    recipient = "9876543210"
    
    # Execute
    sms_record = sms_ingestion_module.ingest_sms(sms_text, sender_id, recipient, datetime.now())
    normalized = sms_normalization_engine.normalize(sms_record.message_text)
    classification = sms_classifier.predict_scheme(normalized.text, "en")
    entities = entity_extractor.extract(normalized.text, classification.scheme_id)
    credit_event = event_store.create_credit_event(
        beneficiary_hash=hash_beneficiary_id(recipient, SALT),
        scheme_id=classification.scheme_id,
        amount=entities.amount,
        date=entities.date,
        reference=entities.reference
    )
    
    # Verify
    assert credit_event.scheme_id == "PM_KISAN"
    assert credit_event.amount == Decimal("2000.00")
    assert credit_event.reference_number == "REF1234567"
    
    # Verify database persistence
    retrieved = event_store.get_credit_event(credit_event.event_id)
    assert retrieved.event_id == credit_event.event_id

def test_integration_withdrawal_correlation_and_anomaly():
    """Test withdrawal correlation and anomaly detection flow."""
    # Setup: Create credit event
    beneficiary = hash_beneficiary_id("9876543210", SALT)
    credit_event = event_store.create_credit_event(
        beneficiary_hash=beneficiary,
        scheme_id="PM_KISAN",
        amount=Decimal("2000.00"),
        date=datetime.now(),
        reference="REF123"
    )
    
    # Execute: Create withdrawal 1 hour later
    withdrawal_time = credit_event.transaction_timestamp + timedelta(hours=1)
    withdrawal_event = event_store.create_withdrawal_event(
        beneficiary_hash=beneficiary,
        amount=Decimal("2000.00"),
        timestamp=withdrawal_time
    )
    
    # Correlate
    correlation = withdrawal_correlator.correlate_withdrawal(withdrawal_event)
    
    # Detect anomalies
    anomalies = anomaly_detector.detect_anomalies(
        correlation_result=correlation,
        beneficiary_history=[credit_event]
    )
    
    # Verify
    assert correlation.credit_event_id == credit_event.event_id
    assert correlation.retention_window_hours == 1
    assert len(anomalies) >= 2  # IMMEDIATE_WITHDRAWAL and FULL_WITHDRAWAL
    assert any(a.anomaly_type == "IMMEDIATE_WITHDRAWAL" for a in anomalies)
    assert any(a.anomaly_type == "FULL_WITHDRAWAL" for a in anomalies)

def test_integration_aggregation_pipeline():
    """Test complete aggregation pipeline from events to cached metrics."""
    # Setup: Create multiple credit/withdrawal events for a village
    village_id = "VIL001"
    beneficiaries = [hash_beneficiary_id(f"987654{i:04d}", SALT) for i in range(100)]
    
    for beneficiary in beneficiaries:
        credit = event_store.create_credit_event(
            beneficiary_hash=beneficiary,
            scheme_id=random.choice(["PM_KISAN", "MGNREGA", "OLD_AGE_PENSION"]),
            amount=Decimal(random.randint(500, 5000)),
            date=date.today()
        )
        # Some with withdrawals
        if random.random() < 0.7:
            withdrawal_hours = random.randint(1, 168)  # 1 hour to 1 week
            event_store.create_withdrawal_event(
                beneficiary_hash=beneficiary,
                amount=credit.amount * Decimal(random.uniform(0.5, 1.0)),
                timestamp=credit.transaction_timestamp + timedelta(hours=withdrawal_hours)
            )
    
    # Execute aggregation
    aggregation_pipeline.run_daily_aggregation(date.today())
    
    # Verify village metrics
    metrics = get_village_metrics(village_id, date.today())
    assert metrics.total_credits == 100
    assert metrics.unique_beneficiaries == 100
    assert metrics.median_retention_hours > 0
    assert sum(metrics.scheme_distribution.values()) == pytest.approx(100.0, abs=0.1)
    
    # Verify cache population
    cached_metrics = redis_cache.get(f"village_metrics:{village_id}:{date.today()}")
    assert cached_metrics is not None
    assert cached_metrics["total_credits"] == 100

def test_integration_consent_withdrawal_anonymization():
    """Test consent withdrawal triggers data anonymization."""
    # Setup: Create beneficiary with consent and events
    phone = "9876543210"
    beneficiary_hash = hash_beneficiary_id(phone, SALT)
    
    consent_manager.record_consent(phone, "CONSENT123", granted=True, evidence=None)
    
    credit_event = event_store.create_credit_event(
        beneficiary_hash=beneficiary_hash,
        scheme_id="PM_KISAN",
        amount=Decimal("2000.00"),
        date=date.today(),
        reference="REF123"
    )
    
    # Execute consent withdrawal
    consent_manager.withdraw_consent(beneficiary_hash)
    
    # Verify anonymization
    consent_record = consent_manager.get_consent(beneficiary_hash)
    assert consent_record.consent_status == "WITHDRAWN"
    
    # Verify beneficiary_hash is re-hashed (anonymized)
    retrieved_event = event_store.get_credit_event(credit_event.event_id)
    assert retrieved_event.beneficiary_hash != beneficiary_hash  # Re-hashed
    
    # Verify aggregated stats preserved
    village_metrics = get_village_metrics("VIL001", date.today())
    assert village_metrics.total_credits >= 1  # Aggregates still exist
```

**Integration Test Configuration**:
- Framework: pytest with fixtures for database/Kafka setup
- Database: Use test database with migrations applied
- Kafka: Use testcontainers for isolated Kafka instance
- Execution: Run on every pull request (CI/CD pipeline)
- Cleanup: Tear down test data after each test

### 13.5 End-to-End Testing

**Scope**: Complete user workflows from SMS submission to dashboard visualization.

**Test Scenarios**:

1. **Happy Path: Credit → Withdrawal → Dashboard**
   - Submit credit SMS
   - Submit withdrawal SMS 48 hours later
   - Verify correlation
   - Verify no anomalies
   - Verify dashboard shows correct retention

2. **Anomaly Detection Path**
   - Submit credit SMS
   - Submit withdrawal SMS 1 hour later (immediate withdrawal)
   - Verify anomaly flagged
   - Verify alert generated
   - Verify alert appears in admin dashboard

3. **Consent Withdrawal Path**
   - Submit consent grant
   - Process SMS data
   - Submit consent withdrawal
   - Verify data anonymized
   - Verify future SMS not processed

4. **Offline Sync Path**
   - Simulate network disconnection
   - Collect SMS data offline
   - Restore network connection
   - Verify automatic synchronization
   - Verify data appears in dashboard

5. **Multi-Lingual Path**
   - Submit SMS in Hindi
   - Verify classification
   - Verify entity extraction
   - Submit SMS in Tamil
   - Verify same processing quality

**E2E Test Configuration**:
- Framework: Selenium/Playwright for UI testing, REST API clients for backend
- Environment: Staging environment with production-like configuration
- Data: Synthetic test data (no real beneficiary information)
- Execution: Nightly runs, before production deployments
- Monitoring: Record test execution videos, capture screenshots on failure

### 13.6 Performance Testing

**Load Testing**:
- Tool: Locust or k6
- Scenario: Simulate 10,000 SMS/minute ingestion
- Metrics: Throughput, latency (p50, p95, p99), error rate
- Target: <2s processing latency at p95, <1% error rate

**Stress Testing**:
- Scenario: Gradually increase load until system degrades
- Goal: Identify breaking point and bottlenecks
- Metrics: Maximum sustainable throughput, resource utilization

**Soak Testing**:
- Scenario: Run at 70% capacity for 24 hours
- Goal: Identify memory leaks, resource exhaustion
- Metrics: Memory usage over time, connection pool leaks

### 13.7 Security Testing

**Penetration Testing**:
- SQL injection attempts on API endpoints
- Authentication bypass attempts
- Authorization escalation attempts (village officer accessing district data)
- Data exfiltration attempts

**Vulnerability Scanning**:
- Tool: OWASP ZAP, Snyk
- Frequency: Weekly automated scans
- Scope: API endpoints, dependencies, container images

**Compliance Testing**:
- DPDP Act 2023 compliance verification
- Consent management workflow validation
- Data deletion request handling
- Audit log completeness

### 13.8 ML Model Testing

**Model Accuracy Testing**:
- Held-out test set: 20% of labeled data
- Metrics: Accuracy, precision, recall, F1 score
- Per-language evaluation: Ensure no language bias
- Confusion matrix analysis: Identify commonly confused schemes

**Model Robustness Testing**:
- Adversarial examples: Slightly modified SMS
- Out-of-distribution inputs: Non-DBT SMS
- Noisy inputs: SMS with typos, special characters
- Edge cases: Very short SMS, very long SMS

**Model Drift Detection**:
- Weekly evaluation on recent manually reviewed samples
- Alert if accuracy drops >5% from baseline
- Trigger retraining if accuracy drops >10%

### 13.9 Test Data Management

**Synthetic Data Generation**:
- Generate realistic SMS in multiple languages
- Generate realistic beneficiary profiles (village, scheme enrollment)
- Generate realistic transaction patterns (normal and anomalous)
- Use Faker library with Indian locale

**Test Data Privacy**:
- Never use real beneficiary data in tests
- Anonymize any production data used for model training
- Separate test database from production
- Automated cleanup of test data after execution

### 13.10 Continuous Testing

**CI/CD Pipeline**:
```
On Commit:
  → Unit tests (5 minutes)
  → Linting and static analysis (2 minutes)
  → Build container images (3 minutes)

On Pull Request:
  → Unit tests
  → Property-based tests (15 minutes)
  → Integration tests (20 minutes)
  → Security scanning (5 minutes)

On Merge to Main:
  → All above tests
  → Deploy to staging
  → E2E tests (30 minutes)
  → Performance smoke tests (10 minutes)

Nightly:
  → Full E2E test suite (2 hours)
  → Load testing (1 hour)
  → Model accuracy evaluation (30 minutes)

Weekly:
  → Soak testing (24 hours)
  → Penetration testing (4 hours)
  → Compliance testing (2 hours)
```

**Test Metrics and Reporting**:
- Test coverage: Track line coverage, branch coverage
- Test execution time: Monitor for slow tests
- Flaky tests: Identify and fix non-deterministic tests
- Test failure trends: Track failure rates over time

### 13.11 Testing Best Practices

1. **Test Isolation**: Each test should be independent and not rely on other tests
2. **Deterministic Tests**: Avoid time-dependent or random behavior (use fixed seeds)
3. **Fast Feedback**: Unit tests should run in <5 minutes
4. **Clear Assertions**: Use descriptive assertion messages
5. **Test Documentation**: Each test should have a docstring explaining what it tests
6. **Property Test Tagging**: Tag each property test with feature name and property number
7. **Continuous Improvement**: Regularly review and refactor tests
8. **Test Coverage Goals**: Maintain >80% coverage for core logic, >60% overall


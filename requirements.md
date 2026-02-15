# Requirements Document: DBT Leakage & Retention Monitoring Engine

## 1. Executive Summary

The DBT Leakage & Retention Monitoring Engine is an AI-based infrastructure layer designed to convert unstructured beneficiary communication data into structured retention analytics for India's Direct Benefit Transfer ecosystem. The system processes SMS notifications and optional voice grievances to detect scheme enrollment, track withdrawal patterns, identify installment continuity gaps, and aggregate insights from beneficiary level to state-level dashboards. This system addresses a critical governance gap in the ₹6+ lakh crore DBT infrastructure by enabling real-time monitoring of beneficiary-level retention behavior and potential leakage patterns.

## 2. Problem Definition

### 2.1 Scale of DBT Operations

India's DBT ecosystem operates at unprecedented scale:
- Annual disbursement: >₹6 lakh crore (>$72 billion USD)
- Active schemes: 300+ central and state programs
- Beneficiary base: 500+ million individuals
- Transaction volume: Billions of transfers annually

### 2.2 Current Monitoring Gap

Existing DBT infrastructure tracks:
- Funds released from government accounts
- Funds credited to beneficiary bank accounts
- Basic transaction success/failure metrics

Critical gaps in current monitoring:
- **No beneficiary-level retention tracking**: Systems cannot identify if credited funds remain with intended beneficiaries
- **Immediate withdrawal pattern blindness**: Assisted or coerced withdrawals within minutes/hours of credit go undetected
- **Installment continuity gaps**: Missing periodic payments (pensions, scholarships) are not systematically flagged
- **Unstructured grievance data**: Voice complaints remain unanalyzed and disconnected from transaction data
- **Aggregation deficit**: No hierarchical rollup from beneficiary → village → district → state for pattern detection

### 2.3 Governance Impact

The monitoring gap creates:
- Inability to detect systematic leakage patterns
- Delayed response to beneficiary distress
- Limited evidence base for policy intervention
- Reduced accountability at implementation levels
- Suboptimal scheme effectiveness measurement

## 3. System Scope

### 3.1 In-Scope

- SMS-based DBT credit notification processing
- Automated scheme detection and classification
- Structured entity extraction (scheme name, amount, date, bank reference)
- Withdrawal timing correlation with credit events
- Periodic installment expectation modeling
- Anomaly detection for suspicious withdrawal patterns
- Optional voice grievance processing (ASR + intent classification)
- Hierarchical aggregation (beneficiary → village → block → district → state)
- Privacy-preserving analytics with consent management
- Multi-lingual support (Hindi, English, regional languages)
- Offline-first architecture for low-connectivity environments

### 3.2 Out-of-Scope

- Direct integration with banking core systems
- Real-time transaction interception or blocking
- Beneficiary identity verification (assumes existing Aadhaar/eKYC)
- Scheme enrollment or eligibility determination
- Fund disbursement or payment processing
- Legal enforcement or punitive actions
- Replacement of existing PFMS/DBT portals

### 3.3 Non-Goals

- Building a new DBT payment infrastructure
- Creating a beneficiary-facing mobile application
- Providing financial services or banking features
- Implementing blockchain or distributed ledger technology
- Real-time fraud prevention (system is monitoring/analytics focused)

## 4. Stakeholders

### 4.1 Primary Stakeholders

**Rural DBT Beneficiary**
- Role: Data source (SMS, voice grievances)
- Needs: Privacy protection, consent control, grievance resolution
- Interaction: Passive (SMS forwarding) or active (voice grievance submission)

### 4.2 Secondary Stakeholders

**Gram Panchayat Officials**
- Role: Village-level monitoring and intervention
- Needs: Village-aggregated retention metrics, anomaly alerts, beneficiary-level drill-down
- Interaction: Dashboard access, alert notifications

**Block Development Officer (BDO)**
- Role: Block-level oversight and coordination
- Needs: Multi-village trend analysis, comparative metrics, escalation management
- Interaction: Web dashboard, periodic reports

**District Welfare Administrator**
- Role: District-level policy implementation and monitoring
- Needs: District-wide analytics, scheme-wise performance, anomaly investigation
- Interaction: Advanced analytics dashboard, export capabilities

**State Welfare Monitoring Unit**
- Role: State-level policy formulation and governance
- Needs: State-wide trends, inter-district comparisons, evidence for policy decisions
- Interaction: Executive dashboards, API access for integration

**Technical Implementation Partner**
- Role: System deployment, maintenance, model retraining
- Needs: Monitoring tools, model performance metrics, operational dashboards
- Interaction: Admin console, logging infrastructure

## 5. Glossary

- **DBT_System**: The Direct Benefit Transfer infrastructure operated by government
- **Beneficiary**: An individual enrolled in one or more DBT schemes
- **Credit_Event**: A DBT fund transfer credited to beneficiary bank account
- **Withdrawal_Event**: A debit transaction from beneficiary bank account
- **SMS_Notification**: Bank-generated message confirming credit or debit
- **Scheme**: A government welfare program disbursing funds via DBT
- **Retention_Window**: Time period between credit and withdrawal (measured in hours/days)
- **Assisted_Withdrawal**: Withdrawal occurring within suspicious timeframe after credit
- **Installment**: Periodic payment expected for recurring schemes (pensions, scholarships)
- **Continuity_Gap**: Missing installment in expected periodic sequence
- **Grievance**: Voice or text complaint from beneficiary regarding DBT issues
- **Aggregation_Level**: Hierarchical tier (beneficiary/village/block/district/state)
- **Anomaly**: Statistical deviation from expected retention or withdrawal patterns
- **Consent_Record**: Documented permission from beneficiary for data processing
- **Event_Store**: Persistent storage for beneficiary-level credit/withdrawal events
- **Classification_Model**: ML model for SMS scheme detection
- **Extraction_Engine**: NLP component for structured field extraction
- **ASR_Module**: Automatic Speech Recognition component for voice processing
- **Correlation_Engine**: Component matching withdrawals to credits
- **Aggregation_Pipeline**: Batch process computing hierarchical statistics

## 6. Functional Requirements

### Requirement 1: SMS Ingestion and Normalization

**User Story:** As a system administrator, I want to ingest SMS notifications from multiple banking sources, so that the system can process diverse message formats uniformly.

#### Acceptance Criteria

1. WHEN an SMS is received from a registered banking source, THE SMS_Ingestion_Module SHALL parse the message text and extract raw content
2. THE SMS_Normalization_Engine SHALL convert regional language text to standardized Unicode representation
3. WHEN SMS contains mixed-language content, THE SMS_Normalization_Engine SHALL preserve all language segments without data loss
4. THE SMS_Ingestion_Module SHALL record message metadata (timestamp, sender ID, beneficiary phone number)
5. WHEN SMS parsing fails, THE SMS_Ingestion_Module SHALL log the failure with original message content and continue processing other messages

### Requirement 2: DBT Scheme Classification

**User Story:** As a data analyst, I want SMS messages automatically classified by DBT scheme type, so that scheme-specific analytics can be generated.

#### Acceptance Criteria

1. WHEN a normalized SMS is processed, THE Classification_Model SHALL predict the DBT scheme category with confidence score
2. THE Classification_Model SHALL support minimum 50 major DBT schemes (PM-KISAN, MGNREGA, pensions, scholarships, LPG subsidy)
3. WHEN confidence score is below threshold (0.7), THE Classification_Model SHALL flag the message for manual review
4. THE Classification_Model SHALL handle multilingual SMS (Hindi, English, Tamil, Telugu, Bengali, Marathi, Gujarati)
5. WHEN SMS does not match any known scheme pattern, THE Classification_Model SHALL assign "UNKNOWN_SCHEME" category

### Requirement 3: Structured Entity Extraction

**User Story:** As a monitoring officer, I want structured data extracted from SMS notifications, so that credit and withdrawal events can be correlated.

#### Acceptance Criteria

1. WHEN a classified SMS is processed, THE Extraction_Engine SHALL extract scheme name, amount, transaction date, and bank reference number
2. THE Extraction_Engine SHALL normalize currency amounts to standard decimal format (₹1,234.56)
3. WHEN date formats vary across banks, THE Extraction_Engine SHALL convert all dates to ISO 8601 format
4. THE Extraction_Engine SHALL extract bank account last 4 digits when present in SMS
5. WHEN extraction confidence is below threshold, THE Extraction_Engine SHALL flag fields as "LOW_CONFIDENCE" and retain original text

### Requirement 4: Credit Event Storage

**User Story:** As a system architect, I want credit events stored with beneficiary linkage, so that longitudinal retention analysis can be performed.

#### Acceptance Criteria

1. WHEN entity extraction completes successfully, THE Event_Store SHALL create a Credit_Event record linked to beneficiary identifier
2. THE Event_Store SHALL store extracted fields (scheme, amount, date, reference) with extraction confidence scores
3. THE Event_Store SHALL maintain immutable audit trail of all credit events
4. WHEN duplicate SMS is detected (same reference number), THE Event_Store SHALL deduplicate and update existing record
5. THE Event_Store SHALL index events by beneficiary ID, scheme type, and transaction date for efficient querying

### Requirement 5: Withdrawal Detection and Correlation

**User Story:** As a welfare officer, I want withdrawal events correlated with credit events, so that retention windows can be calculated.

#### Acceptance Criteria

1. WHEN a debit SMS is detected, THE Correlation_Engine SHALL identify it as a Withdrawal_Event
2. THE Correlation_Engine SHALL match withdrawal to most recent unmatched credit event for same beneficiary
3. WHEN withdrawal occurs within 24 hours of credit, THE Correlation_Engine SHALL calculate retention window in hours
4. WHEN withdrawal occurs after 24 hours, THE Correlation_Engine SHALL calculate retention window in days
5. WHEN no matching credit event exists, THE Correlation_Engine SHALL store withdrawal as unmatched and flag for investigation

### Requirement 6: Assisted Withdrawal Anomaly Detection

**User Story:** As a district administrator, I want suspicious withdrawal patterns flagged, so that potential coercion or leakage can be investigated.

#### Acceptance Criteria

1. WHEN retention window is less than 2 hours, THE Anomaly_Detector SHALL flag the event as "IMMEDIATE_WITHDRAWAL"
2. WHEN a beneficiary has 3+ consecutive credits with retention windows <6 hours, THE Anomaly_Detector SHALL flag as "PATTERN_ANOMALY"
3. WHEN withdrawal amount matches credit amount exactly (within ₹10), THE Anomaly_Detector SHALL flag as "FULL_WITHDRAWAL"
4. THE Anomaly_Detector SHALL compute village-level baseline retention distribution and flag outliers (>2 standard deviations)
5. WHEN anomaly is detected, THE Anomaly_Detector SHALL create alert record with severity level (LOW/MEDIUM/HIGH)

### Requirement 7: Installment Continuity Monitoring

**User Story:** As a pension scheme administrator, I want missing installments detected automatically, so that payment failures can be investigated.

#### Acceptance Criteria

1. THE Installment_Monitor SHALL maintain scheme periodicity registry (monthly pensions, quarterly scholarships, annual subsidies)
2. WHEN expected installment date passes without credit event, THE Installment_Monitor SHALL flag "MISSING_INSTALLMENT" after grace period (7 days)
3. THE Installment_Monitor SHALL track consecutive missing installments and escalate severity after 2+ missed payments
4. WHEN installment arrives after grace period, THE Installment_Monitor SHALL record delay duration and update beneficiary record
5. THE Installment_Monitor SHALL exclude beneficiaries with scheme exit records from continuity checks

### Requirement 8: Voice Grievance Processing (Optional Module)

**User Story:** As a beneficiary, I want to submit voice grievances in my local language, so that my concerns are documented and analyzed.

#### Acceptance Criteria

1. WHEN voice audio is submitted, THE ASR_Module SHALL transcribe speech to text with language detection
2. THE ASR_Module SHALL support minimum 8 Indian languages (Hindi, English, Tamil, Telugu, Bengali, Marathi, Gujarati, Kannada)
3. WHEN transcription is complete, THE Intent_Classifier SHALL categorize grievance (payment delay, wrong amount, coercion, account issue, other)
4. THE Grievance_Extractor SHALL extract entities (scheme name, amount disputed, date of issue, location)
5. WHEN grievance mentions specific credit event, THE Grievance_Linker SHALL correlate with Event_Store records

### Requirement 9: Village-Level Aggregation

**User Story:** As a Gram Panchayat official, I want village-level retention statistics, so that I can identify local issues.

#### Acceptance Criteria

1. THE Aggregation_Pipeline SHALL compute village-level metrics daily (total credits, total withdrawals, median retention window, anomaly count)
2. THE Aggregation_Pipeline SHALL calculate scheme-wise distribution (percentage of beneficiaries per scheme)
3. THE Aggregation_Pipeline SHALL compute retention window distribution (0-2h, 2-6h, 6-24h, 1-7d, 7-30d, 30d+)
4. THE Aggregation_Pipeline SHALL identify top 5 anomaly patterns by frequency
5. THE Aggregation_Pipeline SHALL store aggregated metrics with timestamp and data freshness indicator

### Requirement 10: Hierarchical Rollup (Block/District/State)

**User Story:** As a state monitoring officer, I want aggregated metrics rolled up hierarchically, so that I can compare performance across regions.

#### Acceptance Criteria

1. THE Aggregation_Pipeline SHALL roll up village metrics to block level (sum, mean, median, percentiles)
2. THE Aggregation_Pipeline SHALL roll up block metrics to district level with same statistical measures
3. THE Aggregation_Pipeline SHALL roll up district metrics to state level with same statistical measures
4. WHEN aggregation completes, THE Aggregation_Pipeline SHALL compute inter-region variance and flag high-variance districts
5. THE Aggregation_Pipeline SHALL maintain historical aggregations for trend analysis (minimum 24 months retention)

### Requirement 11: Consent Management

**User Story:** As a beneficiary, I want control over my data processing, so that my privacy rights are respected per DPDP Act 2023.

#### Acceptance Criteria

1. THE Consent_Manager SHALL record explicit consent from beneficiary before processing SMS data
2. THE Consent_Manager SHALL support consent withdrawal with immediate effect on future processing
3. WHEN consent is withdrawn, THE Consent_Manager SHALL anonymize beneficiary-level data while preserving aggregated statistics
4. THE Consent_Manager SHALL maintain consent audit trail with timestamp and consent mode (digital signature, voice recording, written form)
5. THE Consent_Manager SHALL expire consent after 24 months and request renewal

### Requirement 12: Data Anonymization for Analytics

**User Story:** As a privacy officer, I want beneficiary identifiers anonymized in analytics layers, so that individual privacy is protected.

#### Acceptance Criteria

1. THE Anonymization_Engine SHALL hash beneficiary phone numbers using salted SHA-256 before storage
2. THE Anonymization_Engine SHALL replace beneficiary names with pseudonymous identifiers in aggregated reports
3. WHEN data is exported for research, THE Anonymization_Engine SHALL apply k-anonymity (minimum k=5) to prevent re-identification
4. THE Anonymization_Engine SHALL remove or generalize quasi-identifiers (exact GPS coordinates, precise timestamps)
5. WHEN aggregation cell size is <5 beneficiaries, THE Anonymization_Engine SHALL suppress the metric to prevent inference

### Requirement 13: Dashboard Access Control

**User Story:** As a system administrator, I want role-based access control for dashboards, so that users only see data appropriate to their jurisdiction.

#### Acceptance Criteria

1. THE Access_Control_Module SHALL enforce role-based permissions (village/block/district/state/admin)
2. WHEN a village-level user logs in, THE Access_Control_Module SHALL restrict data access to assigned village(s) only
3. WHEN a district-level user logs in, THE Access_Control_Module SHALL grant access to all blocks and villages within district
4. THE Access_Control_Module SHALL log all data access attempts with user ID, timestamp, and data scope
5. WHEN unauthorized access is attempted, THE Access_Control_Module SHALL deny request and alert administrator

### Requirement 14: Anomaly Alert Delivery

**User Story:** As a Block Development Officer, I want real-time alerts for high-severity anomalies, so that I can intervene promptly.

#### Acceptance Criteria

1. WHEN a HIGH severity anomaly is detected, THE Alert_System SHALL deliver notification within 15 minutes
2. THE Alert_System SHALL support multiple delivery channels (SMS, email, dashboard notification, mobile push)
3. THE Alert_System SHALL include anomaly details (beneficiary pseudonym, scheme, retention window, village)
4. WHEN alert is delivered, THE Alert_System SHALL track acknowledgment status and escalate if unacknowledged for 4 hours
5. THE Alert_System SHALL batch LOW severity alerts into daily digest reports

### Requirement 15: Model Retraining Pipeline

**User Story:** As a machine learning engineer, I want automated model retraining, so that classification accuracy remains high as SMS formats evolve.

#### Acceptance Criteria

1. THE Retraining_Pipeline SHALL collect manually reviewed SMS samples flagged during low-confidence classification
2. WHEN labeled sample count exceeds 1000 new examples, THE Retraining_Pipeline SHALL trigger model retraining
3. THE Retraining_Pipeline SHALL evaluate retrained model on held-out test set and compare to production model performance
4. WHEN retrained model achieves >2% accuracy improvement, THE Retraining_Pipeline SHALL promote model to staging environment
5. THE Retraining_Pipeline SHALL require manual approval before production deployment

### Requirement 16: Offline-First Synchronization

**User Story:** As a field officer in low-connectivity area, I want offline data collection with automatic sync, so that monitoring continues despite network issues.

#### Acceptance Criteria

1. THE Offline_Module SHALL queue SMS data locally when network is unavailable
2. WHEN network connectivity is restored, THE Offline_Module SHALL synchronize queued data to central server
3. THE Offline_Module SHALL resolve conflicts using server-side timestamp precedence
4. THE Offline_Module SHALL compress data before transmission to minimize bandwidth usage
5. WHEN sync fails after 3 retry attempts, THE Offline_Module SHALL alert user and preserve data for manual intervention

### Requirement 17: Data Retention and Deletion

**User Story:** As a compliance officer, I want automated data retention policies, so that the system complies with data protection regulations.

#### Acceptance Criteria

1. THE Retention_Manager SHALL retain beneficiary-level event data for maximum 36 months
2. WHEN data exceeds retention period, THE Retention_Manager SHALL archive to cold storage or delete per policy
3. THE Retention_Manager SHALL retain aggregated statistics indefinitely (no PII)
4. WHEN beneficiary requests data deletion (Right to be Forgotten), THE Retention_Manager SHALL purge all identifiable records within 30 days
5. THE Retention_Manager SHALL maintain deletion audit log for compliance verification

### Requirement 18: API for External Integration

**User Story:** As a state IT department, I want REST API access to aggregated metrics, so that I can integrate with existing dashboards.

#### Acceptance Criteria

1. THE API_Gateway SHALL expose RESTful endpoints for aggregated metrics (village/block/district/state level)
2. THE API_Gateway SHALL require OAuth 2.0 authentication with JWT tokens
3. THE API_Gateway SHALL enforce rate limiting (1000 requests per hour per client)
4. THE API_Gateway SHALL return data in JSON format with pagination support (max 1000 records per page)
5. WHEN API request includes date range, THE API_Gateway SHALL validate range does not exceed 12 months

### Requirement 19: Audit Logging

**User Story:** As an auditor, I want comprehensive system logs, so that I can verify system integrity and investigate incidents.

#### Acceptance Criteria

1. THE Audit_Logger SHALL log all data access events (user, timestamp, data scope, action)
2. THE Audit_Logger SHALL log all data modifications (entity type, operation, before/after values, user)
3. THE Audit_Logger SHALL log all authentication events (login, logout, failed attempts)
4. THE Audit_Logger SHALL log all anomaly detections with full context (beneficiary hash, scheme, metrics)
5. THE Audit_Logger SHALL store logs in tamper-evident format with cryptographic integrity verification

### Requirement 20: System Health Monitoring

**User Story:** As a DevOps engineer, I want real-time system health metrics, so that I can ensure service availability.

#### Acceptance Criteria

1. THE Health_Monitor SHALL track SMS processing throughput (messages per minute)
2. THE Health_Monitor SHALL track model inference latency (p50, p95, p99 percentiles)
3. THE Health_Monitor SHALL track database query performance and connection pool utilization
4. WHEN any metric exceeds threshold (latency >2s, error rate >5%), THE Health_Monitor SHALL trigger alert
5. THE Health_Monitor SHALL expose metrics in Prometheus format for integration with monitoring stack

## 7. Non-Functional Requirements

### Requirement 21: Performance and Scalability

**User Story:** As a system architect, I want the system to handle national-scale load, so that it can serve 500M+ beneficiaries.

#### Acceptance Criteria

1. THE DBT_System SHALL process minimum 10,000 SMS messages per minute per processing node
2. THE DBT_System SHALL respond to dashboard queries within 2 seconds for 95th percentile requests
3. THE DBT_System SHALL support horizontal scaling by adding processing nodes without downtime
4. THE DBT_System SHALL handle 1 million concurrent beneficiary records per district node
5. THE Aggregation_Pipeline SHALL complete daily rollup for 10 million beneficiaries within 4 hours

### Requirement 22: Availability and Reliability

**User Story:** As a state administrator, I want high system availability, so that monitoring is continuous.

#### Acceptance Criteria

1. THE DBT_System SHALL maintain 99.5% uptime (excluding planned maintenance)
2. WHEN a processing node fails, THE DBT_System SHALL automatically failover to standby node within 60 seconds
3. THE DBT_System SHALL implement circuit breakers for external dependencies (ASR service, database)
4. THE DBT_System SHALL persist all incoming SMS to durable queue before processing to prevent data loss
5. WHEN database is unavailable, THE DBT_System SHALL queue writes and retry with exponential backoff

### Requirement 23: Security and Encryption

**User Story:** As a security officer, I want data encrypted at rest and in transit, so that beneficiary information is protected.

#### Acceptance Criteria

1. THE DBT_System SHALL encrypt all data at rest using AES-256 encryption
2. THE DBT_System SHALL use TLS 1.3 for all network communication
3. THE DBT_System SHALL store encryption keys in hardware security module (HSM) or secure enclave
4. THE DBT_System SHALL implement certificate pinning for API clients
5. THE DBT_System SHALL rotate encryption keys every 90 days with zero-downtime key migration

### Requirement 24: Multi-Lingual Support

**User Story:** As a beneficiary in rural Tamil Nadu, I want the system to process Tamil SMS, so that my data is analyzed correctly.

#### Acceptance Criteria

1. THE DBT_System SHALL process SMS in Hindi, English, Tamil, Telugu, Bengali, Marathi, Gujarati, Kannada, Malayalam, Punjabi
2. THE Classification_Model SHALL maintain >85% accuracy across all supported languages
3. THE Extraction_Engine SHALL handle mixed-script SMS (e.g., Hindi text with English numerals)
4. THE Dashboard SHALL display UI in user-selected language (minimum Hindi and English)
5. THE ASR_Module SHALL support same 10 languages with >80% word error rate (WER) performance

### Requirement 25: Low-Resource Operation

**User Story:** As a deployment engineer, I want the system to run on modest hardware, so that it can be deployed in resource-constrained environments.

#### Acceptance Criteria

1. THE Offline_Module SHALL operate on devices with minimum 2GB RAM and 16GB storage
2. THE Classification_Model SHALL use quantized model (INT8) with <100MB memory footprint
3. THE DBT_System SHALL function with intermittent connectivity (sync when available)
4. THE Dashboard SHALL render on 2G network speeds (<50 kbps) with progressive loading
5. THE DBT_System SHALL support deployment on ARM architecture (for edge devices)

### Requirement 26: Data Protection and Privacy Compliance

**User Story:** As a legal counsel, I want the system to comply with DPDP Act 2023, so that the government avoids regulatory penalties.

#### Acceptance Criteria

1. THE DBT_System SHALL implement consent management per DPDP Act requirements (explicit, informed, revocable)
2. THE DBT_System SHALL provide beneficiary data access portal (Right to Access) within 48 hours of request
3. THE DBT_System SHALL complete data deletion requests (Right to be Forgotten) within 30 days
4. THE DBT_System SHALL appoint Data Protection Officer and maintain contact information in system
5. THE DBT_System SHALL conduct annual privacy impact assessment and document findings

### Requirement 27: Auditability and Transparency

**User Story:** As a CAG auditor, I want complete audit trails, so that I can verify system operations and data integrity.

#### Acceptance Criteria

1. THE DBT_System SHALL maintain immutable audit logs for minimum 7 years
2. THE DBT_System SHALL provide audit report generation with customizable date ranges and filters
3. THE DBT_System SHALL document all ML model versions with training data provenance
4. THE DBT_System SHALL expose model decision explanations (LIME/SHAP) for flagged anomalies
5. THE DBT_System SHALL publish annual transparency report with aggregate statistics (no PII)

## 8. Measurable KPIs

### 8.1 SMS Classification Performance

- **Scheme Detection Precision**: ≥90% (correctly identified schemes / total identified)
- **Scheme Detection Recall**: ≥85% (correctly identified schemes / total actual schemes)
- **Multi-lingual Accuracy**: ≥85% across all 10 supported languages
- **Low-Confidence Rate**: <15% (messages requiring manual review)

### 8.2 Entity Extraction Accuracy

- **Amount Extraction Accuracy**: ≥95% (within ₹10 of actual amount)
- **Date Extraction Accuracy**: ≥98% (exact date match)
- **Scheme Name Extraction**: ≥90% (exact or fuzzy match)
- **Reference Number Extraction**: ≥85% (when present in SMS)

### 8.3 Anomaly Detection Performance

- **True Positive Rate**: ≥70% (actual leakage cases correctly flagged)
- **False Positive Rate**: <20% (benign cases incorrectly flagged)
- **Alert Response Time**: <15 minutes for HIGH severity
- **Anomaly Investigation Closure**: 80% within 7 days

### 8.4 System Performance

- **SMS Processing Latency**: <2 seconds (p95)
- **Dashboard Query Response**: <2 seconds (p95)
- **System Uptime**: ≥99.5%
- **Data Loss Rate**: <0.01% (SMS messages lost due to system failure)

### 8.5 Voice Grievance Processing (Optional)

- **ASR Word Error Rate**: <20% for supported languages
- **Intent Classification Accuracy**: ≥80%
- **Grievance-Event Linkage Accuracy**: ≥75% (when linkage is possible)

### 8.6 Operational Metrics

- **Beneficiary Coverage**: Target 10M beneficiaries in pilot phase
- **Consent Rate**: ≥60% of contacted beneficiaries
- **Data Freshness**: Aggregated metrics updated within 24 hours
- **Model Drift Detection**: Monthly accuracy monitoring with <5% degradation threshold

## 9. Assumptions

1. Beneficiaries receive SMS notifications from banks for DBT credits and debits
2. SMS format varies across banks but contains essential fields (amount, date, reference)
3. Beneficiary phone numbers are linked to Aadhaar/bank accounts in existing systems
4. Village/block/district geographic hierarchies are available from existing government databases
5. Internet connectivity is intermittent in rural areas (offline-first design required)
6. Government has legal authority to process SMS data with beneficiary consent
7. Existing PFMS/DBT portals will continue to operate (this system is supplementary)
8. Banks will not change SMS formats drastically without notice
9. Voice grievances are optional and will have lower volume than SMS data
10. System will be deployed in phased manner (pilot districts before national rollout)

## 10. Limitations

1. System cannot prevent leakage in real-time (monitoring and alerting only)
2. Accuracy depends on SMS quality and format consistency
3. Withdrawal correlation assumes single bank account per beneficiary per scheme
4. Anomaly detection may have false positives requiring human investigation
5. Voice processing accuracy degrades with background noise and non-standard dialects
6. System cannot verify if withdrawal was voluntary or coerced (requires field investigation)
7. Aggregated metrics have 24-hour latency (not real-time dashboards)
8. Offline mode has limited analytics capabilities (full features require connectivity)
9. Model retraining requires labeled data from manual review (cold start problem)
10. Privacy-preserving techniques (k-anonymity, differential privacy) reduce data granularity

## 11. Risks and Mitigation

### 11.1 Technical Risks

**Risk**: SMS format changes break extraction pipeline
- **Mitigation**: Implement format versioning, maintain regex library, automated anomaly detection for parsing failures

**Risk**: ML model accuracy degrades over time (concept drift)
- **Mitigation**: Continuous monitoring, automated retraining pipeline, human-in-the-loop validation

**Risk**: Scalability bottlenecks at national scale
- **Mitigation**: Horizontal scaling architecture, database sharding by district, caching layer

### 11.2 Privacy and Legal Risks

**Risk**: Beneficiary consent withdrawal creates data gaps
- **Mitigation**: Preserve aggregated statistics, design analytics to handle missing data

**Risk**: Data breach exposes beneficiary information
- **Mitigation**: Encryption at rest/transit, access controls, regular security audits, incident response plan

**Risk**: Non-compliance with DPDP Act 2023
- **Mitigation**: Legal review, privacy-by-design, DPO appointment, regular compliance audits

### 11.3 Operational Risks

**Risk**: False positives overwhelm investigation capacity
- **Mitigation**: Tunable anomaly thresholds, severity-based prioritization, automated triage

**Risk**: Low adoption due to beneficiary distrust
- **Mitigation**: Transparent communication, clear consent process, demonstrated value (grievance resolution)

**Risk**: Insufficient labeled data for model training
- **Mitigation**: Synthetic data generation, transfer learning from existing NLP models, active learning

### 11.4 Governance Risks

**Risk**: Misuse of data for political purposes
- **Mitigation**: Strict access controls, audit logging, independent oversight committee

**Risk**: System findings ignored by administrators
- **Mitigation**: Integration with existing governance workflows, escalation mechanisms, public dashboards

## 12. Success Criteria

The system will be considered successful if:

1. **Technical**: Achieves ≥85% classification accuracy and <2s latency at 10M beneficiary scale
2. **Operational**: Processes 1M+ SMS per day with <0.01% data loss
3. **Impact**: Detects ≥1000 anomalous patterns in pilot phase with ≥70% true positive rate
4. **Adoption**: Achieves ≥60% beneficiary consent rate in pilot districts
5. **Compliance**: Passes independent privacy audit with zero critical findings
6. **Governance**: Integrated into district-level monitoring workflows within 6 months of pilot launch
7. **Scalability**: Successfully scales from pilot (3 districts) to state-wide (30+ districts) within 12 months

## 13. Future Enhancements (Post-V1)

1. Real-time SMS processing with <30 second latency
2. Predictive analytics for installment delay forecasting
3. Integration with Aadhaar authentication for beneficiary verification
4. Mobile app for beneficiaries to view their retention history
5. Advanced ML models (transformer-based) for improved accuracy
6. Blockchain-based audit trail for tamper-proof logging
7. Integration with banking APIs for direct transaction data (eliminating SMS dependency)
8. Chatbot interface for beneficiary queries and grievance submission
9. Geospatial analytics with heat maps for leakage hotspots
10. Cross-scheme analytics to detect beneficiaries with consistent patterns across multiple programs

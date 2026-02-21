# Bank-Grade AWS Data Platform

## Overview
This project simulates a production-style banking data pipeline.

It will process daily savings account transactions using modern
data engineering principles and AWS serverless services.

---

## Objectives
- Implement raw → curated data layering
- Enforce data quality validation
- Write partitioned Parquet outputs
- Orchestrate workflow using Step Functions
- Maintain audit logging
- Provision infrastructure using Terraform

---

## Architecture (Planned)

Event Trigger  
→ Ingestion (Lambda)  
→ Data Quality Validation  
→ Transformation (Glue)  
→ Curated Storage (S3 - Parquet)  
→ Catalog (Glue)  
→ Query Layer (Athena)

---

## Status
🚧 Project setup phase

# Build GTM Data Pipeline for Legal AI Startup

## Overview

You're a full-stack engineer at a legal AI startup building the GTM data infrastructure to acquire midsize law firm clients across the US, Australia, and Asia. This pipeline will power lead acquisition from ingestion through experiment assignment, ensuring a seamless integration of various data sources and systems to optimize customer acquisition strategies.

## Problem Statement

Build an end-to-end GTM data pipeline that integrates API data sources, enriches lead information, scores prospects against ideal customer profiles, routes qualified leads, and assigns experiment variants. This assessment will evaluate not only your engineering capabilities but also your understanding of systems integration, data flow management, and the ability to handle real-world API challenges.

## Getting Started

### Prerequisites

- Python 3.9+

### Setup Instructions

Run the setup script from the project root to get started:

```bash
./setup.sh
```

Any API tokens or additional information can be found on the **Assessment Instruction Page** where you downloaded this assessment.

### Running

Start the mock server in one terminal:

```bash
python mock_server.py
```

Run your pipeline in another terminal.

## Project Structure

```
pipeline.py       — main pipeline orchestrator
enricher.py       — firmographic and contact data enrichment
scorer.py         — ICP scoring
router.py         — lead routing
experiment.py     — A/B experiment assignment
webhook.py        — webhook delivery to downstream systems
mock_server.py    — fully working mock API server (do not modify)
config.yaml       — pipeline configuration (weights and thresholds are yours to set)
```

## Requirements

### Functional Requirements

1. **Pipeline Orchestration**: Implement a main pipeline function that processes firms through the complete GTM workflow, handling configuration from YAML files.

2. **API Integration**: Build robust API clients that handle rate limiting (429 errors), implement exponential backoff retry logic, and gracefully handle network timeouts and connection errors.

3. **Data Enrichment**: Fetch firmographic data (employee count, practice areas) and contact information for each firm, handling cases where enrichment APIs return partial or missing data.

4. **Deduplication**: Identify and handle duplicate firms based on domain, name similarity, and other identifying characteristics to prevent duplicate processing.

5. **ICP Scoring**: Score firms against ideal customer profile criteria (firm size, practice areas, geographic regions) with configurable weights and thresholds.

6. **Lead Routing**: Route leads into categories based on their score and firm data.

7. **Experiment Assignment**: Assign leads to email campaign variants (A/B tests) with proper randomization.

8. **Webhook Integration**: Fire webhooks to downstream systems (CRM, email platforms) with proper error handling and retry logic for failed deliveries.

9. **Error Handling**: Implement error handling for API failures, data validation errors, and system exceptions with appropriate logging and recovery strategies.

### Mock Server

The mock server (`mock_server.py`) is fully working and simulates real-world API imperfections:

- **GET /firms** — paginated list of 55 law firms (includes near-duplicates). ~10% of requests return 500 errors. Rate limit enforced at 20 requests/minute (returns 429 with `Retry-After` header when exceeded).
- **GET /firms/{id}/firmographic** — firmographic data. ~20% of responses have missing fields. Schema is occasionally inconsistent (e.g. `num_lawyers` vs `lawyer_count`).
- **GET /firms/{id}/contact** — contact info. ~30% of contacts have null email or null LinkedIn URL.
- **POST /webhooks/crm** — accepts lead payloads. ~5% chance of failure.
- **POST /webhooks/email** — accepts campaign payloads. ~5% chance of failure.

API docs are available at `http://localhost:8000/docs` when the server is running.

Do not modify the mock server. Your pipeline needs to handle its behavior as-is.

### Deliverables

1. **Working pipeline** — implements all functional requirements above
2. **WRITEUP.md** — a short write-up explaining your approach, trade-offs, and what you'd do differently with more time. Reference specific parts of your code so reviewers can follow your reasoning.

## Submission Guidelines

Upload a zip file of your completed assessment to the assessment platform.

---
**Questions?** Contact us at founders@litmus.build
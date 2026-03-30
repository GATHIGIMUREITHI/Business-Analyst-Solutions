# Business Analyst Solutions

Portfolio projects for Business Analysts focused on Finance:
- Revenue & Cost weekly insights
- Investment performance weekly insights

## What this repo demonstrates
- Excel/CSV ingestion
- Python ETL + data validation 
- Curated outputs for analysis
- Tableau-ready Hyper extracts (`.hyper`)
- Excel analyst pack workflow (Power Query-friendly)

## Repo structure
- `data/sample/` small synthetic datasets committed to git
- `data/raw/` (ignored) place real weekly extracts here
- `data/curated/` (ignored) generated curated datasets
- `src/` python pipeline code
- `docs/` metric catalog + runbook
- `tableau/` notes and wireframes

## Quickstart (Windows)
1. Create and activate a virtual environment:
   ```powershell
   py -m venv .venv
   .\.venv\Scripts\Activate.ps1
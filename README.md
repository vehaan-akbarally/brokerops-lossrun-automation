# BrokerOps Loss-Run Automation (R Shiny)

## What this project does
This app automates ingestion and validation of loss-run / bordereaux-style data to support faster claims QA and cleaner downstream actuarial analysis.

## Key features
- Upload and parse claims bordereaux files
- Data-quality checks (missing values, duplicates, date/amount logic)
- Review tables for exceptions and validation output
- Environment-aware configuration for safer runtime behavior

## Tech stack
- R
- Shiny
- (add your main packages here)

## Run locally
1. Clone this repo
2. Open R/RStudio in project folder
3. Install required packages
4. Run:
   ```r
   shiny::runApp("app.R")

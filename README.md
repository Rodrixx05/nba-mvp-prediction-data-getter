# 🏀 NBA MVP Prediction Pipeline (GCP Cloud Function)

This repository contains a Dockerized Google Cloud Function (2nd gen) that:
- Extracts NBA player stats from Basketball Reference
- Preprocesses and scores them using ML models stored in GCS
- Writes predictions to a PostgreSQL database
- Is triggered via Pub/Sub and scheduled via Cloud Scheduler

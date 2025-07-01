# 🏏 CricAPI Data Pipeline with AWS + Snowflake

This repository contains a complete ETL pipeline that extracts cricket data from [CricAPI](https://www.cricapi.com/),
stores it in **AWS**, loads it into **Snowflake**, and transforms it using **Snowpark**.

It provides access to:

- 📅 Series and match info
- 🧑‍🤝‍🧑 Player and team profiles
- 📊 Player statistics
- 🔴 Live scores

---

## ⚙️ Architecture Overview

1. **CricAPI** → Python scripts extract data via REST API
2. **AWS (e.g., S3)** → Raw JSON stored in S3
3. **Snowflake (Staging Layer)** → Raw Data loaded into Snowflake
4. **Snowpark API** → SQL transformations and logic applied
5. **SQL Layer** → Finalized data models for analytics

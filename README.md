# 🏦 Fintech Transactions Data Pipeline

## 📖 Overview
This project builds an end-to-end **data pipeline** using **Python** and **Pandas**
to process and analyze credit card transaction data from Kaggle:
> “Credit Card Transactions Fraud Detection Dataset” by Kartik Shenoy.

## 🎯 Objectives
- Extract, clean, and transform raw transaction data  
- Generate cleaned and summarized CSV datasets  
- Identify top customers, top categories, and spending patterns over time  
- Prepare data for further BI dashboards or fraud analysis

## 🧱 Folder Structure
fintech_transactions_pipeline/
├─ data/
│ ├─ raw/ # raw CSVs from Kaggle
│ ├─ processed/ # cleaned data
│ └─ outputs/ # summary CSVs
├─ src/
│ ├─ extract.py # extract raw data from CSV
│ ├─ clean_transform.py# clean + transform data
│ └─ analyze.py # insights + plots
├─ notebooks/
│ └─ 01_quick_eda.ipynb# exploratory data analysis (Jupyter)
├─ venv/ # virtual environment
└─ README.md # project overview
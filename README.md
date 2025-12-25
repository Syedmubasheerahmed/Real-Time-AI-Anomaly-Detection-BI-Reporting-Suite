# Real-Time-AI-Anomaly-Detection-BI-Reporting-Suite
<img width="942" height="550" alt="123" src="https://github.com/user-attachments/assets/3cee2ece-8141-4d9e-af84-6e036600994a" />

This project is a data solution focused on automated data quality monitoring, anomaly detection, and business intelligence visualization. It showcases how modern data analysis, machine learning, and dashboarding can be integrated to deliver real-time actionable insights for decision-makers.

Key Features
Data Generation & Ingestion:
Simulates over 10,000 transaction records using Python, with deliberate injection of 15% data anomalies to test and validate system robustness.

Automated Data Cleaning & Validation:
Uses Python (Pandas, NumPy) and SQL scripts to detect, analyze, and automatically correct common data quality issues, including missing, negative, or invalid values.

Machine Learning-Based Anomaly Detection:
Implements Isolation Forest (unsupervised) and Random Forest (supervised) models via Scikit-learn to classify and flag anomalous data entries, achieving up to 81% error detection accuracy.

Business Intelligence Dashboard:
Integrates with Power BI to visualize key KPIs such as Total Revenue, Error Rate, AI Accuracy, and Revenue Recovered, utilizing custom DAX calculations for real-time performance tracking.

Self-Healing Data Pipeline:
Automated scripts execute rule-based remedies to repair corrupted transactions, improving data integrity and quantifying financial impact.
Tech Stack
Python: Data generation, cleaning, model development
SQL, PostgreSQL: Data storage, ingestion, query processing
Scikit-Learn: Machine learning models for anomaly detection
Power BI: Data visualization and business reporting
Pandas, NumPy: Data manipulation and statistical tests

Business Impact
By combining AI-driven anomaly detection with automated self-healing mechanisms and real-time dashboarding, this project demonstrates how organizations can significantly increase data accuracy, reduce manual cleaning, and visualize financial benefits directly tied to data quality improvements.

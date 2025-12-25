import pandas as pd
from sqlalchemy import create_engine, text
from sklearn.ensemble import IsolationForest, RandomForestClassifier
import time

DB_CONN = ''
engine = create_engine(DB_CONN)

anomaly_model = IsolationForest(contamination=0.12, n_estimators=100, random_state=42)
error_model = RandomForestClassifier(n_estimators=50, max_depth=10, random_state=42)

def train():
    df = pd.read_sql("SELECT price, quantity, total_revenue, discount_percent, CASE WHEN data_quality_status='CLEAN' THEN 0 ELSE 1 END as err FROM fact_transactions LIMIT 1000", engine)
    if len(df) < 50: return False
    anomaly_model.fit(df[['total_revenue', 'quantity', 'price']])
    if df['err'].sum() > 10:
        error_model.fit(df[['total_revenue', 'quantity', 'price', 'discount_percent']], df['err'])
    return True

def scan_predict():
    df = pd.read_sql("SELECT * FROM fact_transactions WHERE predicted_error_probability IS NULL LIMIT 100", engine)
    if df.empty: return df

    features = df[['total_revenue', 'quantity', 'price']].fillna(0)
    df['ai_prediction'] = anomaly_model.predict(features)
    df['predicted_error_probability'] = error_model.predict_proba(df[['total_revenue', 'quantity', 'price', 'discount_percent']].fillna(0))[:, 1]

    with engine.connect() as conn:
        for _, row in df.iterrows():
            conn.execute(text("UPDATE fact_transactions SET predicted_error_probability=:p, anomaly_score=:a WHERE transaction_key=:tk"),
                        {'p': float(row['predicted_error_probability']), 'a': float(row['ai_prediction']), 'tk': row['transaction_key']})
        conn.commit()
    return df

def main():
    print("AI Detection Engine Started")
    train()
    while True:
        df = scan_predict()
        if not df.empty:
            print(f"✓ Scanned {len(df)}, Anomalies: {(df['ai_prediction']==-1).sum()}")
        time.sleep(3)

if __name__ == "__main__": main()

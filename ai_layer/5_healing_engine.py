import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime
import time

DB_CONN = ''
engine = create_engine(DB_CONN)

def scan_errors():
    query = """SELECT * FROM fact_transactions 
               WHERE data_quality_status != 'CLEAN' AND is_fixed = FALSE 
               AND predicted_error_probability > 0.5 LIMIT 50"""
    return pd.read_sql(query, engine)

def diagnose(row):
    if row['price'] < 0 or row['total_revenue'] < 0:
        return {'cause': 'NEGATIVE_PRICE', 'fix': 'abs', 'conf': 0.95}
    elif row['price'] == 0:
        avg = pd.read_sql(f"SELECT AVG(price) FROM fact_transactions WHERE product_key={row['product_key']} AND price>0", engine).iloc[0,0]
        return {'cause': 'NULL_VALUE', 'fix': 'avg', 'conf': 0.90, 'avg': avg or 150}
    elif row['quantity'] > 100:
        return {'cause': 'BOT_SPIKE', 'fix': 'cap', 'conf': 0.88}
    return {'cause': 'UNKNOWN', 'fix': None, 'conf': 0.60}

def heal(row, diag):
    if diag['conf'] < 0.75: return False

    fixed = {'price': row['price'], 'qty': row['quantity'], 'revenue': row['total_revenue']}

    if diag['cause'] == 'NEGATIVE_PRICE':
        fixed['price'] = abs(row['price'])
        fixed['revenue'] = abs(row['total_revenue'])
    elif diag['cause'] == 'NULL_VALUE':
        fixed['price'] = diag['avg']
        fixed['revenue'] = diag['avg'] * row['quantity']
    elif diag['cause'] == 'BOT_SPIKE':
        fixed['qty'] = 5
        fixed['revenue'] = row['price'] * 5
    else:
        return False

    with engine.connect() as conn:
        conn.execute(text("UPDATE fact_transactions SET price=:p, quantity=:q, total_revenue=:r, is_fixed=TRUE WHERE transaction_key=:tk"),
                    {'p': fixed['price'], 'q': fixed['qty'], 'r': fixed['revenue'], 'tk': row['transaction_key']})
        conn.commit()
    return True

def main():
    print("Auto-Healing Engine Started")
    total_healed = 0
    while True:
        errors = scan_errors()
        if not errors.empty:
            for _, row in errors.iterrows():
                diag = diagnose(row)
                if heal(row, diag):
                    total_healed += 1
                    print(f"✓ Healed: {row['transaction_id']} - {diag['cause']}")
        time.sleep(5)

if __name__ == "__main__": main()
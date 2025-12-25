import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
import random, time

DB_CONN = ''
engine = create_engine(DB_CONN)

products = pd.read_sql("SELECT * FROM dim_products", engine)
regions = pd.read_sql("SELECT * FROM dim_regions", engine)
customers = pd.read_sql("SELECT * FROM dim_customers", engine)
total = 0

def generate(batch_size=10):
    global total
    data = []
    for _ in range(batch_size):
        prod = products.sample(1).iloc[0]
        region = regions.sample(1).iloc[0]
        cust = customers.sample(1).iloc[0]

        price = prod['base_price']
        qty = random.randint(1, 8)
        discount = 15 if cust['customer_type']=='Premium' else 5 if cust['customer_type']=='Standard' else 0
        price *= (1 - discount/100)

        status = 'CLEAN'
        error_type = None

        if random.random() < 0.08:
            choice = random.random()
            if choice < 0.35: price *= -1; status = 'ERROR_NEGATIVE_PRICE'; error_type = 'PRICING_BUG'
            elif choice < 0.65: price = 0; status = 'ERROR_NULL_VALUE'; error_type = 'DATA_CORRUPTION'
            elif choice < 0.85: qty = random.randint(500, 999); status = 'ERROR_ANOMALY_SPIKE'; error_type = 'BOT_TRAFFIC'
            else: status = 'ERROR_DUPLICATE'; error_type = 'DUPLICATE_RECORD'

        time_key = pd.read_sql(f"SELECT time_key FROM dim_time WHERE full_date='{datetime.now().date()}'", engine).iloc[0,0]

        data.append({
            'transaction_id': f'TXN-{total+len(data)+1:08d}',
            'product_key': prod['product_key'], 'region_key': region['region_key'],
            'time_key': time_key, 'customer_key': cust['customer_key'],
            'timestamp': datetime.now(), 'price': round(price, 2), 'quantity': qty,
            'discount_percent': discount, 'total_revenue': round(price*qty, 2),
            'data_quality_status': status, 'error_type': error_type,
            'is_fixed': False, 'anomaly_score': 0, 'predicted_error_probability': None
        })

    total += len(data)
    pd.DataFrame(data).to_sql('fact_transactions', engine, if_exists='append', index=False)
    return data

def main():
    print("Stream Generator Started")
    while True:
        txns = generate(10)
        print(f"✓ Generated {len(txns)} transactions | Total: {total}")
        time.sleep(1)

if __name__ == "__main__": main()
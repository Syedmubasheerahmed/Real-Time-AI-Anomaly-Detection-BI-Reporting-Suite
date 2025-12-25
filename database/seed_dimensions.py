import psycopg2
from datetime import datetime, timedelta

conn = psycopg2.connect(host='', database='', user='', password='', port=)
cur = conn.cursor()

products = [('PROD-001', 'Gaming Laptop', 'Electronics', 1299.99, 'TechCorp'),
            ('PROD-002', 'Wireless Mouse', 'Electronics', 49.99, 'PeripheralPro'),
            ('PROD-003', '4K Monitor', 'Electronics', 399.99, 'DisplayMax')]

regions = [('REG-001', 'North America', 'United States', 'EST', 'USD'),
           ('REG-002', 'Europe', 'United Kingdom', 'GMT', 'GBP')]

customers = [('CUST-001', 'Premium', 'Platinum', 15000),
             ('CUST-002', 'Standard', 'Gold', 5000)]

for p in products:
    cur.execute("INSERT INTO dim_products (product_id, product_name, category, base_price, supplier) VALUES (%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING", p)

for r in regions:
    cur.execute("INSERT INTO dim_regions (region_id, region_name, country, timezone, currency) VALUES (%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING", r)

for c in customers:
    cur.execute("INSERT INTO dim_customers (customer_id, customer_type, loyalty_tier, lifetime_value) VALUES (%s,%s,%s,%s) ON CONFLICT DO NOTHING", c)

start = datetime.now() - timedelta(days=365)
for i in range(730):
    d = start + timedelta(days=i)
    cur.execute("INSERT INTO dim_time (full_date, year, quarter, month, month_name, week, day_of_week, day_name, is_weekend, is_holiday) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING",
                (d.date(), d.year, (d.month-1)//3+1, d.month, d.strftime('%B'), d.isocalendar()[1], d.weekday()+1, d.strftime('%A'), d.weekday()>=5, False))

conn.commit()
cur.close()
conn.close()
print(" Dimension tables seeded")


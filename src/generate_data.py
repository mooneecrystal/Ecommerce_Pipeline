import pandas as pd
import numpy as np
from faker import Faker
import uuid
import random
import os
from src.project_config import *

fake = Faker()

def inject_chaos(df, corruption_rate=0.05):
    """Randomly corrupts data."""
    if df.empty: return df
    mask = np.random.choice([True, False], size=len(df), p=[corruption_rate, 1-corruption_rate])
    target_col = np.random.choice(df.columns)
    if pd.api.types.is_numeric_dtype(df[target_col]):
        df[target_col] = df[target_col].astype(object)
    df.loc[mask, target_col] = "INVALID_DATA"
    return df

# --- STATE MANAGEMENT ---

def get_master_data(file_name, columns):
    """Loads local CSV state or creates empty DataFrame if not exists."""
    path = os.path.join(DATA_DIR, file_name)
    
    if os.path.exists(path):
        return pd.read_csv(path)
    return pd.DataFrame(columns=columns)

def save_master_data(df, file_name):
    """Saves the updated state back to local CSV."""
    path = os.path.join(DATA_DIR, file_name)
    df.to_csv(path, index=False)

# --- GENERATORS ---

def generate_new_products(existing_df, num_new=5):
    """Adds N new products to the master list."""
    products = []
    categories = ['Electronics', 'Clothing', 'Home', 'Books', 'Sports']
    
    for _ in range(num_new):
        cat = random.choice(categories)
        products.append({
            'product_id': str(uuid.uuid4()),
            'product_name': f"{fake.word().title()} {fake.word().title()}",
            'category': cat,
            'subcategory': fake.word(),
            'brand': fake.company(),
            'unit_price': round(random.uniform(10, 500), 2)
        })
    
    new_df = pd.DataFrame(products)
    if existing_df.empty:
        return new_df
    return pd.concat([existing_df, new_df], ignore_index=True)

def generate_new_customers(existing_df, current_date, num_new=5):
    """Adds N new customers to the master list with specific signup date."""
    customers = []
    for _ in range(num_new):
        customers.append({
            'customer_id': str(uuid.uuid4()),
            'first_name': fake.first_name(),
            'last_name': fake.last_name(),
            'email': fake.email(),
            'phone': fake.phone_number(),
            'city': fake.city(),
            'state': fake.state(),
            'country': 'USA', 
            'signup_date': current_date # They signed up TODAY
        })
        
    new_df = pd.DataFrame(customers)
    if existing_df.empty:
        return new_df
    return pd.concat([existing_df, new_df], ignore_index=True)

def simulate_scd_updates(customers_df, current_date, num_updates=2):
    """
    Simulates SCD Type 2 (City Changes). 
    We update the 'city' in place in the master list.
    S3 uploads will capture the snapshot difference.
    """
    if customers_df.empty or num_updates == 0:
        return customers_df
        
    df = customers_df.copy()
    
    # Pick random victims who signed up BEFORE today
    # (Can't move city the same day you sign up, usually)
    eligible_indices = df[pd.to_datetime(df['signup_date']) < pd.to_datetime(current_date)].index
    
    if len(eligible_indices) > 0:
        victim_indices = np.random.choice(eligible_indices, size=min(num_updates, len(eligible_indices)), replace=False)
        for idx in victim_indices:
            df.at[idx, 'city'] = fake.city()
            
    return df

def generate_daily_orders(date_str, products_df, customers_df, num_orders=50, chaos_rate=0.0):
    """Generates orders using the CURRENT state of customers/products."""
    orders = []
    order_items = []
    
    # Filter: Only customers who exist by this date
    # (Since we are passing the Master DF, it might contain future customers if we aren't careful, 
    # but in our loop logic we append day by day, so the DF passed in IS the current state)
    
    product_ids = products_df['product_id'].tolist()
    customer_ids = customers_df['customer_id'].tolist()
    
    if not customer_ids or not product_ids:
        return pd.DataFrame(), pd.DataFrame()

    for _ in range(num_orders):
        order_id = str(uuid.uuid4())
        cust_id = random.choice(customer_ids)
        
        # Late Arrival Logic
        order_date = pd.to_datetime(date_str)
        if random.random() < 0.1: # 10% chance of late data
            lag = random.randint(1, 3)
            order_date = order_date - pd.Timedelta(days=lag)
            
        orders.append({
            'order_id': order_id,
            'customer_id': cust_id,
            'order_date': order_date.date(),
            'order_status': random.choice(['COMPLETED', 'PENDING', 'CANCELLED']),
            'revenue': 0,
            "discount": 0
        })
        
        # Items
        num_items = random.randint(1, 5)
        current_total = 0
        for _ in range(num_items):
            prod_id = random.choice(product_ids)
            price = float(products_df[products_df['product_id'] == prod_id]['unit_price'].values[0])
            qty = random.randint(1, 4)
            
            order_items.append({
                'order_id': order_id,
                'product_id': prod_id,
                'quantity': qty,
                'unit_price': price,
            })
            current_total += (price * qty)
        orders[-1]['revenue'] = current_total
        orders[-1]['discount'] = random.randint(0, 10) / 100 * current_total

    df_orders = pd.DataFrame(orders)
    df_items = pd.DataFrame(order_items)
    
    if chaos_rate > 0:
        df_orders = inject_chaos(df_orders, chaos_rate)
        
    return df_orders, df_items
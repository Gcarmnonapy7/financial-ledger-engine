import psycopg
import random 
import uuid  
from datetime import datetime, timedelta, timezone
from decimal import Decimal


DB_URI = "postgresql://postgres:postgres@localhost:5432/postgres"

NUM_ACCOUNTS = 100_000
NUM_TRANSACTIONS = 1_000_000
BATCH_SIZE = 50_000

CITIES = [
    (-23.5505, -46.6333),  # São Paulo
    (-22.9068, -43.1729),  # Rio de Janeiro
    (40.7128, -74.0060),   # New York (Impossible travel target)
    (51.5074, -0.1278),    # London (Impossible travel target)
]

def generate_and_seed_data():
    start_time = datetime.now()
    print(f"[{start_time.strftime('%H:%M:%S')}] Connecting to PostgreSQL...")
    
    with psycopg.connect(DB_URI) as conn:
        with conn.cursor() as cur: 
            
            print(f"Generating {NUM_ACCOUNTS:,} accounts...")
            account_ids = [uuid.uuid4() for _ in range(NUM_ACCOUNTS)] # generate unique account IDs
            
            # create a temp table for inserting accounts
            
            with cur.copy("COPY accounts (account_id) FROM STDIN") as copy:
                base_account_time = datetime(2026,7,1,0,0,0, tzinfo=timezone.utc)
                for account_id in account_ids:
                    
                    user_id = uuid.uuid4()
                    balance = Decimal(random.randint(10_000, 100_000))
                    created_at = base_account_time + timedelta(minutes=random.randint(0, 10_000))
                    copy.write_row((account_id, user_id, balance, 'ACTIVE', 'USD', created_at))
                    
            conn.commit()
            print(f"Accounts committed.")
            
            
            # stream transactions (Batch streaming)
            print(f"Generating {NUM_TRANSACTIONS:,} transactions across range partitions...")
            batch_time = datetime(2026,7,1,0,0,0, tzinfo=timezone.utc)
            total_inserted = 0
            
            #loop for insert the number of transactions in batches
            while total_inserted < NUM_TRANSACTIONS:
                
                batch_limit = min(BATCH_SIZE, NUM_TRANSACTIONS - total_inserted)
                
                with cur.copy("""
                              COPY transactions (
                        tx_id, source_account_id, dest_account_id, 
                        amount, status, ip_address, latitude, longitude, created_at
                    ) FROM STDIN
                            """) as copy :
                    for _ in range(batch_limit):
                        tx_id = uuid.uuid4()
                        src = random.choice(account_ids)
                        dst = random.choice(account_ids)
                        while dst == src:
                            dst = random.choice(account_ids)

                        amount = Decimal(round(random.uniform(5.0, 450.0), 2))
                        
                        # Advance simulation time sequentially (ensures realistic time-series distribution)
                        current_sim_time += timedelta(milliseconds=random.randint(50, 400))

                        # 98% Normal, 2% Injected Anomaly (Simulating impossible location jump)
                        is_fraud_probe = random.random() < 0.02
                        if is_fraud_probe:
                            lat, lon = random.choice(CITIES[2:])  # Overseas jump
                            ip = f"198.51.100.{random.randint(1, 254)}"
                        else:
                            lat, lon = random.choice(CITIES[:2])  # Local
                            ip = f"177.18.{random.randint(1, 254)}.{random.randint(1, 254)}"

                        copy.write_row((
                            tx_id, src, dst, amount, 'PENDING', ip, lat, lon, current_sim_time
                        ))
                
                conn.commit()
                total_inserted += batch_limit
                print(f"  -> Inserted {total_inserted:,} / {NUM_TRANSACTIONS:,} transactions")

            # ---------------------------------------------------------
            # 3. POPULATE DOUBLE-ENTRY LEDGER FROM TRANSACTIONS
            # ---------------------------------------------------------
            # Bulk processing through SQL set operations is significantly 
            # faster than running 500k individual triggers during initial seed.
            print("Populating double-entry ledger entries directly via SQL set projection...")
            
            cur.execute("""
                INSERT INTO ledger_entries (tx_id, account_id, direction, amount, balance_after, recorded_at)
                SELECT 
                    tx_id,
                    source_account_id,
                    'DEBIT',
                    amount,
                    -- Synthetic snapshot for initial historical ledger state
                    10000.00 - amount,
                    created_at
                FROM transactions
                UNION ALL
                SELECT 
                    tx_id,
                    dest_account_id,
                    'CREDIT',
                    amount,
                    10000.00 + amount,
                    created_at
                FROM transactions;
            """)
            conn.commit()

            # Mark processed transactions as POSTED
            cur.execute("UPDATE transactions SET status = 'POSTED';")
            conn.commit()

    duration = datetime.now() - start_time
    print(f"\n[DONE] Finished seeding in {duration.total_seconds():.2f} seconds.")
    
if __name__ == "__main__":
    generate_and_seed_data()
import os
import time
import random
from datetime import datetime, timezone, timedelta
from pathlib import Path
import pandas as pd
from faker import Faker

fake = Faker()

DATA_DIR = Path(os.getenv("DATA_DIR", "/app/data"))
EVENT_INTERVAL = int(os.getenv("EVENT_INTERVAL", 10))
DEVICE_TYPES = ["mobile", "tablet", "laptop", "desktop"]
PLATFORMS = ["iOS", "Android", "Windows", "MacOS", "Linux"]
EVENT_TYPES = ["page_view", "click", "scroll", "video_start", "video_complete", "form_submit"]

# Maintain some session continuity
ACTIVE_SESSIONS = []
ACTIVE_USERS = []

def generate_device_id():
    """Generate a consistent device ID for session continuity."""
    return fake.uuid4()

def generate_event():
    """Generate a single device event."""
    device_type = random.choice(DEVICE_TYPES)

    # Create nested payload structure
    payload = {
        "screen_resolution": f"{random.choice([1920, 1366, 1440, 2560])}x{random.choice([1080, 768, 900, 1440])}",
        "browser": random.choice(["Chrome", "Safari", "Firefox", "Edge"]),
        "browser_version": f"{random.randint(90, 120)}.0",
        "location": {
            "city": fake.city(),
            "country": fake.country_code(),
            "latitude": float(fake.latitude()),
            "longitude": float(fake.longitude())
        },
        "session": {
            "session_id": fake.uuid4(),
            "session_start": fake.iso8601(),
            "is_new_session": random.choice([True, False])
        },
        "performance": {
            "page_load_time_ms": random.randint(100, 3000),
            "network_type": random.choice(["4G", "5G", "WiFi", "3G"])
        }
    }

    return {
        "event_id": fake.uuid4(),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "user_id": f"user_{random.randint(1000, 9999)}",
        "device_id": generate_device_id(),
        "device_type": device_type,
        "platform": random.choice(PLATFORMS),
        "event_type": random.choice(EVENT_TYPES),
        "payload": payload
    }

def generate_session_event():
    """Generate a user session event."""
    global ACTIVE_SESSIONS

    # 70% chance to continue existing session, 30% new session
    if ACTIVE_SESSIONS and random.random() < 0.7:
        session = random.choice(ACTIVE_SESSIONS)
        session_id = session['session_id']
        user_id = session['user_id']
        platform = session['platform']
    else:
        session_id = fake.uuid4()
        user_id = f"user_{random.randint(1000, 9999)}"
        platform = random.choice(PLATFORMS)
        ACTIVE_SESSIONS.append({
            'session_id': session_id,
            'user_id': user_id,
            'platform': platform
        })
        # Keep only last 50 sessions
        if len(ACTIVE_SESSIONS) > 50:
            ACTIVE_SESSIONS = ACTIVE_SESSIONS[-50:]

    session_duration = random.randint(30, 3600)

    return {
        "session_id": session_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "user_id": user_id,
        "platform": platform,
        "device_type": random.choice(DEVICE_TYPES),
        "session_duration_seconds": session_duration,
        "pages_viewed": random.randint(1, 20),
        "interactions": random.randint(0, 50),
        "is_bounce": session_duration < 60 and random.random() < 0.3,
        "referrer": random.choice(["organic", "social", "direct", "email", "paid"]),
        "landing_page": f"/page/{random.choice(['home', 'products', 'about', 'contact', 'blog'])}"
    }

def generate_transaction_event():
    """Generate a transaction/purchase event."""
    product_categories = ["electronics", "clothing", "food", "books", "toys", "sports"]

    return {
        "transaction_id": fake.uuid4(),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "user_id": f"user_{random.randint(1000, 9999)}",
        "platform": random.choice(PLATFORMS),
        "product_id": f"prod_{random.randint(100, 999)}",
        "product_category": random.choice(product_categories),
        "quantity": random.randint(1, 5),
        "unit_price": round(random.uniform(9.99, 499.99), 2),
        "total_amount": 0,  # Will calculate
        "currency": "USD",
        "payment_method": random.choice(["credit_card", "debit_card", "paypal", "apple_pay", "google_pay"]),
        "shipping_country": fake.country_code(),
        "is_first_purchase": random.random() < 0.2
    }

def write_events_to_parquet(events, event_type="device"):
    """Write events to parquet file with timestamp-based filename."""
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    filename = f"{event_type}_events_{timestamp}.parquet"
    filepath = DATA_DIR / filename

    DATA_DIR.mkdir(parents=True, exist_ok=True)

    df = pd.DataFrame(events)

    # Calculate total_amount for transactions
    if event_type == "transaction" and 'unit_price' in df.columns:
        df['total_amount'] = df['unit_price'] * df['quantity']

    df.to_parquet(filepath, index=False, engine='pyarrow')

    # Detailed logging
    print(f"\n{'='*80}")
    print(f"[{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}] {event_type.upper()} BATCH GENERATED")
    print(f"{'='*80}")
    print(f"File: {filename}")
    print(f"Total Events: {len(events)}")
    print(f"{'='*80}\n")

def main():
    """Main event generation loop."""
    print(f"\n{'*'*80}")
    print("Event generator started")
    print(f"{'*'*80}")
    print(f"Output Directory: {DATA_DIR}")
    print(f"Generation Interval: {EVENT_INTERVAL} seconds")
    print("Events per batch: 10-100 (random)")
    print("Supported event types: device, session, transaction")
    print(f"{'*'*80}\n")

    batch_count = 0
    while True:
        batch_count += 1
        print(f"Generating batch #{batch_count}...")

        # Generate device events (most frequent)
        num_device_events = random.randint(20, 100)
        device_events = [generate_event() for _ in range(num_device_events)]
        write_events_to_parquet(device_events, "device")

        # Generate session events (medium frequency)
        num_session_events = random.randint(10, 50)
        session_events = [generate_session_event() for _ in range(num_session_events)]
        write_events_to_parquet(session_events, "session")

        # Generate transaction events (least frequent)
        num_transaction_events = random.randint(5, 20)
        transaction_events = [generate_transaction_event() for _ in range(num_transaction_events)]
        write_events_to_parquet(transaction_events, "transaction")

        print(f"Waiting {EVENT_INTERVAL} seconds until next batch...")
        time.sleep(EVENT_INTERVAL)

if __name__ == "__main__":
    main()

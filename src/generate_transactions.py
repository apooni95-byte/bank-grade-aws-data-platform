import json
import random
import uuid
from datetime import datetime, timedelta

PRODUCT_TYPES = ["ISA", "EasySaver", "FixedBond"]
CHANNELS = ["mobile", "web", "branch"]

def generate_transaction():
    transaction = {
        "transaction_id": str(uuid.uuid4()),
        "account_id": f"ACC{random.randint(10000, 99999)}",
        "product_type": random.choice(PRODUCT_TYPES),
        "transaction_ts": (
            datetime.now() - timedelta(minutes=random.randint(0, 1440))
        ).isoformat(),
        "amount": round(random.uniform(-5000, 5000), 2),
        "channel": random.choice(CHANNELS),
    }
    return transaction


def generate_transactions(n=1000):
    return [generate_transaction() for _ in range(n)]


if __name__ == "__main__":
    transactions = generate_transactions(1000)

    file_name = f"transactions_{datetime.now().date()}.json"

    with open(file_name, "w") as f:
        json.dump(transactions, f, indent=2)

    print(f"Generated {len(transactions)} transactions → {file_name}")

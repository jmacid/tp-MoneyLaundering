import sys
import os
from unittest.mock import MagicMock

# Mock pika
sys.modules['pika'] = MagicMock()

from operations.projectors.field_projector import FieldProjector
from operations.filters.currency_filter import CurrencyFilter
from operations.aggregators.local_bank_max_aggregator import LocalBankMaxAggregator

# Transacciones de prueba
transactions = [
    {"from_account": "ACC1", "to_bank": "bank_1", "to_account": "ACC2", 
     "amount_paid": 100.0, "payment_currency": "USD", "receiving_currency": "USD"},
    {"from_account": "ACC3", "to_bank": "bank_1", "to_account": "ACC4", 
     "amount_paid": 200.0, "payment_currency": "USD", "receiving_currency": "USD"},
    {"from_account": "ACC5", "to_bank": "bank_2", "to_account": "ACC6", 
     "amount_paid": 50.0,  "payment_currency": "EUR", "receiving_currency": "USD"},
    {"from_account": "ACC7", "to_bank": "bank_2", "to_account": "ACC8", 
     "amount_paid": 300.0, "payment_currency": "USD", "receiving_currency": "USD"},
]

# Mapping banco
bank_names = {
    "bank_1": "HSBC",
    "bank_2": "Santander",
}

# Paso 1: Currency Filter (primero, con transacción completa)
currency_filter = CurrencyFilter(currency="USD")
filtered = [tx for tx in transactions if currency_filter.process(tx)]
print("=== Currency Filtered ===")
for f in filtered:
    print(f)

# Paso 2: Projection (después, sobre las ya filtradas)
Q_2 = ["from_account", "to_bank", "amount_paid"]
projector = FieldProjector(Q_2)
projected = [projector.process(tx) for tx in filtered]
print("\n=== Projected ===")
for p in projected:
    print(p)

# Paso 3: Bank Dispatcher (simulado, sin sharding)
shards = {}
for tx in projected:
    bank = tx.get("to_bank")
    if bank not in shards:
        shards[bank] = []
    shards[bank].append(tx)
print("\n=== Bank Shards ===")
for bank, txs in shards.items():
    print(f"{bank}: {txs}")

# Paso 4: Local Bank Max Aggregator (simulado sin middleware)
max_per_bank = {}
for tx in projected:
    bank = tx.get("to_bank")
    amount = tx.get("amount_paid", 0)
    if bank not in max_per_bank or amount > max_per_bank[bank]["max_amount"]:
        max_per_bank[bank] = {
            "to_bank": bank,
            "from_account": tx.get("from_account"),
            "max_amount": amount
        }
print("\n=== Local Max Per Bank ===")
for bank, data in max_per_bank.items():
    print(data)

# Paso 5: Bank Resolver (simulado sin middleware)
print("\n=== Final Result ===")
for bank, data in max_per_bank.items():
    bank_name = bank_names.get(bank, "UNKNOWN")
    print({
        "bank_name": bank_name,
        "from_account": data["from_account"],
        "max_amount": data["max_amount"]
    })
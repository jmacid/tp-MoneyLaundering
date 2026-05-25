import sys
from unittest.mock import MagicMock, patch
sys.modules['pika'] = MagicMock()

import os
os.environ["OUTPUT_QUEUE"] = "test_queue"
os.environ["RABBITMQ_HOST"] = "localhost"

from operations.aggregators.local_bank_max_aggregator import LocalBankMaxAggregator

def test_single_client():
    print("\n=== Test: Single Client ===")
    aggregator = LocalBankMaxAggregator()
    aggregator.middleware = MagicMock()  # mockear el middleware

    aggregator.process({"client_id": "client_1", "to_bank": "bank_1", "from_account": "ACC1", "amount_paid": 100.0})
    aggregator.process({"client_id": "client_1", "to_bank": "bank_1", "from_account": "ACC2", "amount_paid": 200.0})
    aggregator.process({"client_id": "client_1", "to_bank": "bank_2", "from_account": "ACC3", "amount_paid": 50.0})

    aggregator.flush("client_1")

    calls = aggregator.middleware.send.call_args_list
    print(f"Mensajes enviados: {len(calls)}")
    for call in calls:
        print(f"  {call}")

    assert len(calls) == 2, f"Esperaba 2 mensajes, recibi {len(calls)}"
    print("PASSED")

def test_multiple_clients_isolated():
    print("\n=== Test: Multiple Clients Isolated ===")
    aggregator = LocalBankMaxAggregator()
    aggregator.middleware = MagicMock()

    # client_1 y client_2 intercalados
    aggregator.process({"client_id": "client_1", "to_bank": "bank_1", "from_account": "ACC1", "amount_paid": 100.0})
    aggregator.process({"client_id": "client_2", "to_bank": "bank_1", "from_account": "ACC3", "amount_paid": 500.0})
    aggregator.process({"client_id": "client_1", "to_bank": "bank_1", "from_account": "ACC2", "amount_paid": 200.0})
    aggregator.process({"client_id": "client_2", "to_bank": "bank_1", "from_account": "ACC4", "amount_paid": 300.0})

    # Flush client_1
    aggregator.flush("client_1")
    calls_after_client1 = aggregator.middleware.send.call_args_list
    print(f"Mensajes enviados tras flush client_1: {len(calls_after_client1)}")
    for call in calls_after_client1:
        print(f"  {call}")

    # Verificar que client_2 sigue vivo
    assert "client_2" in aggregator.max_amounts, "client_2 no deberia haber sido limpiado"
    assert "client_1" not in aggregator.max_amounts, "client_1 deberia haber sido limpiado"

    # Flush client_2
    aggregator.flush("client_2")
    calls_after_client2 = aggregator.middleware.send.call_args_list
    print(f"Mensajes enviados tras flush client_2: {len(calls_after_client2)}")
    for call in calls_after_client2:
        print(f"  {call}")

    assert len(calls_after_client2) == 2, f"Esperaba 2 mensajes totales, recibi {len(calls_after_client2)}"
    print("PASSED")

def test_max_is_correct():
    print("\n=== Test: Max Amount is Correct ===")
    import json
    aggregator = LocalBankMaxAggregator()
    aggregator.middleware = MagicMock()

    aggregator.process({"client_id": "client_1", "to_bank": "bank_1", "from_account": "ACC1", "amount_paid": 100.0})
    aggregator.process({"client_id": "client_1", "to_bank": "bank_1", "from_account": "ACC2", "amount_paid": 999.0})
    aggregator.process({"client_id": "client_1", "to_bank": "bank_1", "from_account": "ACC3", "amount_paid": 50.0})

    aggregator.flush("client_1")

    call = aggregator.middleware.send.call_args_list[0]
    result = json.loads(call[0][0])

    print(f"Resultado: {result}")
    assert result["max_amount"] == 999.0, f"Esperaba 999.0, recibi {result['max_amount']}"
    assert result["from_account"] == "ACC2", f"Esperaba ACC2, recibi {result['from_account']}"
    print("PASSED")

def test_flush_nonexistent_client():
    print("\n=== Test: Flush Nonexistent Client ===")
    aggregator = LocalBankMaxAggregator()
    aggregator.middleware = MagicMock()

    # No deberia explotar
    aggregator.flush("client_inexistente")
    assert aggregator.middleware.send.call_count == 0
    print("PASSED")

if __name__ == "__main__":
    test_single_client()
    test_multiple_clients_isolated()
    test_max_is_correct()
    test_flush_nonexistent_client()
    print("\n=== Todos los tests pasaron ===")
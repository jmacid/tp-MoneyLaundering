import sys
from unittest.mock import MagicMock
sys.modules['pika'] = MagicMock()

import os
os.environ["OUTPUT_QUEUE"] = "test_queue"
os.environ["RABBITMQ_HOST"] = "localhost"

import json
from operations.resolver.bank_resolver import BankResolver
from domain.message_type import MessageType

def test_resolves_bank_name():
    print("\n=== Test: Resolves Bank Name ===")
    resolver = BankResolver()
    resolver.output_middleware = MagicMock()

    # Cargar mapping
    resolver.handle({"type": MessageType.BANK_NAME, "client_id": "client_1", "bank_id": "bank_1", "bank_name": "HSBC"})
    resolver.handle({"type": MessageType.BANK_NAME, "client_id": "client_1", "bank_id": "bank_2", "bank_name": "Santander"})

    # Procesar transaccion
    resolver.handle({"type": MessageType.TRANSACTION, "client_id": "client_1", "to_bank": "bank_1", "from_account": "ACC1", "max_amount": 200.0})

    call = resolver.output_middleware.send.call_args_list[0]
    result = json.loads(call[0][0])
    print(f"Resultado: {result}")

    assert result["bank_name"] == "HSBC", f"Esperaba HSBC, recibi {result['bank_name']}"
    assert result["from_account"] == "ACC1"
    assert result["max_amount"] == 200.0
    print("PASSED")

def test_multiple_clients_isolated():
    print("\n=== Test: Multiple Clients Isolated ===")
    resolver = BankResolver()
    resolver.output_middleware = MagicMock()

    # Mappings de dos clientes distintos
    resolver.handle({"type": MessageType.BANK_NAME, "client_id": "client_1", "bank_id": "bank_1", "bank_name": "HSBC"})
    resolver.handle({"type": MessageType.BANK_NAME, "client_id": "client_2", "bank_id": "bank_1", "bank_name": "Santander"})

    # Procesar transacciones de cada cliente
    resolver.handle({"type": MessageType.TRANSACTION, "client_id": "client_1", "to_bank": "bank_1", "from_account": "ACC1", "max_amount": 100.0})
    resolver.handle({"type": MessageType.TRANSACTION, "client_id": "client_2", "to_bank": "bank_1", "from_account": "ACC2", "max_amount": 200.0})

    calls = resolver.output_middleware.send.call_args_list
    result_1 = json.loads(calls[0][0][0])
    result_2 = json.loads(calls[1][0][0])

    print(f"Resultado client_1: {result_1}")
    print(f"Resultado client_2: {result_2}")

    assert result_1["bank_name"] == "HSBC"
    assert result_2["bank_name"] == "Santander"
    print("PASSED")

def test_flush_cleans_state():
    print("\n=== Test: Flush Cleans State ===")
    resolver = BankResolver()
    resolver.output_middleware = MagicMock()

    resolver.handle({"type": MessageType.BANK_NAME, "client_id": "client_1", "bank_id": "bank_1", "bank_name": "HSBC"})
    assert "client_1" in resolver.bank_names

    resolver.flush("client_1")
    assert "client_1" not in resolver.bank_names
    print("PASSED")

def test_unknown_bank_raises_error():
    print("\n=== Test: Unknown Bank Raises Error ===")
    resolver = BankResolver()
    resolver.output_middleware = MagicMock()

    try:
        resolver.handle({"type": MessageType.TRANSACTION, "client_id": "client_1", "to_bank": "bank_inexistente", "from_account": "ACC1", "max_amount": 100.0})
        print("FAILED: deberia haber lanzado ValueError")
    except ValueError as e:
        print(f"Correctamente lanzó ValueError: {e}")
        print("PASSED")

if __name__ == "__main__":
    test_resolves_bank_name()
    test_multiple_clients_isolated()
    test_flush_cleans_state()
    test_unknown_bank_raises_error()
    print("\n=== Todos los tests pasaron ===")
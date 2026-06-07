# Cambios del sistema

## Protocolo de mensajes (MsgType)

**Antes:**
- `TRANSACTION_RECORD` — enviaba una transacción por mensaje
- `ACK` — sin payload
- `END_OF_RECORDS`
- `MINOR_RESULT`

**Ahora:**
- `BATCH_RECORD` — reemplaza a `TRANSACTION_RECORD`, envía un batch de líneas crudas CSV
- `BANK_MAPPING` — nuevo, envía un batch del archivo de mapeo de bancos
- `ACK` — ahora lleva el `sequence_number` del batch confirmado
- `END_OF_RECORDS`
- `MINOR_RESULT`
- `ACK_EOF` — nuevo, confirmación específica del END_OF_RECORDS del cliente, sin payload

---

## Batch

**Antes:** no existía, se enviaba fila por fila.

**Ahora:** es la unidad de envío. Tiene:
- `client_id` — identifica al cliente que lo generó
- `sequence_number` — número ordinal dentro del archivo, arranca en 0
- `lines` — lista de strings, cada uno es una línea cruda del CSV
- `is_last` — indica si es el último batch del archivo

El tamaño del batch se configura por variable de entorno `BATCH_SIZE_BYTES`.
Si una línea sola supera el tamaño configurado, se emite igual con un warning.

---

## Identificación única de un batch

No hay un campo `id` separado. El identificador único en el sistema es `(client_id, sequence_number)`.

---

## Quién arma los batches

El `BatchSplitter` vive dentro del cliente (`batch_splitter.py`). La función `build_batches(file_path, client_id)` es un generador que nunca tiene más de un batch en memoria a la vez.

Sirve tanto para transacciones como para bank mapping. El `MsgType` lo decide el caller al momento de enviar, no el batch en sí.

Con multiples clientes corriendo en paralelo, cada uno llama a la función *build_batches* y tendra su propio generador sin interferir con los demás.

---

## ACKs

**Antes:** un ACK por cada `TRANSACTION_RECORD`, sin payload.

**Ahora:**
- Un ACK por batch, lleva el `sequence_number` del batch confirmado
- El gateway manda ACK para `BATCH_RECORD` y `BANK_MAPPING`
- El `END_OF_RECORDS` que manda el cliente recibe un `ACK_EOF` como confirmación
- Si no llega el `ACK_EOF`, el cliente reenvía el `END_OF_RECORDS` hasta recibirlo, con el mismo `ACK_TIMEOUT_SECONDS` configurado

---

## Manejo del EOF

**Antes:** el cliente mandaba `END_OF_RECORDS` y esperaba un ACK.

**Ahora:** hay dos EOFs distintos:

**EOF del cliente → gateway:**
- El cliente manda `END_OF_RECORDS` cuando terminó de enviar todos los batches y tiene la `PendingBatchesTable` vacía
- El gateway responde con `ACK_EOF` una vez que procesó el cierre y mandó el mensaje al `control_queue`
- Si el cliente no recibe el `ACK_EOF` dentro de `ACK_TIMEOUT_SECONDS`, reenvía el `END_OF_RECORDS`

**EOF del gateway → cliente:**
- El servidor manda `END_OF_RECORDS` cuando terminó de procesar una query
- El cliente espera 5 EOFs (uno por query), configurable via `EXPECTED_EOFS` (por sí en el futuro existieran más querys)
- Cuando llegan los 5, el receiver thread termina

---

## Envío concurrente (sender/receiver threads)

**Antes:** el cliente enviaba una fila, esperaba el ACK, enviaba la siguiente. Completamente secuencial.

**Ahora:** dos threads dentro del cliente:
- **sender thread** — manda batches, chequea timeouts y reenvía los expirados
- **receiver thread** — escucha ACKs y MINOR_RESULTs en paralelo

Se coordinan a través de `PendingBatchesTable`, un dict thread-safe `{sequence_number: PendingBatch}`.

Cada `PendingBatch` tiene:
- `batch` — para poder reenviarlo
- `sent_at` — timestamp del último envío
- `retries` — cantidad de reintentos

---

## Retry de batches

**Antes:** un timeout por fila, bloqueante.

**Ahora:**
- El sender chequea `PendingBatchesTable` en cada iteración
- Si un batch superó `ACK_TIMEOUT_SECONDS` sin recibir ACK, lo reenvía
- Si superó `MAX_RETRIES`, loggea warning y lo descarta
- Configurable: `ACK_TIMEOUT_SECONDS`, `MAX_RETRIES`

---

## Ventana de batches en vuelo

El sender se bloquea si hay demasiados batches pendientes de ACK. Configurable via `MAX_PENDING_BATCHES`.

---

## Bank mapping

**Antes:** el gateway leía un archivo CSV local al inicio de cada conexión y lo mandaba a los resolvers antes de procesar transacciones.

**Ahora:**
- El cliente manda el bank mapping como batches con `MsgType.BANK_MAPPING`
- Puede llegar en cualquier momento, no necesariamente al inicio
- El gateway lo reenvía batch a batch al `BankResolver` via fanout exchange
- El `BankResolver` acumula las líneas en `self.bank_names_map` batch a batch hasta recibir `is_last=True`
- Si llega un batch de mapping duplicado (por retry del cliente), el `update` del dict es idempotente

---

## Exchange para BankResolver

**Antes:** el gateway instanciaba N exchanges direct, uno por réplica del resolver, y mandaba el mapping N veces en un loop.

**Ahora:** un solo exchange fanout. El gateway manda una vez y RabbitMQ se encarga de distribuirlo a todas las réplicas conectadas.

Esto implicó separar el middleware en dos clases:
- `MessageMiddlewareExchangeDirectRabbitMQ` — renombrada de la clase original
- `MessageMiddlewareExchangeFanoutRabbitMQ` — nueva, sin routing keys

---

## Variables de entorno nuevas (cliente)

| Variable | Default | Descripción |
|---|---|---|
| `CLIENT_ID` | — | Identificador del cliente |
| `BATCH_SIZE_BYTES` | 1024 | Tamaño máximo de cada batch |
| `ACK_TIMEOUT_SECONDS` | 5.0 | Tiempo de espera antes de reenviar |
| `MAX_RETRIES` | 3 | Reintentos antes de descartar un batch |
| `MAX_PENDING_BATCHES` | 10 | Máximo de batches sin ACK en simultáneo |
| `EXPECTED_EOFS` | 5 | EOFs del servidor esperados antes de terminar |
| `BANK_MAPPING_FILE` | — | Path al CSV de mapeo de bancos |
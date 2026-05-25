# tp-MoneyLaundering

## Query 2 - Max Transaction per Bank

### Objetivo
Obtener el nombre de banco, cuenta de origen y monto de la máxima transacción USD de cada banco.

---

### Flujo de datos

Gateway --> (transaction) --> Projection Dispatcher --> (max_query) --> Current Filter --> (USD_transactions) --> Bank Dispatcher --> (bank_shards) --> Local Bank Max Aggregator --> (max_bank_transactions) y (bankname_shards)--> Bank resolver --> (bank_max_transactions) --> Gateway

---

### Decisiones de diseño

**Eliminación de Bank Max Aggregator y Bank Max Joiner**

El Bank Dispatcher hace sharding por hash de banco, lo que garantiza que todas las transacciones de un mismo banco siempre van al mismo Local Bank Max Aggregator. Por lo tanto cada instancia del aggregator ya computa el máximo global de sus bancos, sin necesidad de una etapa adicional de consolidación.

---

### Protocolo de mensajes

Se introduce un campo `"type"` en todos los mensajes del sistema para que cada componente pueda identificar cómo procesarlos:

| Tipo | Descripción |
|---|---|
| `transaction` | Datos de una transacción a procesar |
| `bank_name` | Mapping de `bank_id` → `bank_name` enviado por el Gateway al Bank Resolver vía fanout |
| `eof` | Señal de fin de stream para un cliente |

---

### Componentes

**Currency Filter** — Filtra transacciones, conservando solo las realizadas en USD.

**Projection Dispatcher** — Recorta los campos necesarios para Query 2: `from_account`, `to_bank`, `amount_paid`.

**Bank Dispatcher** — Hace sharding por hash de banco usando MD5 para garantizar determinismo entre instancias. Todas las transacciones del mismo banco siempre van al mismo shard.

**Local Bank Max Aggregator** — Acumula el máximo `amount_paid` por banco dentro de su shard. Envía resultados al recibir el EOF del cliente.

**Bank Resolver** — Recibe el mapping `bank_id → bank_name` vía fanout broadcast (todas las instancias reciben el mapping completo) y enriquece cada resultado antes de enviarlo al Gateway.

TODO: Ver bien tema EOF
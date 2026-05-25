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

**Aislamiento de estado por cliente**

Los componentes con estado (`LocalBankMaxAggregator` y `BankResolver`) mantienen su estado separado por `client_id`. Esto permite que múltiples clientes corran concurrentemente sin que sus datos se mezclen. Al recibir el EOF de un cliente, se hace flush solo de ese cliente y se limpia su estado sin afectar a los demás.

### Protocolo de mensajes

Se introduce un campo `"type"` en todos los mensajes del sistema para que cada componente pueda identificar cómo procesarlos:

| Tipo | Descripción |
|---|---|
| `transaction` | Datos de una transacción a procesar |
| `bank_name` | Mapping de `bank_id` → `bank_name` enviado por el Gateway al Bank Resolver vía fanout |
| `eof` | Señal de fin de stream para un cliente |
| `ready` | Respuesta de un worker al EOF Handler indicando que terminó de procesar |

Todos los mensajes incluyen además `client_id` para identificar a qué cliente pertenecen.

### EOF Handler

El EOF Handler es un nodo separado y único que coordina el fin de procesamiento de cada cliente a través de toda la pipeline.

**Flujo del EOF:**

1) Gateway detecta fin de stream de un cliente
2) Gateway → EOF(client_id, query_id) → EOF Handler
3) EOF Handler → EOF(client_id, query_id) → Nodo 1 (todas sus instancias)
4) Nodo 1 termina de procesar y hace flush()
5) Nodo 1 → Ready(client_id, query_id, node_name) → EOF Handler
6) EOF Handler espera Readys de todas las instancias del Nodo 1
7) EOF Handler → EOF(client_id, query_id) → Nodo 2
8) ... y así hasta el último nodo
9) EOF Handler → EOF(client_id) → Gateway (pipeline completa)
---

**Consideraciones de diseño (EOF)**

- El EOF Handler es una **única instancia** (por ahora) para evitar problemas de coordinación entre instancias (duplicación de EOFs, routing de Readys, etc.)
- El EOF viaja por una **cola separada** de los datos, garantizando que no se mezcla con las transacciones en tránsito
- Los datos siguen viajando por sus colas normales (`max_query`, `USD_transactions`, etc.)
- El EOF Handler avanza al siguiente nodo solo cuando recibe tantos Readys como instancias tiene el nodo actual
- Los nodos con estado (`stateful: true`) hacen `flush()` antes de responder Ready, enviando sus resultados acumulados
- Los nodos sin estado (`stateful: false`) responden Ready inmediatamente ya que no acumulan datos

---

**Topología de la pipeline:**

La topología se define en `eof_handler/pipeline_config.yaml`. Cada query tiene su propia sección.

**Colas del EOF Handler:**

| Cola | Dirección | Descripción |
|---|---|---|
| `eof_handler` | entrada | Recibe EOFs del Gateway |
| `eof_<nodo>_<query>` | salida | Envía EOF a cada nodo |
| `ready_<nodo>_<query>` | entrada | Recibe Readys de cada nodo |
| `gateway` | salida | Notifica al Gateway que terminó |

---

### Componentes

**Projection Dispatcher** — Separa las ramas de cada query proyectando los campos necesarios. Para Query 2 conserva: `from_account`, `to_bank`, `amount_paid`, `payment_currency`, `receiving_currency`.

**Currency Filter** — Filtra transacciones conservando solo las realizadas en USD. Opera sobre la transacción ya proyectada.

**Bank Dispatcher** — Hace sharding por hash MD5 de banco. Todas las transacciones del mismo banco siempre van al mismo shard garantizando determinismo entre instancias.

**Local Bank Max Aggregator** — Acumula el máximo `amount_paid` por banco dentro de su shard, con estado separado por `client_id`. Al recibir el EOF hace flush del cliente correspondiente y limpia su estado.

**Bank Resolver** — Recibe el mapping `bank_id → bank_name` vía fanout broadcast (todas las instancias reciben el mapping completo) con estado separado por `client_id`. Enriquece cada resultado con el nombre del banco antes de enviarlo al Gateway. Al recibir el EOF limpia el estado del cliente.
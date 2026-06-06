# tp-MoneyLaundering

## Verification tools

El proyecto incluye scripts de verificación para generar los outputs esperados de cada regla y compararlos contra los outputs reales generados por el TP.

Los scripts se encuentran en:

```text
src/tools/verification/
```

Cada regla tiene su propio verificador:

```text
verify_rule_1.py
verify_rule_2.py
verify_rule_3.py
verify_rule_4.py
verify_rule_5.py
```

---

### 1. Generar outputs esperados

Para generar todos los archivos esperados:

```bash
make expected-all
```

Esto genera los expected de las reglas 1 a 5 usando como input por defecto:

```text
test/data/INPUT_FILE.csv
```

Los archivos esperados se generan en:

```text
src/tools/verification/
```

También se puede generar el expected de una regla puntual:

```bash
make expected-rule-1
make expected-rule-2
make expected-rule-3
make expected-rule-4
make expected-rule-5
```

Ejemplo:

```bash
make expected-rule-3
```

---

### 2. Comparar outputs esperados contra outputs reales

Para comparar todas las reglas contra las salidas reales del TP:

```bash
make compare-all
```

También se puede comparar una regla puntual:

```bash
make compare-rule-1
make compare-rule-2
make compare-rule-3
make compare-rule-4
make compare-rule-5
```

Ejemplo:

```bash
make compare-rule-1
```

En este caso, el script usa:

```text
expected output = archivo esperado generado por el verificador
actual output   = archivo real generado por el TP
```

La comparación no depende del orden de las filas. Internamente, los archivos se comparan como un multiset, por lo que también se respetan duplicados.

Esto permite detectar:

- Filas faltantes.
- Filas inesperadas.
- Diferencias en la cantidad de apariciones de una misma fila.

---

### 3. Generar y comparar en un solo comando

Los comandos `compare-rule-x` primero generan el output esperado y luego comparan contra el output real.

Por ejemplo:

```bash
make compare-rule-1
```

equivale a:

```text
1. Leer el archivo original de transacciones.
2. Generar el expected de la regla 1.
3. Leer el output real generado por el TP.
4. Comparar expected vs actual.
```

También existen aliases equivalentes con `verify`:

```bash
make verify-all
make verify-rule-1
make verify-rule-2
make verify-rule-3
make verify-rule-4
make verify-rule-5
```

Ejemplo:

```bash
make verify-rule-2
```

---

### 4. Ejecutar usando rutas custom

Las rutas se pueden modificar desde la consola sin cambiar el `Makefile`.

#### Cambiar el archivo de entrada

```bash
make expected-all INPUT_FILE="/path/to/HI-Medium_Trans.csv"
```

Ejemplo:

```bash
make expected-all INPUT_FILE="/home/franco/Desktop/Sistemas Distribuidos I/tp-final/test/data/HI-Medium_Trans.csv"
```

#### Cambiar la salida esperada de una regla

```bash
make expected-rule-1 \
  INPUT_FILE="/path/to/HI-Medium_Trans.csv" \
  RULE_1_EXPECTED="/path/to/RULE_1_expected.csv"
```

#### Cambiar el archivo real a comparar

```bash
make compare-rule-1 \
  RULE_1_ACTUAL="output/minor_transactions.csv"
```

#### Cambiar input, expected y actual en una comparación

```bash
make compare-rule-3 \
  INPUT_FILE="/path/to/HI-Medium_Trans.csv" \
  RULE_3_EXPECTED="/path/to/RULE_3_expected.csv" \
  RULE_3_ACTUAL="output/rule_3_output.csv"
```

---

### 5. Variables disponibles

Archivo input original:

```makefile
INPUT_FILE
```

Archivos esperados:

```makefile
RULE_1_EXPECTED
RULE_2_EXPECTED
RULE_3_EXPECTED
RULE_4_EXPECTED
RULE_5_EXPECTED
```

Archivos reales generados por el TP:

```makefile
RULE_1_ACTUAL
RULE_2_ACTUAL
RULE_3_ACTUAL
RULE_4_ACTUAL
RULE_5_ACTUAL
```

---

### 6. Rule 5 y tipos de cambio

La regla 5 requiere convertir montos a USD.

Para eso, el script `verify_rule_5.py` consulta la API de Frankfurter una sola vez para traer los tipos de cambio del período:

```text
[2022-09-01, 2022-09-05]
```

Luego reutiliza esos tipos de cambio en memoria para procesar todas las transacciones.

Si la API no está disponible o devuelve error, la generación del expected de la regla 5 puede fallar.

---

### 7. Ayuda

Para ver los comandos disponibles:

```bash
make verify-help
```

---

### 8. Resumen de comandos útiles

```bash
# Generar todos los expected
make expected-all

# Generar el expected de una regla
make expected-rule-1

# Comparar todas las reglas
make compare-all

# Comparar una regla puntual
make compare-rule-1 RULE_1_ACTUAL="output/minor_transactions.csv"

# Generar y comparar todas las reglas
make verify-all

# Ver ayuda de verificación
make verify-help
```


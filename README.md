# ETL — Análisis de Clientes, Contratos y Uso

Proyecto de análisis de datos sobre tres tablas relacionadas: clientes, contratos y registros de uso. El objetivo era entender los datos, limpiarlos y extraer conclusiones de negocio útiles.

---

## Estructura de los datos

Trabajé con tres archivos CSV:

| Tabla | Registros | Descripción |
|---|---|---|
| `customers` | 5.000 | Clientes con categoría y score interno |
| `contracts` | 5.500 | Contratos vinculados a clientes |
| `usage` | 60.000 | Eventos de consumo por contrato |

### Relaciones entre tablas

```
customers
  └── customer_id ──► contracts
                          └── contract_id ──► usage
```

`usage` no tiene relación directa con `customers`. Para cruzarlas hay que pasar siempre por `contracts`.

---

## Fase 1 — Descubrimiento y comprensión de los datos

Lo primero fue entender qué significaba cada columna, identificar las claves primarias y ver cómo se relacionaban las tablas.

### Renombrado de columnas

Los nombres originales eran crípticos, así que los traduje a algo más descriptivo:

```python
contracts.rename(columns={
    "con_id":   "contract_id",  # PK única
    "cli_ref":  "customer_id",  # FK → customers
    "p_type":   "plan_type",    # F1, F2, M1, M2
    "val_mo":   "monthly_value",
    "st":       "status",       # 1=activo, 0=inactivo, -1=cancelado
    "dt_start": "start_date",
}, inplace=True)

customers.rename(columns={
    "id_cli":       "customer_id",
    "full_name":    "name",
    "contact_info": "email",
    "reg_date":     "registration_date",
    "cat":          "category",        # A, B, C, X
    "int_score":    "internal_score",  # 0–100
}, inplace=True)

usage.rename(columns={
    "u_id":  "usage_id",
    "c_ref": "contract_id",  # FK → contracts
    "qty":   "quantity",
    "ts":    "timestamp",
    "loc":   "location",
}, inplace=True)
```

### Claves primarias y relaciones

```python
# Verificar que las PKs son únicas
assert contracts["contract_id"].is_unique  # ✅
assert usage["usage_id"].is_unique         # ✅
# customers["customer_id"].is_unique       # ❌ tiene duplicados → investigar
```

---

## Fase 2 — Detección y resolución de anomalías

### Customers — customer_id duplicado

`C_00001` aparecía 7 veces con emails distintos, es decir, 7 personas distintas con el mismo ID asignado por error. No era un historial de cambios (SCD) porque los emails eran todos diferentes.

**Solución:** reconstruir el `customer_id` a partir del número del campo `name`:

```python
numero_name = customers["name"].str.extract(r"(\d+)")[0]
customers["customer_id"] = "C_" + numero_name.str.zfill(5)
# Resultado: 5.000 registros con PK única garantizada
```

### Customers — emails inválidos

Había emails con el patrón `invalid_N` que no correspondían con el número del nombre del cliente.

```python
num_name = customers["name"].str.extract(r"(\d+)")[0]
email_esperado = "user_" + num_name + "@domain.com"
errores = customers["email"] != email_esperado

customers.loc[errores, "email"] = "user_" + num_name[errores] + "@domain.com"
# Resultado: 0 emails inválidos
```

### Contracts — monthly_value con valores anómalos

El `describe()` mostraba un mínimo de `-1` y un máximo de `9999.99` con una desviación estándar de 444 sobre una media de 88. Eso no tenía sentido.

Después de analizar los 20 registros afectados vi que no seguían ningún patrón por plan ni por status → eran errores de carga, no códigos de negocio.

```python
condicion = (contracts["monthly_value"] < 0) | (contracts["monthly_value"] >= 9999)
contracts["monthly_value"] = contracts["monthly_value"].mask(condicion, np.nan)
# 20 valores convertidos a NaN
```

El `describe()` tras la limpieza quedó razonable: media 68, std 28, rango 0–150.

### Contracts — columna status

Mapeé los valores numéricos a texto para trabajar más cómodamente:

```python
contracts["status_name"] = contracts["status"].map({
    1:  "activo",
    0:  "inactivo",
    -1: "cancelado"
})
# activo: 4.386 | inactivo: 528 | cancelado: 586
```

### Usage — contract_id CON_999999

501 registros de uso apuntaban a un contrato que no existía en `contracts`. Era un valor placeholder sin información útil.

```python
usage = usage[usage["contract_id"] != "CON_999999"]
# De 60.000 → 59.499 registros
```

### Usage — quantity con anomalías

- **Valores negativos** (58 registros, `quantity = -5`): sin contexto de negocio suficiente para confirmar si eran devoluciones → eliminados.
- **Valor centinela 1440** (= 60 min × 24h): tope de campo, posible valor real de consumo → no elimino, es posible ese consumo alto

```python
usage = usage[usage["quantity"] != -5]
# Distribución resultante no cambia mucho ya que estos datos representan menos del 1% del total
```

---

## Fase 3 — Inconsistencias lógicas cruzadas

### Contratos con fecha anterior al registro del cliente

Detecté 2.538 contratos cuya `start_date` era anterior a la `registration_date` del cliente. En un sistema real esto no debería pasar.

```python
merged = contracts.merge(customers[["customer_id", "registration_date"]])
errores_fecha = merged[merged["start_date"] < merged["registration_date"]]
# 2.538 contratos afectados — documentado como supuesto de migración histórica
```

### Clientes sin actividad reciente

Identifiqué clientes que, en los últimos 90 días, no tenían contrato activo ni uso registrado.

```python
fecha_corte = usage["timestamp"].max() - pd.Timedelta(days=90)

contratos_activos = contracts[contracts["status_name"] == "activo"]["customer_id"]
sin_contrato = customers[~customers["customer_id"].isin(contratos_activos)]

ultimo_uso_cliente = (
    usage[usage["quantity"] > 0]
    .merge(contracts[["contract_id", "customer_id"]], on="contract_id")
    .groupby("customer_id")["timestamp"].max()
    .reset_index()
)

sin_uso_reciente = customers.merge(ultimo_uso_cliente, on="customer_id", how="left")
sin_uso_reciente = sin_uso_reciente[
    sin_uso_reciente["timestamp"].isna() |
    (sin_uso_reciente["timestamp"] < fecha_corte)
]
```

---

## Fase 4 — Análisis de negocio y KPIs

### ARPU por categoría y tipo de plan

Calculado solo sobre contratos activos. La correlación entre `monthly_value` y las variables categóricas era inferior a 0.03 en todos los casos → el precio no está determinado por el segmento ni el plan.

```python
contratos_activos = contracts[contracts["status_name"] == "activo"]
arpu = contratos_activos.merge(customers[["customer_id", "category"]], on="customer_id")

arpu_segmentado = arpu.groupby(["category", "plan_type"])["monthly_value"].mean()
# Top: C+M1 (74.09), C+M2 (73.71), A+M1 (70.27)
```

### Patrones de consumo por ubicación y plan

```python
df_uso_planes = usage.merge(contracts[["contract_id", "plan_type"]], on="contract_id")
patrones = df_uso_planes.groupby(["location", "plan_type"])["quantity"].agg(["mean", "sum"])

# Por región:  DE > US > FR > ?? > ES > UK
# Por plan:    F1 > M1 > F2 > M2
# Diferencia máxima entre regiones: 8.2% | entre planes: 10.2%
```

### Tasa de churn por plan y categoría

```python
contracts["is_churn"] = contracts["status"] == -1

churn = contracts.merge(customers[["customer_id", "category"]], on="customer_id")
reporte_churn = churn.groupby(["plan_type", "category"])["is_churn"].mean().reset_index()
reporte_churn["churn_%"] = (reporte_churn["is_churn"] * 100).round(2)

# Churn global: 19.56%
# Top 3: M2+C (27.46%), F1+A (22.22%), M2+A (21.36%)
# Menor: F2+X (13.33%)
```

### Clientes de alto valor (VIP)

Definí como VIP a los clientes activos con `monthly_value` y `internal_score` superiores a la media.

```python
ingreso_promedio = contracts["monthly_value"].mean()
score_promedio   = customers["internal_score"].mean()

clientes_vip = (
    contracts[contracts["status"] == 1]
    .merge(customers[["customer_id", "category", "internal_score"]], on="customer_id")
    .query("monthly_value > @ingreso_promedio and internal_score > @score_promedio")
)
# 58 clientes VIP — categoría A domina (38/58)
```

### Perfiles de comportamiento anómalo

```python
# Dormidos: activos que pagan pero casi no usan
dormidos = contracts.merge(uso_total).query("status == 1 and quantity < umbral_bajo")

# Poco rentables: pagan menos del ARPU ref. pero consumen más que el top
poco_rentable = consumo_por_contrato[
    (consumo_por_contrato["monthly_value"] <= 74.09) &
    (consumo_por_contrato["quantity"] >= uso_referencia_top)
]
# 1.274 contratos con potencial de upgrade
```

---

## Fase 5 — Análisis de cohorts y correlaciones

```python
# Cohort mensual por fecha de registro
customers["cohort_month"] = customers["registration_date"].dt.to_period("M")

df_cohorts = customers.merge(
    contracts[["customer_id", "monthly_value", "status"]], on="customer_id"
).query("status == 1")

analisis_mensual = df_cohorts.groupby("cohort_month").agg(
    num_clientes  = ("customer_id", "count"),
    revenue_total = ("monthly_value", "sum"),
    ARPU          = ("monthly_value", "mean")
)
```

---

## Conclusiones y recomendaciones

### 1 — Retención en segmentos de alto churn
M2+C (27.46%) y F1+A (22.22%) superan en más de 7 puntos la media global. Los planes mensuales dan más flexibilidad para cancelar. Propuesta: activar campañas de retención a los 60 días de inactividad e incentivar la migración de plan M a plan F.

### 2 — Upgrade de clientes poco rentables
1.274 contratos están en planes baratos pero consumen por encima del top. Hay margen directo de subida de precio o migración a un plan superior. El campo `potencial_pago` calculado muestra el ingreso que se deja de capturar por contrato.

### 3 — Activación de clientes dormidos
168 clientes con contrato activo llevan más de 30 días sin uso. Pagan pero no consumen → candidatos a churn en la próxima renovación. Primer paso: NPS survey a los 30 días de inactividad.

### 4 — Fidelización de categoría X
Es el segmento más fiel en todos los planes (churn mínimo 13.33%) pero está infrarepresentado entre los VIP. Tiene sentido entender qué los hace tan retenidos y replicarlo, además de subirles el valor con upselling.

---

## Stack utilizado

- **Python 3** + **pandas** + **numpy**
- **Jupyter Notebook**

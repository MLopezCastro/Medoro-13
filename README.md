
---

# Medoro 13 – Rolling Window Optimization for SQL Server (3-Year Window)

## 1. Overview

This project implements a **rolling 3-year window** over the production events stored in `ConCubo`, optimizing both:

* **A “blocks” view** (≈ 17K rows) – daily aggregated sequences by machine & order.
* **A detailed “events” view** (≈ 2.4M rows) – every production/preparation/stop/maintenance event.

Both pipelines:

* Keep **exactly 3 years of history** (rolling purge).
* Use a **time-window function** with:

  * Local timezone: `Argentina Standard Time`.
  * Date correction: **–2 days** on raw `Inicio` / `Fin`.
* Provide **incremental refresh** via stored procedures:

  * `RefrescarV5_Incremental` – blocks path.
  * `RefrescarV5E_Incremental` – events path.

These outputs feed **Power BI** using SQL statements in *Advanced options*.

---

## 2. Main objects

### 2.1 Source

* `dbo.ConCubo`
  Raw events from the production system (Inicio, Fin, Estado, ID, Renglon, quantities, operators, etc.).

* `dbo.TablaVinculadaUNION`
  Provides `saccod1` per numeric `OP`.

* `dbo.TablaVinculadaNEW`
  Provides: `OP`, `CodAlfa`, `CodMaq`, dimensions (`Alto`, `Ancho`, `AltoV`, `Fuelle`) and `NroGlobal`.

---

## 3. Shared time-window function

### 3.1 `dbo.fnConCubo_Ventana(@DiasVentana INT)`

**Purpose**

* Defines a **movable time window** (X days) aligned to:

  * **Argentina local date** (`SYSDATETIMEOFFSET() AT TIME ZONE 'Argentina Standard Time'`).
  * **Corrected date** = `Inicio` – 2 days.
* Normalizes events and pre-computes hours by state.

**Key logic**

* Filters rows where:

```sql
CONVERT(DATE, DATEADD(DAY, -2, TRY_CONVERT(DATETIME, Inicio))) 
    >= DATEADD(DAY, -@DiasVentana, CAST(SYSDATETIMEOFFSET() AT TIME ZONE 'Argentina Standard Time' AS DATE))
```

* Converts `Inicio` / `Fin` to datetime (`InicioDT`, `FinDT`).
* Applies –2 days correction to both:

  * `Inicio_Corregido`, `Fin_Corregido`
  * Legible text columns: `Inicio_Legible_Texto`, `Fin_Legible_Texto`
* Calculates:

  * `Total_Horas`
  * `Horas_Produccion`, `Horas_Preparacion`, `Horas_Parada`, `Horas_Mantenimiento`
* Normalizes IDs and attributes:

  * `ID_Limpio` (numeric)
  * `CantidadBuenosProducida`, `CantidadMalosProducida`
  * `Turno`, `Maquinista`, `Operario`, `CodProducto`, `Motivo`

> This function is the **single entry point** reused by both incremental procedures (blocks & events).

---

## 4. Path 1 – “Blocks” (≈ 17K rows)

### 4.1 Views (V1 → V5)

1. **`ConCubo3Años_Completo` (V1)**

   * Applies **3-year window** directly on `ConCubo` using `GETDATE()`.
   * Fixes dates (–2 days).
   * Normalizes `ID_Limpio`, state hours and quantities.

2. **`ConCubo3AñosSec_Completo` (V2)**

   * Recomputes `Duracion_Horas` between `Inicio_Corregido` and `Fin_Corregido`.
   * Rebuilds per-state hours using this *corrected* duration.

3. **`ConCubo3AñosSecFlag_Completo` (V3)**

   * Adds:

     * `Nro_Secuencia` per `(ID_Limpio, Renglon)` ordered by `Inicio_Corregido`.
     * `FlagPreparacion` – detects the **start of each preparation block**.
     * `SecuenciaPreparacion` – cumulative sequence of preparation blocks per `(ID_Limpio, Renglon)`.

4. **`ConCuboSecuenciasBloques_M11_Completo` (V4)**

   * Joins with:

     * `TablaVinculadaUNION` → `saccod1`.
     * `TablaVinculadaNEW` → `OP`, `CodAlfa`, `CodMaq`, dimensions.
   * Collapses events into **daily blocks** per OT + machine:

     * Groups by `Renglon`, `GrupoOT`, `ID`, `ID_Limpio`, `Fecha`.
     * Aggregates hours and quantities.
     * Keeps **first Turno/Maquinista/Operario/Motivo of the block**.
   * Generates `SortKey` as a **numeric ordering key**:

     * `yyyyMMddHHmmss + Renglon + ID_Limpio` (packed in DECIMAL(38,0)).

5. **`ConCuboSecuenciasBloques_Rango_M11_Completo` (V5 – view version)**

   * Adds:

     * `OrdenGlobalText` – text key with `yyyyMMddHHmmss + Renglon(4) + OT(10)`.
     * `SecuenciaGlobalSQL` – global row number across all blocks.

> After migration, this V5 becomes a **view over the materialized table**, not over V4.

---

### 4.2 Materialized table & indexes

* **Table**: `ConCuboSecuenciasBloques_Rango_M11_Completo_MAT`

  * Initial load: `SELECT * INTO ... FROM ConCuboSecuenciasBloques_Rango_M11_Completo`.

* **Clustered index**:

```sql
CREATE CLUSTERED INDEX CIX_V5MAT_Fecha_Renglon_ID
ON dbo.ConCuboSecuenciasBloques_Rango_M11_Completo_MAT (
    FechaSecuencia,
    Renglon,
    ID_Limpio
);
```

> Optimized for **DELETE + INSERT** on date ranges and for Power BI filters.

* **Final view for Power BI**:

```sql
CREATE OR ALTER VIEW dbo.ConCuboSecuenciasBloques_Rango_M11_Completo AS
SELECT *
FROM dbo.ConCuboSecuenciasBloques_Rango_M11_Completo_MAT;
```

---

### 4.3 Incremental procedure – `RefrescarV5_Incremental`

**Parameters**

* `@DiasVentana INT = 7` → size of the **recalculation window** in days.

**Logic**

1. Compute local reference dates:

```sql
DECLARE @HoyAR  DATE = CAST(SYSDATETIMEOFFSET() AT TIME ZONE 'Argentina Standard Time' AS DATE);
DECLARE @Lim3A  DATE = DATEADD(DAY, -1095, @HoyAR);    -- 3 years
DECLARE @FechaCorte DATE = DATEADD(DAY, -@DiasVentana, @HoyAR);
```

2. **Rolling purge (3 years exact)**

   * Delete everything **older than 3 years**:

```sql
DELETE FROM ConCuboSecuenciasBloques_Rango_M11_Completo_MAT
WHERE FechaSecuencia < @Lim3A;
```

3. **Delete last X days** (recalculated window):

```sql
DELETE FROM ConCuboSecuenciasBloques_Rango_M11_Completo_MAT
WHERE FechaSecuencia >= @FechaCorte;
```

4. **Offset for global sequence**

   * `@Offset = MAX(SecuenciaGlobalSQL)` *before* `@FechaCorte`.
   * Used to keep `SecuenciaGlobalSQL` **continuous** after each refresh.

5. Rebuild last X days:

* From `fnConCubo_Ventana(@DiasVentana)` → V1 (V2/V3)… → `Dia` → `V4`.
* Only **rows with `FechaSecuencia >= @FechaCorte`** are considered at the end.
* In `V5_Rango`:

  * Rebuild `OrdenGlobalText`.
  * Rebuild `SecuenciaGlobalSQL` as `ROW_NUMBER() ... + @Offset`.

6. Insert into MAT:

```sql
INSERT INTO ConCuboSecuenciasBloques_Rango_M11_Completo_MAT
SELECT * FROM V5_Rango;
```

7. Log message with **Argentina local time**.

---

### 4.4 Power BI (Blocks path)

**Initial connection (Advanced options)**

```sql
EXEC dbo.RefrescarV5_Incremental @DiasVentana = 7;
SELECT * FROM dbo.ConCuboSecuenciasBloques_Rango_M11_Completo;
```

**IT fallback**

If José cannot refresh within 7 days, someone with SQL access can manually run:

```sql
EXEC dbo.RefrescarV5_Incremental @DiasVentana = 7;
```

If the delay was larger, adjust:

```sql
EXEC dbo.RefrescarV5_Incremental @DiasVentana = 14;   -- example
```

---

## 5. Path 2 – “Events” (≈ 2.4M rows)

### 5.1 Event view & materialization

1. **`MED_V4E_Eventos_Completo`**

* Starts from `ConCubo3AñosSecFlag_Completo` (V3 – events level).

* Adds joins:

  * `TablaVinculadaUNION` → `saccod1`.
  * `TablaVinculadaNEW` → `OP`, `CodAlfa`, `CodMaq`, dimensions.

* Outputs, per event:

  * `Renglon`, `ID`, `ID_Limpio`
  * `Estado`
  * `InicioSecuencia` = `Inicio_Corregido`
  * `FinSecuencia`    = `Fin_Corregido`
  * `FechaSecuencia`
  * `HorasProd`, `HorasPrep`, `HorasPara`, `HorasMant`
  * `BuenosTotal`, `MalosTotal`
  * Operators & product info
  * `SortKey` (same numeric key pattern)

2. **Materialized table**: `MED_V5E_Eventos_MAT`

* Initial load:

```sql
SELECT * INTO MED_V5E_Eventos_MAT
FROM MED_V4E_Eventos_Completo;
```

3. **Indexes**

* Clustered:

```sql
CREATE CLUSTERED INDEX CIX_V5E_Fecha_Renglon_ID
ON MED_V5E_Eventos_MAT (FechaSecuencia, Renglon, ID_Limpio);
```

* Nonclustered:

```sql
CREATE NONCLUSTERED INDEX IX_V5E_Renglon_ID
ON MED_V5E_Eventos_MAT (Renglon, ID_Limpio)
INCLUDE (InicioSecuencia, FinSecuencia, Estado, HorasProd, HorasPrep, BuenosTotal);
```

4. **Computed column for legible datetime**

```sql
ALTER TABLE MED_V5E_Eventos_MAT
ADD FechaSecuenciaTextoHora AS CONVERT(varchar(16), InicioSecuencia, 120) PERSISTED;
```

5. **Final view for Power BI**

```sql
CREATE OR ALTER VIEW MED_V5E_Eventos AS
SELECT * FROM MED_V5E_Eventos_MAT;
```

---

### 5.2 Incremental procedure – `RefrescarV5E_Incremental`

**Parameters**

* `@DiasVentana INT = 7`

**Logic**

1. Same **local date** + **3-year limit**:

```sql
DECLARE @HoyAR     DATE = CAST(SYSDATETIMEOFFSET() AT TIME ZONE 'Argentina Standard Time' AS DATE);
DECLARE @Lim3A     DATE = DATEADD(DAY, -1095, @HoyAR);
DECLARE @FechaCorte DATE = DATEADD(DAY, -@DiasVentana, @HoyAR);
```

2. **Rolling purge** (>3 years old):

```sql
DELETE FROM MED_V5E_Eventos_MAT
WHERE FechaSecuencia < @Lim3A;
```

3. **Delete last X days** (recalculation):

```sql
DELETE FROM MED_V5E_Eventos_MAT
WHERE FechaSecuencia >= @FechaCorte;
```

4. **Rebuild last X days from `fnConCubo_Ventana`**

* CTE `V` → calls `fnConCubo_Ventana(@DiasVentana)`.
* CTE `S` → shapes rows as event-level records (Inicio/Fin, hours by state, quantities, attributes).
* Join with `TablaVinculadaUNION` and `TablaVinculadaNEW` (via `VU`, `NEW_base`, `NEWmap`).
* Computes `SortKey` the same way as in the initial view.
* Inserts only where `FechaSecuencia >= @FechaCorte`.

5. Prints log with local Argentina time.

---

### 5.3 Power BI (Events path)

**Advanced query**:

```sql
EXEC dbo.RefrescarV5E_Incremental @DiasVentana = 7;
SELECT * FROM dbo.MED_V5E_Eventos;
```

**IT fallback / larger gaps**

If refresh was paused for more than 7 days, IT can run:

```sql
EXEC dbo.RefrescarV5E_Incremental @DiasVentana = 14;
EXEC dbo.RefrescarV5_Incremental  @DiasVentana = 14;
```

Adjust `@DiasVentana` according to the real gap to ensure the entire missing period is rebuilt.

---

## 6. How to deploy (one-time)

1. **Create / update all views** (V1 → V5 and `MED_V4E_Eventos_Completo`).
2. **Create materialized tables**:

   * `ConCuboSecuenciasBloques_Rango_M11_Completo_MAT`
   * `MED_V5E_Eventos_MAT`
3. **Create indexes** on both MAT tables.
4. **Create the time-window function** `fnConCubo_Ventana`.
5. **Create the incremental procedures**:

   * `RefrescarV5_Incremental`
   * `RefrescarV5E_Incremental`
6. **Create the final views for Power BI**:

   * `ConCuboSecuenciasBloques_Rango_M11_Completo`
   * `MED_V5E_Eventos`
7. **Add `FechaSecuenciaTextoHora`** to `MED_V5E_Eventos_MAT`.
8. Configure Power BI to call:

   * Blocks: `EXEC RefrescarV5_Incremental ...; SELECT * FROM ConCuboSecuenciasBloques_Rango_M11_Completo;`
   * Events: `EXEC RefrescarV5E_Incremental ...; SELECT * FROM MED_V5E_Eventos;`

---

## 7. Operational notes

* **Daily scenario (normal)**
  José refreshes Power BI with `@DiasVentana = 7` on both procedures (via Advanced SQL).
* **If refresh is delayed**
  IT can run the SPs manually with a higher `@DiasVentana` to cover the gap.
* **Storage control**
  3-year window is guaranteed by:

  * `DELETE WHERE FechaSecuencia < @Lim3A` in both MAT tables.
* **Consistency**
  Both paths (blocks & events) rely on the **same time-window function** and the same –2 day correction.

---


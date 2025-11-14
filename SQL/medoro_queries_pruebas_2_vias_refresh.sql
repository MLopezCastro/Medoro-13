--via 1, 17K:
/* =============================================================
   V1 – ConCubo3Años_Completo
   - Ventana 3 años
   - Corrección -2 días
   - Normalización y horas por estado
   - Aliases: CodProducto, Motivo
   ============================================================= */
CREATE OR ALTER VIEW dbo.ConCubo3Años_Completo AS
WITH DatosParseados AS (
    SELECT *,
           TRY_CAST(Inicio AS DATETIME) AS InicioDT,
           TRY_CAST(Fin    AS DATETIME) AS FinDT
    FROM dbo.ConCubo
    WHERE TRY_CAST(Inicio AS DATETIME) >= DATEADD(YEAR, -3, CAST(GETDATE() AS DATE))
      AND ISNUMERIC(SUBSTRING(ID, PATINDEX('%[0-9]%', ID), LEN(ID))) = 1
),
HorasCalculadas AS (
    SELECT *,
           DATEDIFF(SECOND, InicioDT, FinDT) / 3600.0 AS Total_Horas
    FROM DatosParseados
)
SELECT
    ID,
    TRY_CAST(SUBSTRING(ID, PATINDEX('%[0-9]%', ID), LEN(ID)) AS INT) AS ID_Limpio,
    Renglon, Estado,
    DATEADD(DAY, -2, InicioDT) AS Inicio_Corregido,
    DATEADD(DAY, -2, FinDT)    AS Fin_Corregido,
    CONVERT(VARCHAR(16), DATEADD(DAY, -2, InicioDT), 120) AS Inicio_Legible_Texto,
    CONVERT(VARCHAR(16), DATEADD(DAY, -2, FinDT)   , 120) AS Fin_Legible_Texto,
    CONVERT(DATE, DATEADD(DAY, -2, InicioDT)) AS Fecha,
    Total_Horas,
    CASE WHEN Estado='Producción'     THEN Total_Horas ELSE 0 END AS Horas_Produccion,
    CASE WHEN Estado='Preparación'    THEN Total_Horas ELSE 0 END AS Horas_Preparacion,
    CASE WHEN Estado='Maquina Parada' THEN Total_Horas ELSE 0 END AS Horas_Parada,
    CASE WHEN Estado='Mantenimiento'  THEN Total_Horas ELSE 0 END AS Horas_Mantenimiento,
    TRY_CAST(CantidadBuenosProducida AS FLOAT) AS CantidadBuenosProducida,
    TRY_CAST(CantidadMalosProducida  AS FLOAT) AS CantidadMalosProducida,
    Turno, Maquinista, Operario,
    codproducto AS CodProducto,
    motivo      AS Motivo
FROM HorasCalculadas;
GO

/* =============================================================
   V2 – ConCubo3AñosSec_Completo
   - Duración real entre fechas corregidas
   - Mantiene cantidades y campos operativos
   ============================================================= */
CREATE OR ALTER VIEW dbo.ConCubo3AñosSec_Completo AS
WITH Base AS (
    SELECT *,
           DATEDIFF(SECOND, Inicio_Corregido, Fin_Corregido) / 3600.0 AS Duracion_Horas
    FROM dbo.ConCubo3Años_Completo
)
SELECT
    ID, ID_Limpio, Renglon, Estado,
    Inicio_Corregido, Fin_Corregido,
    Inicio_Legible_Texto, Fin_Legible_Texto,
    CONVERT(DATE, Inicio_Corregido) AS Fecha,
    Duracion_Horas AS Total_Horas,
    CASE WHEN Estado='Producción'     THEN Duracion_Horas ELSE 0 END AS Horas_Produccion,
    CASE WHEN Estado='Preparación'    THEN Duracion_Horas ELSE 0 END AS Horas_Preparacion,
    CASE WHEN Estado='Maquina Parada' THEN Duracion_Horas ELSE 0 END AS Horas_Parada,
    CASE WHEN Estado='Mantenimiento'  THEN Duracion_Horas ELSE 0 END AS Horas_Mantenimiento,
    CantidadBuenosProducida, CantidadMalosProducida,
    Turno, Maquinista, Operario, CodProducto, Motivo
FROM Base;
GO

/* =============================================================
   V3 – ConCubo3AñosSecFlag_Completo
   - Nro_Secuencia por OT+Renglón
   - Flag y secuencia acumulada de preparación
   ============================================================= */
CREATE OR ALTER VIEW dbo.ConCubo3AñosSecFlag_Completo AS
WITH Base AS (
    SELECT *,
           ROW_NUMBER() OVER (
             PARTITION BY ID_Limpio, Renglon
             ORDER BY Inicio_Corregido
           ) AS Nro_Secuencia
    FROM dbo.ConCubo3AñosSec_Completo
),
PrepFlag AS (
    SELECT *,
           CASE WHEN Estado='Preparación'
                 AND LAG(Estado) OVER (PARTITION BY ID_Limpio, Renglon ORDER BY Inicio_Corregido)
                     IS DISTINCT FROM 'Preparación'
                THEN 1 ELSE 0 END AS FlagPreparacion
    FROM Base
),
PrepSecuencia AS (
    SELECT *,
           SUM(FlagPreparacion) OVER (
              PARTITION BY ID_Limpio, Renglon
              ORDER BY Inicio_Corregido
              ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
           ) AS SecuenciaPreparacion
    FROM PrepFlag
)
SELECT
  ID, ID_Limpio, Renglon, Estado,
  Inicio_Corregido, Fin_Corregido,
  Inicio_Legible_Texto, Fin_Legible_Texto,
  Fecha, Total_Horas,
  Horas_Produccion, Horas_Preparacion, Horas_Parada, Horas_Mantenimiento,
  CantidadBuenosProducida, CantidadMalosProducida,
  Turno, Maquinista, Operario, CodProducto, Motivo,
  Nro_Secuencia, FlagPreparacion, SecuenciaPreparacion
FROM PrepSecuencia;
GO

/* =============================================================
   V4 – ConCuboSecuenciasBloques_M11_Completo
   - Colapso a BLOQUES (día + OT + renglón)
   - Primer Turno/Maquinista/Operario/Motivo del bloque
   - JOIN UNION (saccod1), JOIN NEW dedup (OP, CodAlfa, CodMaq, Alto, Ancho, AltoV, Fuelle)
   - SortKey sin FORMAT (DECIMAL)
   ============================================================= */
CREATE OR ALTER VIEW dbo.ConCuboSecuenciasBloques_M11_Completo AS
WITH VU AS (  -- saccod1 por OP numérico
    SELECT TRY_CAST(OP AS INT) AS ID_Limpio, MIN(saccod1) AS saccod1
    FROM dbo.TablaVinculadaUNION
    WHERE ISNUMERIC(OP)=1
    GROUP BY TRY_CAST(OP AS INT)
),
NEW_base AS ( -- normalizo NEW y calculo ID_Limpio
    SELECT
        COALESCE(
            TRY_CAST(NroGlobal AS INT),
            TRY_CAST(SUBSTRING(OP, PATINDEX('%[0-9]%', OP), 50) AS INT)
        ) AS ID_Limpio,
        OP, CodAlfa, CodMaq,
        TRY_CAST(Alto   AS INT) AS Alto,
        TRY_CAST(Ancho  AS INT) AS Ancho,
        TRY_CAST(AltoV  AS INT) AS AltoV,
        TRY_CAST(Fuelle AS INT) AS Fuelle
    FROM dbo.TablaVinculadaNEW
),
NEWmap AS ( -- deduplico NEW: una fila por ID
    SELECT
        ID_Limpio,
        MAX(OP)      AS OP,
        MAX(CodAlfa) AS CodAlfa,
        MAX(CodMaq)  AS CodMaq,
        MAX(Alto)    AS Alto,
        MAX(Ancho)   AS Ancho,
        MAX(AltoV)   AS AltoV,
        MAX(Fuelle)  AS Fuelle
    FROM NEW_base
    WHERE ID_Limpio IS NOT NULL
    GROUP BY ID_Limpio
),
Base AS ( -- métricas + atributos base
    SELECT
        s.Renglon, s.ID, s.ID_Limpio,
        s.Inicio_Corregido, s.Fin_Corregido,
        CAST(ISNULL(s.CantidadBuenosProducida,0) AS DECIMAL(18,4)) AS CantBuenos,
        CAST(ISNULL(s.CantidadMalosProducida ,0) AS DECIMAL(18,4)) AS CantMalos,
        CAST(ISNULL(s.Horas_Produccion       ,0) AS DECIMAL(18,6)) AS HorasProd,
        CAST(ISNULL(s.Horas_Preparacion      ,0) AS DECIMAL(18,6)) AS HorasPrep,
        CAST(ISNULL(s.Horas_Parada           ,0) AS DECIMAL(18,6)) AS HorasPara,
        CAST(ISNULL(s.Horas_Mantenimiento    ,0) AS DECIMAL(18,6)) AS HorasMant,
        s.CodProducto,
        s.Turno, s.Maquinista, s.Operario, s.Motivo
    FROM dbo.ConCubo3AñosSecFlag_Completo s
    WHERE s.Inicio_Corregido IS NOT NULL AND s.Fin_Corregido IS NOT NULL
),
Marcado AS (
    SELECT *,
           CASE WHEN LAG(ID_Limpio) OVER (PARTITION BY Renglon ORDER BY Inicio_Corregido)=ID_Limpio
                THEN 0 ELSE 1 END AS CambioID
    FROM Base
),
Grupos AS (
    SELECT *,
           SUM(CambioID) OVER (PARTITION BY Renglon ORDER BY Inicio_Corregido
                               ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS GrupoOT
    FROM Marcado
),
ConRN AS ( -- nro en el día para “primer valor del bloque”
    SELECT *,
           ROW_NUMBER() OVER (
             PARTITION BY Renglon, GrupoOT, CONVERT(date, Inicio_Corregido)
             ORDER BY Inicio_Corregido
           ) AS rnBloque
    FROM Grupos
),
Dia AS ( -- colapso diario OT+renglón
    SELECT
        Renglon, ID, ID_Limpio, GrupoOT,
        CONVERT(date, Inicio_Corregido) AS FechaSecuencia,
        MIN(Inicio_Corregido) AS InicioSecuencia,
        MAX(Fin_Corregido)    AS FinSecuencia,
        MAX(CodProducto)      AS CodProducto_Bloque,
        SUM(CantBuenos)       AS BuenosTotal,
        SUM(CantMalos)        AS MalosTotal,
        SUM(HorasProd)        AS HorasProd,
        SUM(HorasPrep)        AS HorasPrep,
        SUM(HorasPara)        AS HorasPara,
        SUM(HorasMant)        AS HorasMant,
        COUNT(*)              AS FilasColapsadas,
        -- atributos del primer evento del bloque
        MAX(CASE WHEN rnBloque=1 THEN Turno      END) AS Turno,
        MAX(CASE WHEN rnBloque=1 THEN Maquinista END) AS Maquinista,
        MAX(CASE WHEN rnBloque=1 THEN Operario   END) AS Operario,
        MAX(CASE WHEN rnBloque=1 THEN Motivo     END) AS Motivo
    FROM ConRN
    GROUP BY Renglon, GrupoOT, ID, ID_Limpio, CONVERT(date, Inicio_Corregido)
)
SELECT
    d.Renglon, d.ID, d.ID_Limpio,
    d.CodProducto_Bloque AS CodProducto,
    d.FechaSecuencia,
    CONVERT(varchar(16), d.InicioSecuencia, 120) AS FechaSecuenciaTextoHora,
    d.InicioSecuencia, d.FinSecuencia,
    d.BuenosTotal, d.MalosTotal, d.HorasProd, d.HorasPrep, d.HorasPara, d.HorasMant,
    d.FilasColapsadas,
    d.Turno, d.Maquinista, d.Operario, d.Motivo,

    ROW_NUMBER() OVER (
        PARTITION BY d.FechaSecuencia
        ORDER BY d.InicioSecuencia, d.Renglon, d.ID_Limpio
    ) AS NumeroBloqueDiaSQL,

    ROW_NUMBER() OVER (
        PARTITION BY d.FechaSecuencia, d.Renglon
        ORDER BY d.InicioSecuencia, d.ID_Limpio
    ) AS NumeroBloqueDiaPorRenglonSQL,

    -- SortKey sin FORMAT (DECIMAL para evitar overflow)
    CAST(REPLACE(REPLACE(REPLACE(CONVERT(varchar(19), d.InicioSecuencia, 120),'-',''),' ',''),':','') AS DECIMAL(38,0)) * 10000000000
      + CAST(d.Renglon   AS DECIMAL(38,0)) * 1000000000
      + CAST(d.ID_Limpio AS DECIMAL(38,0))                                    AS SortKey,

    VU.saccod1,
    N.OP, N.CodAlfa, N.CodMaq,
    N.Alto, N.Ancho, N.AltoV, N.Fuelle
FROM Dia d
LEFT JOIN VU     ON VU.ID_Limpio  = d.ID_Limpio
LEFT JOIN NEWmap N ON N.ID_Limpio = d.ID_Limpio;
GO

/* =============================================================
   V5 – ConCuboSecuenciasBloques_Rango_M11_Completo  (FINAL PBI)
   - OrdenGlobalText (texto) + SecuenciaGlobalSQL 1..N
   - Hereda TODO lo de V4_Completo
   ============================================================= */
CREATE OR ALTER VIEW dbo.ConCuboSecuenciasBloques_Rango_M11_Completo AS
SELECT
    d.*,

    -- orden textual yyyyMMddHHmmss + Renglon(4) + OT(10)
    REPLACE(REPLACE(REPLACE(CONVERT(varchar(19), d.InicioSecuencia, 120),'-',''),' ',''),':','')
    + RIGHT('0000' + CAST(d.Renglon AS varchar(4)), 4)
    + RIGHT('0000000000' + CAST(d.ID_Limpio AS varchar(10)), 10) AS OrdenGlobalText,

    -- índice global 1..N (no se reinicia)
    ROW_NUMBER() OVER (ORDER BY d.InicioSecuencia, d.Renglon, d.ID_Limpio) AS SecuenciaGlobalSQL
FROM dbo.ConCuboSecuenciasBloques_M11_Completo AS d;
GO


-------------
---intento replicar

-- PASO 2: Crear Tabla Materializada (Migración Inicial)
-- Materializar 3 años de datos (V5 = 1 fila por Renglon–ID con SecuenciaGlobalSQL)

IF OBJECT_ID('dbo.ConCuboSecuenciasBloques_Rango_M11_Completo_MAT','U') IS NOT NULL
    DROP TABLE dbo.ConCuboSecuenciasBloques_Rango_M11_Completo_MAT;

SELECT *
INTO dbo.ConCuboSecuenciasBloques_Rango_M11_Completo_MAT
FROM dbo.ConCuboSecuenciasBloques_Rango_M11_Completo;  -- << V5 >>

-- Verificar cantidad de filas (esperado ~17,649)
SELECT COUNT(*) AS TotalFilas
FROM dbo.ConCuboSecuenciasBloques_Rango_M11_Completo_MAT;


---------
-- PASO 3: Índice CLUSTERED sobre la MAT de V5
-- Optimiza borrados/inserciones por fecha y filtros en PBI
-- Si existe el índice, lo dropeo primero
IF EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE name = 'CIX_V5MAT_Fecha_Renglon_ID'
      AND object_id = OBJECT_ID('dbo.ConCuboSecuenciasBloques_Rango_M11_Completo_MAT')
)
BEGIN
    DROP INDEX CIX_V5MAT_Fecha_Renglon_ID
    ON dbo.ConCuboSecuenciasBloques_Rango_M11_Completo_MAT;
END
GO

-- Crear el CLUSTERED correcto (sin ; al final)
CREATE CLUSTERED INDEX CIX_V5MAT_Fecha_Renglon_ID
ON dbo.ConCuboSecuenciasBloques_Rango_M11_Completo_MAT (
    FechaSecuencia,
    Renglon,
    ID_Limpio
);
GO

-- Validación
SELECT i.name, i.type_desc
FROM sys.indexes i
WHERE i.object_id = OBJECT_ID('dbo.ConCuboSecuenciasBloques_Rango_M11_Completo_MAT');

-----------

/* =============================================================
   PASO 4 -Función: fnConCubo_Ventana  (Versión corregida)
   Proyecto: Medoro 13 – Camino BLOQUES
   Objetivo:
     - Alinear la ventana temporal al huso horario argentino
     - Mantener corrección de -2 días (Inicio/Fin)
     - Calcular horas por estado
     - Evitar desfases de ventana (caso 28/oct)
   ============================================================= */
CREATE OR ALTER FUNCTION dbo.fnConCubo_Ventana (@DiasVentana INT)
RETURNS TABLE
AS
RETURN
(
    WITH DatosParseados AS (
        SELECT  
            *,
            TRY_CONVERT(DATETIME, Inicio) AS InicioDT,
            TRY_CONVERT(DATETIME, Fin)    AS FinDT
        FROM dbo.ConCubo
        /* 🔧 Ventana ajustada con TZ Argentina y corrección -2 días */
        WHERE CONVERT(DATE, DATEADD(DAY, -2, TRY_CONVERT(DATETIME, Inicio))) >= DATEADD(
                  DAY, -@DiasVentana,
                  CAST(SYSDATETIMEOFFSET() AT TIME ZONE 'Argentina Standard Time' AS DATE)
              )
          AND PATINDEX('%[0-9]%', ID) > 0
    ),
    HorasCalculadas AS (
        SELECT  
            *,
            DATEDIFF(SECOND, InicioDT, FinDT) / 3600.0 AS Total_Horas
        FROM DatosParseados
        WHERE InicioDT IS NOT NULL AND FinDT IS NOT NULL
    )
    SELECT
        ID,
        TRY_CONVERT(INT, SUBSTRING(ID, PATINDEX('%[0-9]%', ID), LEN(ID))) AS ID_Limpio,
        Renglon,
        Estado,
        /* 🔧 Corrección -2 días aplicada a Inicio y Fin */
        DATEADD(DAY, -2, InicioDT) AS Inicio_Corregido,
        DATEADD(DAY, -2, FinDT)    AS Fin_Corregido,
        CONVERT(VARCHAR(16), DATEADD(DAY, -2, InicioDT), 120) AS Inicio_Legible_Texto,
        CONVERT(VARCHAR(16), DATEADD(DAY, -2, FinDT), 120)   AS Fin_Legible_Texto,
        CONVERT(DATE, DATEADD(DAY, -2, InicioDT)) AS Fecha,
        Total_Horas,

        /* Horas por estado */
        CASE WHEN Estado = 'Producción'     THEN Total_Horas ELSE 0 END AS Horas_Produccion,
        CASE WHEN Estado = 'Preparación'    THEN Total_Horas ELSE 0 END AS Horas_Preparacion,
        CASE WHEN Estado = 'Maquina Parada' THEN Total_Horas ELSE 0 END AS Horas_Parada,
        CASE WHEN Estado = 'Mantenimiento'  THEN Total_Horas ELSE 0 END AS Horas_Mantenimiento,

        /* Cantidades y atributos */
        TRY_CONVERT(FLOAT, CantidadBuenosProducida) AS CantidadBuenosProducida,
        TRY_CONVERT(FLOAT, CantidadMalosProducida)  AS CantidadMalosProducida,
        Turno, Maquinista, Operario,
        codproducto AS CodProducto,
        motivo      AS Motivo
    FROM HorasCalculadas
);
GO



----------
--validar

-- Ultimos 7 días (smoke test)
SELECT COUNT(*) AS FilasUltimos7Dias
FROM dbo.fnConCubo_Ventana(7);

-- Ventana 3 años (1095) si querés comparar contra V1/V5
SELECT COUNT(*) AS Filas_3A
FROM dbo.fnConCubo_Ventana(1095);

--------

/* -------------------------------------------------------------
   SP: RefrescarV5_Incremental  (Versión corregida)
   Proyecto: Medoro 13 – Camino BLOQUES
   Objetivo:
     - Mantener 3 años exactos de datos (rolling purge)
     - Alinear cortes con TZ Argentina
     - Evitar desfase -2d en la ventana
   ------------------------------------------------------------- */

CREATE OR ALTER PROCEDURE dbo.RefrescarV5_Incremental
    @DiasVentana INT = 7
AS
BEGIN
    SET NOCOUNT ON;

    BEGIN TRY
        /* 🔧 agregado: ajustar a hora local AR y purgar lo anterior a 3 años */
        DECLARE @HoyAR DATE = CAST(SYSDATETIMEOFFSET() AT TIME ZONE 'Argentina Standard Time' AS DATE);
        DECLARE @Lim3A DATE = DATEADD(DAY, -1095, @HoyAR);  -- 3 años exactos

        DELETE FROM dbo.ConCuboSecuenciasBloques_Rango_M11_Completo_MAT
        WHERE FechaSecuencia < @Lim3A;  -- 🔧 purga rolling por antigüedad

        /* 🔧 corte de ventana corregido (TZ AR) */
        DECLARE @FechaCorte DATE = DATEADD(DAY, -@DiasVentana, @HoyAR);

        /* 1) Borrar últimos X días en la MAT de V5 */
        DELETE FROM dbo.ConCuboSecuenciasBloques_Rango_M11_Completo_MAT
        WHERE FechaSecuencia >= @FechaCorte;

        /* 2) Calcular offset de secuencia previo al corte */
        DECLARE @Offset BIGINT = ISNULL((
            SELECT MAX(SecuenciaGlobalSQL)
            FROM dbo.ConCuboSecuenciasBloques_Rango_M11_Completo_MAT
            WHERE FechaSecuencia < @FechaCorte
        ), 0);

        /* 3) Recalcular SOLO últimos X días (V1→V4) desde la función parametrizada */
        ;WITH
        V1 AS (
            SELECT * 
            FROM dbo.fnConCubo_Ventana(@DiasVentana)
        ),
        V2 AS (
            SELECT
                ID, ID_Limpio, Renglon, Estado,
                Inicio_Corregido, Fin_Corregido,
                Inicio_Legible_Texto, Fin_Legible_Texto,
                CONVERT(DATE, Inicio_Corregido) AS Fecha,
                DATEDIFF(SECOND, Inicio_Corregido, Fin_Corregido) / 3600.0 AS Total_Horas,
                CASE WHEN Estado='Producción'     THEN DATEDIFF(SECOND, Inicio_Corregido, Fin_Corregido) / 3600.0 ELSE 0 END AS Horas_Produccion,
                CASE WHEN Estado='Preparación'    THEN DATEDIFF(SECOND, Inicio_Corregido, Fin_Corregido) / 3600.0 ELSE 0 END AS Horas_Preparacion,
                CASE WHEN Estado='Maquina Parada' THEN DATEDIFF(SECOND, Inicio_Corregido, Fin_Corregido) / 3600.0 ELSE 0 END AS Horas_Parada,
                CASE WHEN Estado='Mantenimiento'  THEN DATEDIFF(SECOND, Inicio_Corregido, Fin_Corregido) / 3600.0 ELSE 0 END AS Horas_Mantenimiento,
                CantidadBuenosProducida, CantidadMalosProducida,
                Turno, Maquinista, Operario, CodProducto, Motivo
            FROM V1
        ),
        V3_Base AS (
            SELECT * ,
                   ROW_NUMBER() OVER (PARTITION BY ID_Limpio, Renglon ORDER BY Inicio_Corregido) AS Nro_Secuencia
            FROM V2
        ),
        V3_Flag AS (
            SELECT * ,
                   CASE WHEN Estado='Preparación'
                         AND LAG(Estado) OVER (PARTITION BY ID_Limpio, Renglon ORDER BY Inicio_Corregido)
                             IS DISTINCT FROM 'Preparación'
                        THEN 1 ELSE 0 END AS FlagPreparacion
            FROM V3_Base
        ),
        V3 AS (
            SELECT * ,
                   SUM(FlagPreparacion) OVER (
                        PARTITION BY ID_Limpio, Renglon
                        ORDER BY Inicio_Corregido
                        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                   ) AS SecuenciaPreparacion
            FROM V3_Flag
        ),
        VU AS (  -- saccod1 por OP numérico
            SELECT TRY_CAST(OP AS INT) AS ID_Limpio, MIN(saccod1) AS saccod1
            FROM dbo.TablaVinculadaUNION
            WHERE ISNUMERIC(OP)=1
            GROUP BY TRY_CAST(OP AS INT)
        ),
        NEW_base AS ( -- normalizo NEW y calculo ID_Limpio
            SELECT
                COALESCE(
                    TRY_CAST(NroGlobal AS INT),
                    TRY_CAST(SUBSTRING(OP, PATINDEX('%[0-9]%', OP), 50) AS INT)
                ) AS ID_Limpio,
                OP, CodAlfa, CodMaq,
                TRY_CAST(Alto   AS INT) AS Alto,
                TRY_CAST(Ancho  AS INT) AS Ancho,
                TRY_CAST(AltoV  AS INT) AS AltoV,
                TRY_CAST(Fuelle AS INT) AS Fuelle
            FROM dbo.TablaVinculadaNEW
        ),
        NEWmap AS ( -- deduplico NEW: una fila por ID
            SELECT
                ID_Limpio,
                MAX(OP)      AS OP,
                MAX(CodAlfa) AS CodAlfa,
                MAX(CodMaq)  AS CodMaq,
                MAX(Alto)    AS Alto,
                MAX(Ancho)   AS Ancho,
                MAX(AltoV)   AS AltoV,
                MAX(Fuelle)  AS Fuelle
            FROM NEW_base
            WHERE ID_Limpio IS NOT NULL
            GROUP BY ID_Limpio
        ),
        Base AS ( -- métricas + atributos base
            SELECT
                s.Renglon, s.ID, s.ID_Limpio,
                s.Inicio_Corregido, s.Fin_Corregido,
                CAST(ISNULL(s.CantidadBuenosProducida,0) AS DECIMAL(18,4)) AS CantBuenos,
                CAST(ISNULL(s.CantidadMalosProducida ,0) AS DECIMAL(18,4)) AS CantMalos,
                CAST(ISNULL(s.Horas_Produccion       ,0) AS DECIMAL(18,6)) AS HorasProd,
                CAST(ISNULL(s.Horas_Preparacion      ,0) AS DECIMAL(18,6)) AS HorasPrep,
                CAST(ISNULL(s.Horas_Parada           ,0) AS DECIMAL(18,6)) AS HorasPara,
                CAST(ISNULL(s.Horas_Mantenimiento    ,0) AS DECIMAL(18,6)) AS HorasMant,
                s.CodProducto,
                s.Turno, s.Maquinista, s.Operario, s.Motivo
            FROM V3 s
            WHERE s.Inicio_Corregido IS NOT NULL AND s.Fin_Corregido IS NOT NULL
        ),
        Marcado AS (
            SELECT * ,
                   CASE WHEN LAG(ID_Limpio) OVER (PARTITION BY Renglon ORDER BY Inicio_Corregido)=ID_Limpio
                        THEN 0 ELSE 1 END AS CambioID
            FROM Base
        ),
        Grupos AS (
            SELECT * ,
                   SUM(CambioID) OVER (
                        PARTITION BY Renglon
                        ORDER BY Inicio_Corregido
                        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                   ) AS GrupoOT
            FROM Marcado
        ),
        ConRN AS ( -- nro en el día para primer valor del bloque
            SELECT * ,
                   ROW_NUMBER() OVER (
                        PARTITION BY Renglon, GrupoOT, CONVERT(date, Inicio_Corregido)
                        ORDER BY Inicio_Corregido
                   ) AS rnBloque
            FROM Grupos
        ),
        Dia AS ( -- colapso diario OT+renglón
            SELECT
                Renglon, ID, ID_Limpio, GrupoOT,
                CONVERT(date, Inicio_Corregido) AS FechaSecuencia,
                MIN(Inicio_Corregido) AS InicioSecuencia,
                MAX(Fin_Corregido)    AS FinSecuencia,
                MAX(CodProducto)      AS CodProducto_Bloque,
                SUM(CantBuenos)       AS BuenosTotal,
                SUM(CantMalos)        AS MalosTotal,
                SUM(HorasProd)        AS HorasProd,
                SUM(HorasPrep)        AS HorasPrep,
                SUM(HorasPara)        AS HorasPara,
                SUM(HorasMant)        AS HorasMant,
                COUNT(*)              AS FilasColapsadas,
                MAX(CASE WHEN rnBloque=1 THEN Turno      END) AS Turno,
                MAX(CASE WHEN rnBloque=1 THEN Maquinista END) AS Maquinista,
                MAX(CASE WHEN rnBloque=1 THEN Operario   END) AS Operario,
                MAX(CASE WHEN rnBloque=1 THEN Motivo     END) AS Motivo
            FROM ConRN
            GROUP BY Renglon, GrupoOT, ID, ID_Limpio, CONVERT(date, Inicio_Corregido)
        ),
        V4 AS ( -- tu V4 colapsada con joins
            SELECT
                d.Renglon, d.ID, d.ID_Limpio,
                d.CodProducto_Bloque AS CodProducto,
                d.FechaSecuencia,
                CONVERT(varchar(16), d.InicioSecuencia, 120) AS FechaSecuenciaTextoHora,
                d.InicioSecuencia, d.FinSecuencia,
                d.BuenosTotal, d.MalosTotal, d.HorasProd, d.HorasPrep, d.HorasPara, d.HorasMant,
                d.FilasColapsadas,
                d.Turno, d.Maquinista, d.Operario, d.Motivo,
                ROW_NUMBER() OVER (
                    PARTITION BY d.FechaSecuencia
                    ORDER BY d.InicioSecuencia, d.Renglon, d.ID_Limpio
                ) AS NumeroBloqueDiaSQL,
                ROW_NUMBER() OVER (
                    PARTITION BY d.FechaSecuencia, d.Renglon
                    ORDER BY d.InicioSecuencia, d.ID_Limpio
                ) AS NumeroBloqueDiaPorRenglonSQL,
                CAST(REPLACE(REPLACE(REPLACE(CONVERT(varchar(19), d.InicioSecuencia, 120),'-',''),' ',''),':','') AS DECIMAL(38,0)) * 10000000000
                  + CAST(d.Renglon   AS DECIMAL(38,0)) * 1000000000
                  + CAST(d.ID_Limpio AS DECIMAL(38,0))                                    AS SortKey,
                VU.saccod1,
                N.OP, N.CodAlfa, N.CodMaq,
                N.Alto, N.Ancho, N.AltoV, N.Fuelle
            FROM Dia d
            LEFT JOIN VU     ON VU.ID_Limpio  = d.ID_Limpio
            LEFT JOIN NEWmap N ON N.ID_Limpio = d.ID_Limpio
            WHERE d.FechaSecuencia >= @FechaCorte
        ),
        /* 4) V5 para el rango: OrdenGlobalText + SecuenciaGlobalSQL con offset */
        V5_Rango AS (
            SELECT
                V4.*,
                -- orden textual yyyyMMddHHmmss + Renglon(4) + OT(10)
                REPLACE(REPLACE(REPLACE(CONVERT(varchar(19), V4.InicioSecuencia, 120),'-',''),' ',''),':','')
                + RIGHT('0000' + CAST(V4.Renglon AS varchar(4)), 4)
                + RIGHT('0000000000' + CAST(V4.ID_Limpio AS varchar(10)), 10) AS OrdenGlobalText,

                -- Secuencia global continua con offset previo
                ROW_NUMBER() OVER (ORDER BY V4.InicioSecuencia, V4.Renglon, V4.ID_Limpio) + @Offset
                    AS SecuenciaGlobalSQL
            FROM V4
        )
        INSERT INTO dbo.ConCuboSecuenciasBloques_Rango_M11_Completo_MAT
        SELECT * FROM V5_Rango;

        PRINT 'V5 refrescada (últimos ' + CAST(@DiasVentana AS VARCHAR(10)) 
              + ' días) - ' + CONVERT(VARCHAR(20), SYSDATETIMEOFFSET() AT TIME ZONE 'Argentina Standard Time', 120);
    END TRY
    BEGIN CATCH
        DECLARE @ErrorMsg NVARCHAR(4000) = ERROR_MESSAGE();
        RAISERROR(@ErrorMsg, 16, 1);
    END CATCH
END;
GO

------------

--validación

-- SP creado
SELECT name, create_date, modify_date
FROM sys.procedures
WHERE name = 'RefrescarV5_Incremental';

-- Ejecutar prueba (últimos 7 días)
EXEC dbo.RefrescarV5_Incremental @DiasVentana = 7;

-- Chequear continuidad de secuencia
SELECT MIN(SecuenciaGlobalSQL) AS MinSec,
       MAX(SecuenciaGlobalSQL) AS MaxSec,
       COUNT(*)                AS N
FROM dbo.ConCuboSecuenciasBloques_Rango_M11_Completo_MAT;
-- Esperado: Min = 1, Max = N

---------

---ejecutar desde sql IT: Marcelo, si José no puede en 7 días:

EXEC dbo.RefrescarV5_Incremental @DiasVentana = 7;

--------

-- V5 ahora lee de la TABLA materializada (sin recalcular secuencia)
CREATE OR ALTER VIEW dbo.ConCuboSecuenciasBloques_Rango_M11_Completo AS
SELECT
    *
FROM dbo.ConCuboSecuenciasBloques_Rango_M11_Completo_MAT;   -- ← APUNTA A LA MAT V5
GO

--------
SELECT TOP 20 
    ID_Limpio, FechaSecuencia, SecuenciaGlobalSQL,
    Turno, Maquinista, BuenosTotal
FROM dbo.ConCuboSecuenciasBloques_Rango_M11_Completo
ORDER BY SecuenciaGlobalSQL ASC;

----

--power bi

EXEC dbo.RefrescarV5_Incremental @DiasVentana = 7;
SELECT * FROM dbo.ConCuboSecuenciasBloques_Rango_M11_Completo;

----

--vía 2. 2.4M:

--1) Vista por EVENTO (sin colapso)

--Toma directamente la vista de eventos ConCubo3AñosSecFlag_Completo 
--y le suma los joins + campos útiles.

CREATE OR ALTER VIEW dbo.MED_V4E_Eventos_Completo
AS
WITH VU AS (
    SELECT TRY_CAST(OP AS INT) AS ID_Limpio, MIN(saccod1) AS saccod1
    FROM dbo.TablaVinculadaUNION
    WHERE ISNUMERIC(OP)=1
    GROUP BY TRY_CAST(OP AS INT)
),
NEW_base AS (
    SELECT
        COALESCE(TRY_CAST(NroGlobal AS INT),
                 TRY_CAST(SUBSTRING(OP, PATINDEX('%[0-9]%', OP), 50) AS INT)) AS ID_Limpio,
        OP, CodAlfa, CodMaq,
        TRY_CAST(Alto AS INT) AS Alto,
        TRY_CAST(Ancho AS INT) AS Ancho,
        TRY_CAST(AltoV AS INT) AS AltoV,
        TRY_CAST(Fuelle AS INT) AS Fuelle
    FROM dbo.TablaVinculadaNEW
),
NEWmap AS (
    SELECT ID_Limpio,
           MAX(OP) AS OP, MAX(CodAlfa) AS CodAlfa, MAX(CodMaq) AS CodMaq,
           MAX(Alto) AS Alto, MAX(Ancho) AS Ancho, MAX(AltoV) AS AltoV, MAX(Fuelle) AS Fuelle
    FROM NEW_base
    WHERE ID_Limpio IS NOT NULL
    GROUP BY ID_Limpio
)
SELECT
    s.Renglon, s.ID, s.ID_Limpio,
    s.Estado,
    s.Inicio_Corregido  AS InicioSecuencia,
    s.Fin_Corregido     AS FinSecuencia,
    CONVERT(date, s.Inicio_Corregido) AS FechaSecuencia,
    s.Horas_Produccion  AS HorasProd,
    s.Horas_Preparacion AS HorasPrep,
    s.Horas_Parada      AS HorasPara,
    s.Horas_Mantenimiento AS HorasMant,
    CAST(ISNULL(s.CantidadBuenosProducida,0) AS DECIMAL(18,4)) AS BuenosTotal,
    CAST(ISNULL(s.CantidadMalosProducida ,0) AS DECIMAL(18,4)) AS MalosTotal,
    s.Turno, s.Maquinista, s.Operario,
    s.CodProducto, s.Motivo,
    -- Orden estable por evento
    CAST(REPLACE(REPLACE(REPLACE(CONVERT(varchar(19), s.Inicio_Corregido, 120),'-',''),' ',''),':','') AS DECIMAL(38,0)) * 10000000000
      + CAST(s.Renglon   AS DECIMAL(38,0)) * 1000000000
      + CAST(s.ID_Limpio AS DECIMAL(38,0)) AS SortKey,
    VU.saccod1,
    N.OP, N.CodAlfa, N.CodMaq,
    N.Alto, N.Ancho, N.AltoV, N.Fuelle
FROM dbo.ConCubo3AñosSecFlag_Completo s   -- <<< por EVENTO, sin GROUP BY
LEFT JOIN VU     ON VU.ID_Limpio  = s.ID_Limpio
LEFT JOIN NEWmap N ON N.ID_Limpio = s.ID_Limpio
WHERE s.Inicio_Corregido IS NOT NULL AND s.Fin_Corregido IS NOT NULL;
GO

------------

--2) Materialización por evento (esperado ~2,4M)

IF OBJECT_ID('dbo.MED_V5E_Eventos_MAT','U') IS NOT NULL
    DROP TABLE dbo.MED_V5E_Eventos_MAT;

SELECT *
INTO dbo.MED_V5E_Eventos_MAT
FROM dbo.MED_V4E_Eventos_Completo;
GO

---

SELECT COUNT(*) AS TotalFilas_Eventos
FROM dbo.MED_V5E_Eventos_MAT;
GO

------------

--3) Índices

IF EXISTS (
  SELECT 1 FROM sys.indexes
  WHERE name='CIX_V5E_Fecha_Renglon_ID'
    AND object_id=OBJECT_ID('dbo.MED_V5E_Eventos_MAT')
)
BEGIN
  DROP INDEX CIX_V5E_Fecha_Renglon_ID ON dbo.MED_V5E_Eventos_MAT;
END
GO

CREATE CLUSTERED INDEX CIX_V5E_Fecha_Renglon_ID
ON dbo.MED_V5E_Eventos_MAT (FechaSecuencia, Renglon, ID_Limpio);
GO

CREATE NONCLUSTERED INDEX IX_V5E_Renglon_ID
ON dbo.MED_V5E_Eventos_MAT (Renglon, ID_Limpio)
INCLUDE (InicioSecuencia, FinSecuencia, Estado, HorasProd, HorasPrep, BuenosTotal);
GO

--------

/* =============================================================
   PASO 4: SP: RefrescarV5E_Incremental  (Versión corregida)
   Proyecto: Medoro 13 – Camino EVENTOS
   Objetivo:
     - Mantener 3 años exactos de datos (rolling purge)
     - Alinear cortes con TZ Argentina
     - Evitar desfase -2d en la ventana
   ============================================================= */
CREATE OR ALTER PROCEDURE dbo.RefrescarV5E_Incremental
  @DiasVentana INT = 7
AS
BEGIN
  SET NOCOUNT ON;
  BEGIN TRY
    /* 🔧 Zona horaria local y ventana */
    DECLARE @HoyAR DATE = CAST(SYSDATETIMEOFFSET() AT TIME ZONE 'Argentina Standard Time' AS DATE);
    DECLARE @Lim3A DATE = DATEADD(DAY, -1095, @HoyAR);  -- 3 años
    DECLARE @FechaCorte DATE = DATEADD(DAY, -@DiasVentana, @HoyAR);

    /* 🔧 Purga rolling (antigüedad > 3 años) */
    DELETE FROM dbo.MED_V5E_Eventos_MAT
    WHERE FechaSecuencia < @Lim3A;

    /* 🔧 Borrar últimos X días para recálculo */
    DELETE FROM dbo.MED_V5E_Eventos_MAT
    WHERE FechaSecuencia >= @FechaCorte;

    /* 1️⃣ Obtener ventana desde función parametrizada */
    ;WITH V AS (
      SELECT * FROM dbo.fnConCubo_Ventana(@DiasVentana)
    ),
    S AS (
      SELECT  
        v.Renglon, v.ID,
        TRY_CAST(SUBSTRING(v.ID, PATINDEX('%[0-9]%', v.ID), LEN(v.ID)) AS INT) AS ID_Limpio,
        v.Estado,
        v.Inicio_Corregido AS InicioSecuencia,
        v.Fin_Corregido    AS FinSecuencia,
        CONVERT(date, v.Inicio_Corregido) AS FechaSecuencia,
        CASE WHEN v.Estado='Producción'     THEN DATEDIFF(SECOND, v.Inicio_Corregido, v.Fin_Corregido)/3600.0 ELSE 0 END AS HorasProd,
        CASE WHEN v.Estado='Preparación'    THEN DATEDIFF(SECOND, v.Inicio_Corregido, v.Fin_Corregido)/3600.0 ELSE 0 END AS HorasPrep,
        CASE WHEN v.Estado='Maquina Parada' THEN DATEDIFF(SECOND, v.Inicio_Corregido, v.Fin_Corregido)/3600.0 ELSE 0 END AS HorasPara,
        CASE WHEN v.Estado='Mantenimiento'  THEN DATEDIFF(SECOND, v.Inicio_Corregido, v.Fin_Corregido)/3600.0 ELSE 0 END AS HorasMant,
        CAST(ISNULL(v.CantidadBuenosProducida,0) AS DECIMAL(18,4)) AS BuenosTotal,
        CAST(ISNULL(v.CantidadMalosProducida ,0) AS DECIMAL(18,4)) AS MalosTotal,
        v.Turno, v.Maquinista, v.Operario, v.CodProducto, v.Motivo
      FROM V v
      WHERE v.Inicio_Corregido IS NOT NULL AND v.Fin_Corregido IS NOT NULL
    ),
    VU AS (
      SELECT TRY_CAST(OP AS INT) AS ID_Limpio, MIN(saccod1) AS saccod1
      FROM dbo.TablaVinculadaUNION
      WHERE ISNUMERIC(OP)=1
      GROUP BY TRY_CAST(OP AS INT)
    ),
    NEW_base AS (
      SELECT  
        COALESCE(TRY_CAST(NroGlobal AS INT),
                 TRY_CAST(SUBSTRING(OP, PATINDEX('%[0-9]%', OP), 50) AS INT)) AS ID_Limpio,
        OP, CodAlfa, CodMaq,
        TRY_CAST(Alto AS INT) AS Alto,
        TRY_CAST(Ancho AS INT) AS Ancho,
        TRY_CAST(AltoV AS INT) AS AltoV,
        TRY_CAST(Fuelle AS INT) AS Fuelle
      FROM dbo.TablaVinculadaNEW
    ),
    NEWmap AS (
      SELECT  
        ID_Limpio,
        MAX(OP) AS OP, MAX(CodAlfa) AS CodAlfa, MAX(CodMaq) AS CodMaq,
        MAX(Alto) AS Alto, MAX(Ancho) AS Ancho, MAX(AltoV) AS AltoV, MAX(Fuelle) AS Fuelle
      FROM NEW_base
      WHERE ID_Limpio IS NOT NULL
      GROUP BY ID_Limpio
    )
    INSERT INTO dbo.MED_V5E_Eventos_MAT
    SELECT
      s.Renglon, s.ID, s.ID_Limpio, s.Estado,
      s.InicioSecuencia, s.FinSecuencia, s.FechaSecuencia,
      s.HorasProd, s.HorasPrep, s.HorasPara, s.HorasMant,
      s.BuenosTotal, s.MalosTotal,
      s.Turno, s.Maquinista, s.Operario, s.CodProducto, s.Motivo,
      CAST(REPLACE(REPLACE(REPLACE(CONVERT(varchar(19), s.InicioSecuencia, 120),'-',''),' ',''),':','') AS DECIMAL(38,0)) * 10000000000
        + CAST(s.Renglon   AS DECIMAL(38,0)) * 1000000000
        + CAST(s.ID_Limpio AS DECIMAL(38,0)) AS SortKey,
      VU.saccod1,
      N.OP, N.CodAlfa, N.CodMaq,
      N.Alto, N.Ancho, N.AltoV, N.Fuelle
    FROM S
    LEFT JOIN VU     ON VU.ID_Limpio  = S.ID_Limpio
    LEFT JOIN NEWmap N ON N.ID_Limpio = S.ID_Limpio
    WHERE s.FechaSecuencia >= @FechaCorte;

    PRINT '✅ MED_V5E_Eventos_MAT refrescada (últimos ' 
          + CAST(@DiasVentana AS VARCHAR(10)) 
          + ' días) - ' + CONVERT(VARCHAR(20), SYSDATETIMEOFFSET() AT TIME ZONE 'Argentina Standard Time', 120);
  END TRY
  BEGIN CATCH
    DECLARE @Err NVARCHAR(4000) = ERROR_MESSAGE();
    RAISERROR(@Err, 16, 1);
  END CATCH
END;
GO


----------

--5) Vista final para PBI

CREATE OR ALTER VIEW dbo.MED_V5E_Eventos
AS
SELECT *
FROM dbo.MED_V5E_Eventos_MAT;
GO

---

-- Ejecutalo ahora mismo (después de tener creada MED_V5E_Eventos_MAT)
ALTER TABLE dbo.MED_V5E_Eventos_MAT
ADD FechaSecuenciaTextoHora AS CONVERT(varchar(16), InicioSecuencia, 120) PERSISTED;
GO
--
EXEC sp_refreshview 'dbo.MED_V5E_Eventos';
-- (si tenés más vistas encadenadas, refrescá cada una)


--
--6) Power BI (Advanced) para la vía por evento

EXEC dbo.RefrescarV5E_Incremental @DiasVentana = 7;
SELECT * FROM dbo.MED_V5E_Eventos;





---
--chequeo
SELECT TOP (1) FechaSecuenciaTextoHora
FROM dbo.MED_V5E_Eventos_MAT;


-----
---tema duplicados ver

---it

EXEC dbo.RefrescarV5E_Incremental @DiasVentana = 7;
EXEC dbo.RefrescarV5_Incremental @DiasVentana = 7;


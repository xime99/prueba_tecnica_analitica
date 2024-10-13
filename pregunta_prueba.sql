---------------------------------------------------------------------------------------------
------------------------------ REPORTE PREGUNTA ---------------------------------------------
---------------------------------------------------------------------------------------------
-------- se filtran las transacciones en los últimos 6 meses --------------------------------
-------- se calcula el monto transado por cliente, se identifican los canales utlizados -----

DROP TABLE IF EXISTS proceso_cumplimiento.info_trx_seism PURGE;

CREATE TABLE proceso_cumplimiento.info_trx_seism STORED AS PARQUET AS

WITH
seis_meses AS(
SELECT 
    fecha_transaccion,
    tipo_doc,
    num_doc,
    nombre,
    tipo_persona,
    ingresos_mensuales,
    naturaleza,
    monto,
    cod_canal,
    nom_canal,
    tipo,
    cod_jurisdiccion
FROM proceso_cumplimiento.base_info_final_pruebaxm
WHERE fecha_transaccion >= NOW() - INTERVAL 6 MONTH )

SELECT 
     tipo_doc,
     num_doc,
     nombre,
     tipo_persona,
     ingresos_mensuales,
     SUM(monto) AS total_transacciones,
     GROUP_CONCAT(DISTINCT CAST(cod_canal AS STRING)) canales_usados,
     GROUP_CONCAT(DISTINCT CAST(tipo AS STRING)) tipo_canales_usados
FROM seis_meses
GROUP BY
        num_doc, 
        tipo_doc, 
        nombre, 
        tipo_persona, 
        ingresos_mensuales;

COMPUTE STATS proceso_cumplimiento.info_trx_seism;

---------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------
------------ se agrega el percentil 95 por tipo de cliente y se hace el cruce ---------------

DROP TABLE IF EXISTS proceso_cumplimiento.info_trx_seism_p PURGE;

CREATE TABLE proceso_cumplimiento.info_trx_seism_p STORED AS PARQUET AS

WITH

percentil AS(
SELECT 
    tipo_persona,
    (CASE WHEN tipo_persona = "PERSONA NATURAL" THEN   66759540 ELSE 1393406872 END) percentil
FROM proceso_cumplimiento.info_trx_seism
GROUP BY tipo_persona )

SELECT 
    t1.tipo_doc,
    t1.num_doc,
    t1.nombre,
    t1.tipo_persona,
    t1.ingresos_mensuales,
    t1.total_transacciones,
    t1.canales_usados,
    t1.tipo_canales_usados
FROM proceso_cumplimiento.info_trx_seism t1
LEFT JOIN  percentil t2 ON t1.tipo_persona = t1.tipo_persona
WHERE t1.total_transacciones > 2*t1.ingresos_mensuales
AND t1.total_transacciones > t2.percentil;

COMPUTE STATS proceso_cumplimiento.info_trx_seism_p;

----------------------------------------------------------------------------------------
----------------------------- CONSULTA RESULTADO ---------------------------------------
SELECT 
    tipo_doc,
    num_doc,
    nombre,
    tipo_persona,
    ingresos_mensuales,
    total_transacciones,
    canales_usados,
    tipo_canales_usados
FROM  proceso_cumplimiento.info_trx_seism_p;





































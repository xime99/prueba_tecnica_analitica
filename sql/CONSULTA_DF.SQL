-----------------------------------------------------------------------------------------------------
--------- nos quedamos con la información solo de los clientes que tienen tipo_doc ------------------
-----------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS proceso_cumplimiento.info_clientes_completa PURGE;

CREATE TABLE proceso_cumplimiento.info_clientes_completa STORED AS PARQUET AS

SELECT 
    tipo_doc,
    num_doc,
    nombre,
    tipo_persona,
    ingresos_mensuales
FROM proceso_cumplimiento.clientes_base_prueba_x
WHERE tipo_doc <> "-";

COMPUTE STATS proceso_cumplimiento.info_clientes_completa;

-----------------------------------------------------------------------------------------------------
-----nos quedamos con la información transaccional de los clientes que tienen tipo_doc --------------
-----------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS proceso_cumplimiento.base_trx_final PURGE;

CREATE TABLE proceso_cumplimiento.base_trx_final STORED AS PARQUET AS

SELECT 
    t1.fecha_transaccion,
    t2.tipo_doc,
    t2.num_doc,
    t2.nombre,
    t2.tipo_persona,
    t2.ingresos_mensuales,
    t1.cod_canal,
    t1.naturaleza,
    t1.monto
FROM proceso_cumplimiento.transacciones_base_prueba_x t1
INNER JOIN proceso_cumplimiento.info_clientes_completa t2
ON t1.tipo_doc = t2.tipo_doc AND t1.num_doc = t2.num_doc;

COMPUTE STATS proceso_cumplimiento.base_trx_final;

----------------------------------------------------------------------------------------
---------------------- TABLA CON CRUCE FINAL -------------------------------------------
----------------------------------------------------------------------------------------

DROP TABLE IF EXISTS proceso_cumplimiento.base_info_final_pruebaxm PURGE;

CREATE TABLE proceso_cumplimiento.base_info_final_pruebaxm STORED AS PARQUET AS

SELECT 
    t1.fecha_transaccion,
    t1.tipo_doc,
    t1.num_doc,
    t1.nombre,
    t1.tipo_persona,
    t1.ingresos_mensuales,
    t1.naturaleza,
    t1.monto,
    t1.cod_canal,
    t3.nombre nom_canal,
    t3.tipo,
    t3.cod_jurisdiccion
FROM proceso_cumplimiento.base_trx_final t1
LEFT JOIN proceso_cumplimiento.canales_base_prueba_x t3 ON CAST(t1.cod_canal AS BIGINT) = CAST(codigo AS BIGINT);

COMPUTE STATS proceso_cumplimiento.base_info_final_pruebaxm;

---------------------------------------------------------------------------------------------------------
----------------------------- SE AGREGA INFORMACIÓN DE LAS JURISDICCIONES -------------------------------
---------------------------------------------------------------------------------------------------------

DROP TABLE IF EXISTS proceso_cumplimiento.prueba_base_dane PURGE;

CREATE TABLE proceso_cumplimiento.prueba_base_dane STORED AS PARQUET AS

SELECT DISTINCT
    t2.coddane,
    t2.latitud, 
    t2.longitud,
    t2.municipio,
    t2.departamento 
FROM resultados_cumplimiento.gmr_pbi_jurisdicciones_nacionales t2;

COMPUTE STATS proceso_cumplimiento.prueba_base_dane;

----------------------------------------------------------------------------------------
---------------- TABLA CON CRUCE FINAL AGRUPADA PARA TABLERO ---------------------------
----------------------------------------------------------------------------------------

DROP TABLE IF EXISTS proceso_cumplimiento.base_info_final_tablero PURGE;

CREATE TABLE proceso_cumplimiento.base_info_final_tablero STORED AS PARQUET AS

WITH
fecha_trx AS (
SELECT 
    YEAR(fecha_transaccion) year,
    MONTh (fecha_transaccion) mes,
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
FROM proceso_cumplimiento.base_info_final_pruebaxm)

SELECT 
    year,
    mes,
    tipo_doc,
    num_doc,
    nombre,
    tipo_persona,
    ingresos_mensuales,
    naturaleza,
    SUM(monto) AS monto,
    COUNT(monto) AS recuento,
    cod_canal,
    nom_canal,
    tipo,
    cod_jurisdiccion
FROM fecha_trx 
GROUP BY  
    year,
    mes,
    tipo_doc,
    num_doc,
    nombre,
    tipo_persona,
    ingresos_mensuales,
    naturaleza,
    cod_canal,
    nom_canal,
    tipo,
    cod_jurisdiccion;

COMPUTE STATS proceso_cumplimiento.base_info_final_tablero;
























/*

------------------- CONSULTAS DE INICIALES PARA LA TOMA DE DECISIONES -------------------------------
-----------------------------------------------------------------------------------------------------
-----Cuento cuantos num_doc tiene diferente tipo_doc en la base de clientes con transacciones -------
-----------------------------------------------------------------------------------------------------

DROP TABLE IF EXISTS proceso_cumplimiento.repetidos_clientes_x PURGE;

CREATE TABLE proceso_cumplimiento.repetidos_clientes_x STORED AS PARQUET AS

WITH
cuenta_duplicados AS(
SELECT 
    num_doc,
    count(tipo_doc) cuenta_doc 
FROM proceso_cumplimiento.info_clientes_completa
GROUP BY num_doc)

SELECT 
t1.num_doc,
t1.tipo_doc,
t2.cuenta_doc,
t1.nombre,
t1.tipo_persona,
t1.ingresos_mensuales
from proceso_cumplimiento.info_clientes t1
LEFT JOIN cuenta_duplicados t2 on t1.num_doc = t2.num_doc
order by cuenta_doc,t1.num_doc;

COMPUTE STATS proceso_cumplimiento.repetidos_clientes_x;

-----------------------------------------------------------------------------------------------------
--------- se agrupa las transacciones por mes para validar si son transacciones repetidas  -----------
-----------------------------------------------------------------------------------------------------

DROP TABLE IF EXISTS proceso_cumplimiento.calculo_mov_mes PURGE;

CREATE TABLE proceso_cumplimiento.calculo_mov_mes STORED AS PARQUET AS

WITH 
total_mes AS(
SELECT 
    tipo_doc,
    num_doc,
    DATE_TRUNC('month', fecha_transaccion) AS mes,  
    SUM(monto) AS movimiento_mensual,
    cod_canal,
    naturaleza
FROM proceso_cumplimiento.transacciones_base_prueba_x 
GROUP BY tipo_doc, num_doc, cod_canal,DATE_TRUNC('month', fecha_transaccion),naturaleza)

SELECT 
    t1.tipo_doc,
    t2.cuenta_doc,
    t1.num_doc,
    t1.mes,  
    t1.movimiento_mensual,
    t1.cod_canal,
    t1.naturaleza
FROM total_mes t1 
INNER JOIN proceso_cumplimiento.repetidos_clientestrx_x t2 ON t1.num_doc = t2.num_doc;

COMPUTE STATS proceso_cumplimiento.calculo_mov_mes;

---------------------------------------------------------------------------------
---- validación de las transacciones repetidas cuanto tienen en tipo_doc = - 

DROP TABLE IF EXISTS proceso_cumplimiento.validar_repetidos_trx_pa PURGE;
CREATE TABLE proceso_cumplimiento.validar_repetidos_trx_pa STORED AS PARQUET AS

WITH
con_tipodoc AS (
SELECT 
    tipo_doc,
    cuenta_doc,
    num_doc,
    mes,
    movimiento_mensual,
    cod_canal,
    naturaleza
FROM proceso_cumplimiento.calculo_mov_mes
WHERE tipo_doc <> "-"),

sin_tipodoc AS (
SELECT 
    tipo_doc doc_tipo,
    cuenta_doc cuenta,
    num_doc id,
    mes periodo,
    movimiento_mensual mov_men,
    cod_canal id_canal,
    naturaleza e_s
FROM proceso_cumplimiento.calculo_mov_mes
WHERE tipo_doc = "-")

SELECT 
    num_doc,
    id,
    mes,
    periodo,
    movimiento_mensual,
    mov_men,
    cod_canal,
    id_canal,
    naturaleza,
    e_s,
    tipo_doc,
    doc_tipo
FROM con_tipodoc t1
INNER JOIN sin_tipodoc t2 ON (num_doc=id AND mes=periodo AND movimiento_mensual=mov_men AND cod_canal = id_canal AND naturaleza = e_s)
WHERE cuenta_doc > 1;

COMPUTE STATS proceso_cumplimiento.validar_repetidos_trx_pa;

*/
























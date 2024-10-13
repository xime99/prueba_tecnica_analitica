
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

-------------------------------------------------------------------------------
--------------------------------------------------------------------------------
--- SE IDENTIFICAN CUANTOS TIPO DE DOCUMENTO TIENE CADA NUMERO DE DOCUMENTO DENTRO DE LA TABLA DE TRANSACCIONES
DROP TABLE IF EXISTS proceso_cumplimiento.repetidos_clientestrx_x PURGE;

CREATE TABLE proceso_cumplimiento.repetidos_clientestrx_x STORED AS PARQUET AS

WITH 
nit_duplicados AS (
SELECT DISTINCT
    tipo_doc,
    num_doc
FROM proceso_cumplimiento.transacciones_base_prueba_x),

cuenta_duplicados AS (
SELECT num_doc,
    count(tipo_doc) cuenta_doc 
FROM  nit_duplicados
GROUP BY num_doc)

SELECT 
    t1.num_doc,
    t1.tipo_doc,
    t2.cuenta_doc
FROM nit_duplicados t1
LEFT JOIN cuenta_duplicados t2 ON t1.num_doc = t2.num_doc
ORDER BY cuenta_doc,t1.num_doc DESC;

COMPUTE STATS proceso_cumplimiento.repetidos_clientestrx_x;

select * from proceso_cumplimiento.repetidos_clientestrx_x;
SELECT COUNT(DISTINCT num_doc) FROM proceso_cumplimiento.repetidos_clientestrx_x;


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



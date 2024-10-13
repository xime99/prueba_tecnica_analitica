DESCRIBE proceso_cumplimiento.clientes_base_prueba_x;
DESCRIBE proceso_cumplimiento.canales_base_prueba_x;
DESCRIBE proceso_cumplimiento.transacciones_base_prueba_x;
---------------------------------------------------------------------
COMPUTE STATS proceso_cumplimiento.clientes_base_prueba_x;
COMPUTE STATS proceso_cumplimiento.canales_base_prueba_x;
COMPUTE STATS proceso_cumplimiento.transacciones_base_prueba_x;
------------------------------------------------------------------------
SELECT COUNT(1) FROM proceso_cumplimiento.clientes_base_prueba_x;
SELECT COUNT(1) FROM proceso_cumplimiento.canales_base_prueba_x;
SELECT COUNT(1) FROM proceso_cumplimiento.transacciones_base_prueba_x;
-------------------------------------------------------------------------
SELECT tipo_persona, COUNT(num_doc) AS cuenta from proceso_cumplimiento.clientes_base_prueba_x GROUP BY tipo_persona;
SELECT tipo_doc, COUNT(num_doc) AS cuenta from proceso_cumplimiento.clientes_base_prueba_x GROUP BY tipo_doc;
SELECT avg(ingresos_mensuales) FROM proceso_cumplimiento.clientes_base_prueba_x;
-------------------------------------------------------------
SELECT COUNT(DISTINCT codigo) FROM proceso_cumplimiento.canales_base_prueba_x;
SELECT COUNT(DISTINCT cod_jurisdiccion) FROM proceso_cumplimiento.canales_base_prueba_x;
SELECT tipo, COUNT(nombre) AS cuenta FROM proceso_cumplimiento.canales_base_prueba_x GROUP BY tipo; 
---------------------------------------------------------------------------------------
SELECT MIN(fecha_transaccion),MAX(fecha_transaccion) FROM proceso_cumplimiento.transacciones_base_prueba_x;
SELECT COUNT(DISTINCT cod_canal) FROM proceso_cumplimiento.transacciones_base_prueba_x;
SELECT COUNT(DISTINCT num_doc) FROM proceso_cumplimiento.transacciones_base_prueba_x;
SELECT tipo_doc, COUNT(num_doc) AS cuenta from proceso_cumplimiento.transacciones_base_prueba_x GROUP BY tipo_doc;
SELECT naturaleza, COUNT(num_doc) AS cuenta from proceso_cumplimiento.transacciones_base_prueba_x GROUP BY naturaleza;






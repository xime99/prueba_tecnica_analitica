import os 

#--------------------------------------------------------------------------------

df_daset = f'''
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
    FROM {"proceso_cumplimiento.base_info_final_pruebaxm" }
    '''

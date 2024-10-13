
import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt

import os
import time
from datetime import datetime
from sparky_bc import Sparky
from retry import retry
from helper import Helper
import re
import shutil
from tabulate import tabulate
from sql import queries as query

import warnings
warnings.filterwarnings("ignore", category=UserWarning)


#-----------------------------------------------------------------------------------------
#----------------------------------- RUTAS  ----------------------------------------------
#----------------------------------------------------------------------------------------- 

user = os.getenv('user')
pwd = os.getenv('pwd')
dsn = os.getenv('dsn')
hp = Helper(dsn = dsn)
ruta_sql = os.path.join(os.getcwd(), 'sql')

#-----------------------------------------------------------------------------------------
#----------------------------------- INSTACIAR HOSTNAME ----------------------------------
#-----------------------------------------------------------------------------------------

def instanciar():
    
    '''
    Instancia el proceso asociado a un API de de alto nivel para interactuar con Spark 
    y en los servidores del Banco. Si no se logra realizar la conexión el proceso no se 
    crea exitosamente.
    '''
    
    list_hostnames = [
            'sbmdeblze003.bancolombia.corp',
            'sbmdeblze004.bancolombia.corp'
            ]
    
    for hostname in list_hostnames:
        try:
            sp = Sparky(
                dsn = dsn, 
                hostname=hostname,
                username = user, 
                password = pwd
                )
            print('Conexión con el hostname :' + hostname)
            break
        except:
            print('ERROR de conexión con el hostname :' + hostname)
    return sp

#-----------------------------------------------------------------------------------------
#--------------------------------- EJECUTAR ETL ------------------------------------------
#-----------------------------------------------------------------------------------------

def ejectuar_etl():
    
    ruta_ETL = os.path.join(ruta_sql,"CONSULTA_DF.sql")
    
    parametros = {'usuario':user}
    
    hp.ejecutar_archivo(ruta = ruta_ETL, params = parametros)
    
    
    return print('Se ha ejecutado el ETL con éxito')


#-----------------------------------------------------------------------------------------
#------------------------------ CONSULTAR DATAFRAME --------------------------------------
#-----------------------------------------------------------------------------------------

@retry(tries=5, delay=0)
def query_to_df(sp,query, log=True):

    '''
    Desarrolla una consulta SQl sobre la Landing Zone y la retorna como un DataFrame

    query : string
    consulta que se desea realizar en la Landing Zone

    log: bool
    log del evento de ejecución e información basica del dataframe
    '''

    conn = sp.helper.conn
    start_time = datetime.now()
    dataframe = pd.read_sql(query,conn)
    end_time = datetime.now()

    if log:

        info = [[
            start_time.strftime('%d-%m-%Y-%H:%M:%S'),
            end_time.strftime('%d-%m-%Y-%H:%M:%S'),
            str(end_time - start_time)[:10],
            dataframe.shape[0],
            dataframe.shape[1]
            ]]

        tabla = tabulate(
            info,
            headers=[
                'Start Query',
                'End Query',
                'Execution time',
                'Rows',
                'Columns'],
            tablefmt='simple_grid'
            )
        print(tabla)
    return dataframe

def consulta_df(sp):
    
    result = {}
    print('Exracción del dataset')
    result['df_dataset'] = query_to_df(sp,query.df_daset,log=True)

    return result

#-----------------------------------------------------------------------------------------
#---------------------DF CON NUMERO DE REGISTROS Y PORCENTAJE ----------------------------
#-----------------------------------------------------------------------------------------

def calcular_frecuencia_porcentaje(df, columna):

    conteos = df[columna].value_counts()

    # Calcular los porcentajes
    porcentaje = (conteos / conteos.sum()) * 100

    tabla_resultados = pd.DataFrame({
        'Frecuencia': conteos,
        'Porcentaje (%)': porcentaje
    })

    return tabla_resultados


#-----------------------------------------------------------------------------------------
#-------------------------------- HISTOGRAMA Y BOXPLOT -----------------------------------
#-----------------------------------------------------------------------------------------

def plot_histogram_and_boxplot(data, column_name, bins=30):
    """
    Crea un histograma y un boxplot para una columna específica de un DataFrame.

    Parameters:
    data (DataFrame): El DataFrame que contiene los datos.
    column_name (str): El nombre de la columna para la cual se van a crear los gráficos.
    bins (int): Número de bins para el histograma. Valor predeterminado es 30.

    Returns:
    None
    """
    plt.figure(figsize=(10, 4))

    # Subplot 1: Histograma
    plt.subplot(1, 2, 1)
    sns.histplot(data[column_name], bins=bins, kde=True)
    plt.xlabel(column_name)
    plt.ylabel('Frecuencia')
    plt.title(f'Histograma de {column_name}')

    # Subplot 2: Boxplot
    plt.subplot(1, 2, 2)
    sns.boxplot(y=data[column_name])
    plt.ylabel(column_name)
    plt.title(f'Boxplot de {column_name}')

    # Mostrar los gráficos
    plt.tight_layout()
    plt.show()
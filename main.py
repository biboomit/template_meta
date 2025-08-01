# %%
import pandas as pd
import requests
from requests_oauthlib import OAuth2Session
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
import facebook_business
from datetime import datetime, timedelta, date  # Cambié la importación aquí
import smtplib
import numpy as np
from google.cloud import bigquery
from google.oauth2 import service_account
import os
from pandas_gbq import to_gbq

# %%
def queryBigQuery(query):
    credentials = service_account.Credentials.from_service_account_file(
        'key.json', scopes=["https://www.googleapis.com/auth/cloud-platform","https://www.googleapis.com/auth/drive",
        "https://www.googleapis.com/auth/bigquery"],
    )
    client = bigquery.Client(credentials=credentials, project=credentials.project_id,)
    query_job = client.query(query)
    df = query_job.to_dataframe()
    return df

# %%
#función para cargar el DataFrame a BigQuery
def load_data_to_bigquery(dataframe, project_id, table_id):
    dataframe.to_gbq(table_id, project_id=project_id,  if_exists="replace")

# %%
def rename_columns(df_to_rename, df_new_names):
    # Crear un diccionario de equivalencias
    new_names = []

    # Recorre cada columna del DataFrame original
    for column in df_to_rename.columns:
        # Buscar el nombre estandarizado correspondiente en el DataFrame `df_new_names`
        api_name = df_new_names.loc[df_new_names['NOMBRE_API'] == column, 'NOMBRE_TABLA']
        
        # Si existe un nombre estandarizado, añadirlo a la lista
        if not api_name.empty:
            new_names.append(api_name.values[0])
        else:
            new_names.append(None)

    # Crear un diccionario de columnas originales a estandarizadas
    new_column_names_dict = dict(zip(df_to_rename.columns, new_names))
    
    # Renombrar las columnas en el DataFrame
    df_to_rename = df_to_rename.rename(columns=new_column_names_dict)

    return df_to_rename

# %%
def change_column_types(df_to_change, df_new_types):
    # Creamos un diccionario vacío para almacenar los nuevos tipos de datos
    for column in df_to_change.columns:
        matched_row = df_new_types[df_new_types['NOMBRE_TABLA'] == column]

        if not matched_row.empty:
            new_type = matched_row['TIPO_DATO'].iloc[0]

            if new_type !='date':
                df_to_change[column] = df_to_change[column].astype(new_type)
            else:
                df_to_change['fecha'] = pd.to_datetime(df_to_change['fecha'])
                df_to_change['fecha'] = pd.to_datetime(df_to_change['fecha'], format='%Y-%m-%d')
                
    return df_to_change

# %%
def update_access_token(token,id):
    # Actualiza el valor de 'access_token' en la tabla
    query = f'''
        UPDATE `dimensiones.Data_Cruda.metaapi`
        SET access_token = '{token}'
        where plataforma ='Meta'
        AND id='{id}'
    '''
    queryBigQuery(query)

# %%
def get_token(id):
    query = f"SELECT * FROM `dimensiones.Data_Cruda.metaapi` WHERE id='{id}'"
    bx_data = queryBigQuery(query)
    return bx_data

# %%
def get_dimensions():
    query = """
        SELECT 
            CONCEPTO,
            NOMBRE_API,
            TIPO_DATO,
            NOMBRE_TABLA,
            TIPO_CAMPO,
            SOURCE
        FROM `dimensiones.Data_Cruda.campos_estandar_api`
    """
    bx_data = queryBigQuery(query)
    return bx_data

# %%
# Función auxiliar para obtener el token de larga duración
def get_long_lived_token(app_id, app_secret, access_token, api_version):

    url = f'https://graph.facebook.com/{api_version}/oauth/access_token'
    
    params = {
        'grant_type': 'fb_exchange_token',
        'client_id': app_id,
        'client_secret': app_secret,
        'fb_exchange_token': access_token
    }

    # Imprimir URL completa con parámetros
    requests.Request('GET', url, params=params).prepare().url

    response = requests.get(url, params=params)
    
    if response.status_code == 200:
        print(f"Token de larga duración obtenido: {response.json()['access_token']}")
        return response.json()['access_token']
    else:
        print(f"Error al obtener el token: {response.text}")
        return None

#Función auxiliar para calcular el rango de fechas
def get_date_range(days_back=5):
    hoy = datetime.now()
    until = hoy - timedelta(days=1)

    since = hoy - timedelta(days=days_back)
    return since.strftime('%Y-%m-%d'), until.strftime('%Y-%m-%d')


# %%
def get_data(ids):
    # Obtener la configuración de dimensiones y métricas para Meta desde BigQuery
    dimensions_data = get_dimensions()
    filtered_data = dimensions_data[dimensions_data['SOURCE'] == 'Meta']  # Filtrar por "Meta"
    
    # Extraer solo las columnas necesarias para renombrar y cambiar tipos
    #rename_map = filtered_data[['NOMBRE_API', 'NOMBRE_TABLA']]  # Mapeo para renombrar columnas
    column_types = filtered_data[['NOMBRE_TABLA', 'TIPO_DATO']]  # Mapeo para tipos de datos

    final_df = pd.DataFrame()
    
    for id in ids:
        ads_data = []  # Lista para almacenar los datos de insights

        # Obtener datos de la tabla de BigQuery (tokens y credenciales)
        df = get_token(id)

        # Configurar la API de Meta con los datos obtenidos
        my_app_id = df['app_id'].iloc[0]
        my_app_secret = df['app_secret'].iloc[0]
        my_access_token = df['access_token'].iloc[0]
        graph_api_version = df['api_version'].iloc[0]
        my_account_id = df['account_id'].iloc[0]

        FacebookAdsApi.init(my_app_id, my_app_secret, my_access_token)

        # Obtener el token de acceso de larga duración
        long_lived_token = get_long_lived_token(my_app_id, my_app_secret, my_access_token, graph_api_version)
        if not long_lived_token:
            continue  # Si no se obtiene el token, pasa a la siguiente iteración

        # Actualizar el token de acceso en la base de datos
        update_access_token(long_lived_token, id)

        # Configurar la cuenta publicitaria
        my_account = AdAccount(f'act_{my_account_id}')
        my_account.access_token = long_lived_token

        # Obtener fechas
        since_str, until_str = get_date_range(days_back=15)  # Cambia el número de días según sea necesario

        # Obtener las dimensiones y métricas para la consulta
        dimensions_data = get_dimensions()
        filtered_data = dimensions_data[dimensions_data['SOURCE'] == 'Meta']
        fields = dimensions_data[(dimensions_data['SOURCE'] == 'Meta') & (dimensions_data['TIPO_CAMPO'] == 'field')]
        fields = fields['NOMBRE_API'].tolist()
        breakdowns=['country']
        
        # Obtener insights de la API de Meta
        try:
            insights = my_account.get_insights(
                fields=fields,
                params={
                    'time_increment': 1,
                    'level': 'ad',
                    'time_range': {'since': since_str, 'until': until_str},
                    'breakdowns': breakdowns
                }
            )
        except Exception as e:
            print(f"Error al obtener insights de la API: {e}")
            continue

        # Procesar los insights
        for insight in insights:
            lead = [int(action['value']) for action in insight.get('actions', []) if action['action_type'] == 'lead']
            contact = [int(action['value']) for action in insight.get('actions', []) if action['action_type'] == 'onsite_conversion.messaging_first_reply']
            submit_application = [int(action['value']) for action in insight.get('actions', []) if action['action_type'] == 'offsite_conversion.fb_pixel_custom']
            omni_purchases = [int(action['value']) for action in insight.get('actions', []) if action['action_type'] == 'omni_purchase']
            add_payment_info = [int(action['value']) for action in insight.get('actions', []) if action['action_type'] == 'offsite_conversion.fb_pixel_add_payment_info']

            # Filtrar por `campaign_name` que comienza con 'BOOMIT_'
            if insight.get(filtered_data.loc[filtered_data['CONCEPTO'] == 'campana', 'NOMBRE_API'].iloc[0], '').startswith('BOOMIT_'):
                ads_data.append({
                    'fecha': insight[filtered_data.loc[filtered_data['CONCEPTO'] == 'Fecha', 'NOMBRE_API'].iloc[0]],  # Mapea usando `NOMBRE_API`
                    'pais': insight[filtered_data.loc[filtered_data['CONCEPTO'] == 'Pais', 'NOMBRE_API'].iloc[0]],
                    'nombre_campana': insight[filtered_data.loc[filtered_data['CONCEPTO'] == 'campana', 'NOMBRE_API'].iloc[0]],
                    'inversion': str(insight[filtered_data.loc[filtered_data['CONCEPTO'] == 'Inversion', 'NOMBRE_API'].iloc[0]]),
                    'impresiones': insight[dimensions_data.loc[dimensions_data['CONCEPTO'] == 'Impresiones', 'NOMBRE_API'].iloc[0]],
                    'clicks': insight[filtered_data.loc[filtered_data['CONCEPTO'] == 'Clicks', 'NOMBRE_API'].iloc[0]],
                    'lead': lead[0] if lead else None,
                    'contact': contact[0] if contact else None,
                    'submit_application': submit_application[0] if submit_application else None,
                    'omni_purchases': omni_purchases[0] if omni_purchases else None,
                    'add_payment_info': add_payment_info[0] if add_payment_info else None,
                    'ad_name': insight[filtered_data.loc[filtered_data['CONCEPTO'] == 'Nombre ad', 'NOMBRE_API'].iloc[0]],
                    'adset_name': insight[filtered_data.loc[filtered_data['CONCEPTO'] == 'Nombre adgrop ', 'NOMBRE_API'].iloc[0]]

                })

        # Crear el DataFrame temporal con los resultados de esta propiedad
        ads_df = pd.DataFrame(ads_data)
        
        # Cambiar los tipos de columnas según la configuración
        ads_df = change_column_types(ads_df, column_types)

        # Concatenar con el DataFrame final
        final_df = pd.concat([final_df, ads_df], ignore_index=True)

    return final_df

# %%
def process_and_load_data(request):
    project_id = "example_project_id"
    table_id = "example_table_id"

    ids = []  # Lista de IDs a procesar
    # Obtener los datos
    data_meta = get_data(ids)

    # Cargar los datos a BigQuery
    load_data_to_bigquery(data_meta, project_id, table_id)
    
    return "0"
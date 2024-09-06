# -*- coding: utf-8 -*-
"""
Created on Thu Sep  5 23:12:34 2024

@author: vanessa coudert
"""

# Import libraries
import requests
import json
import pandas as pd



# FUNCTIONS

def read_json_api(url, key1, skey1):

    '''Reads and parses json data from an API and returns the
       json data as a dataframe. '''
    
    # Reda json file and handle errors
    try:
        response = requests.get(url, timeout=120) # Request to API with timeout
        response.raise_for_status()               # Exception for HTTP errors
               
        # Parse JSON response to a Python dictionary
        json_file = response.json()
        print("Successfully fetched data!")
        
            
    except requests.exceptions.HTTPError as http_err:
        if http_err.response:
            return print(f"Failed to load JSON data. Error status code:\n\
                  {http_err.response.status_code}, HTTP error: {http_err}")
        else:
            return print(f"HTTP error occurred: {http_err}")

    except requests.exceptions.ConnectionError as conn_err:
        if conn_err.response:
            return print(f"Failed to load JSON data. Error status code:\n\
                  {conn_err.response.status_code}, Connection error: {conn_err}")
        else: 
            return print(f"Connection error occurred: {conn_err}")

    except requests.exceptions.Timeout as timeout_err:
        if timeout_err.response:
            return print(f"Failed to load JSON data. Error status code:\n\
                  {timeout_err.response.status_code}, Connection error: {timeout_err}")
        else: 
            return print(f"Timeout occurred: {timeout_err}")

    except requests.exceptions.RequestException as req_err:
        if req_err.response:
            return print(f"Failed to load JSON data. Error status code:\n\
                  {req_err.response.status_code}, Connection error: {req_err}")
        else: 
            return print(f"An error occurred: {req_err}")

    except ValueError as json_err:
        if json_err.response:
            return print(f"Failed to load JSON data. Error status code:\n\
                  {json_err.response.status_code}, Connection error: {json_err}")
        else: 
            return print(f"JSON decode error: {json_err}")
            
     
    # Get data in key1 and sub-key1 
    json_dict = json_file[key1][skey1]
    
    # Get dataframe
    df = pd.json_normalize(json_dict, max_level=0)
    
    
    return df


def explode_json_df_cols(df):
    
    '''Explodes a column that contains a json dictionary up to 1
       level. The resulting dataframe may contain nested lists'''
    
    # get columnslist
    df_cols = list(df.columns)
    
    # Get columns to unpack:
    cols_to_unpack = []
    
    # Check if a column contains dictionaries, if yes then unpack
    for col in df_cols:   
        if df[col].apply(lambda x: isinstance(x, dict)).all():  # Check if a column contains dictionaries, if yes then unpack
            cols_to_unpack.append(col)
    
    # Explode columns
    df_exp = df.explode(column=cols_to_unpack)
    
    # Explode columns
    df_exp = df.explode(column=cols_to_unpack)
    
    # Lists to unpack
    list_to_unpack = []

    for col in df_cols:   
        if df_exp[col].apply(lambda x: isinstance(x, list)).all():  # Check if a column contains lists, if yes then unpack
            list_to_unpack.append(col)

    df_exp2 =  df_exp.explode(column=list_to_unpack)
    
    return df_exp2


# SCRAP JSON DATA

# URL to get data
url = "https://api.db.nomics.world/v22/series/CEPII/CHELEM-TRADE-CHEL?facets=1&format=json&limit=1000&observations=1"

# Reda Json data
df_data = read_json_api(url, 'series', ['docs'])                                # Time Series
df_ref_data = read_json_api(url, 'dataset', ['dimensions_values_labels'])       # Reference data


# Sort out columns with dictionaries (explode json columns)
df_data_exp = explode_json_df_cols(df_data)
df_ref_data_exp = explode_json_df_cols(df_ref_data)

# DATASET INFO






# SAVE DATAFRAMES TO CSV/TEXT FILES

# Time series to csv
df_data_exp.to_csv('CHELEM_TRADE_CHEL.csv')

# Ref data to csv
df_ref_data_exp.to_csv('CHELEM_TRADE_CHEL_REFDATA.csv')

































































































































# -*- coding: utf-8 -*-
"""
Created on Mon Sep  2 17:51:24 2024

@author: c23091913
"""

# Import libraries
import requests
import json
import pandas as pd

# Set pandas display options
pd.set_option('display.max_columns', None)
pd.set_option('display.max_colwidth', 25)
pd.set_option('display.width', 200) 

## FUNCTION

def read_json_api(url, key1, skeys=[]):

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
            
     
    # Keys to keep
    skeys_to_extract = skeys   
    json_dict = {key: json_file[key1].get(key, None) for key in skeys_to_extract}
    
    # Get dataframe
    df = pd.json_normalize(json_dict, max_level=0)
    
    
    return df

####################### GETTING DATA

# URL to get data
url = "https://api.db.nomics.world/v22/series/CEPII/CHELEM-TRADE-CHEL?facets=1&format=json&limit=1000&observations=1"

# Reda Json data
df_data = read_json_api(url, 'series', ['docs'])                                # Time Series
df_ref_data = read_json_api(url, 'dataset', ['dimensions_values_labels'])       # Reference data
df_info = read_json_api(url, 'dataset', ['description'])                        # Dat set info (in text file)

#################### CLEANING COLUMNS - time series

# Explode arrays (1000 obs for each 55 period)
df_full = df_data.explode(column=['period','period_start_day','value'])

df_full.info(verbose=True)
df_full.head()


#################### CLEANING COLUMNS - reference data

df_ref_exp = df_ref_data.explode(column=['exporter', 'importer'])

df_ref_exp.info(verbose=True)
df_ref_exp.head()


#################### CLEANING COLUMNS (FUNCTION) Im still working on this...

# Unpack columns 

# cols_to unpack = ['exporter', 'importer']

# Wrap this 'if' inside for loop for each col to unpack:
#if df['dimensions'].apply(lambda x: isinstance(x, dict)).all():                # Check if a column contains dictionaries, if yes then unpack:
#    df[new_col] = df.explode(column=['exporter'])



###################### SAVE AS (Ideally wrapped in a function too. Working on it...)

# Time series to csv
df_full.to_csv('CHELEM_TRADE_CHEL.csv')

# Ref data to csv
df_ref_exp.to_csv('CHELEM_TRADE_CHEL_REFDATA.csv')

# Dataset Info to text
df_info.to_csv('Dataset_Info.txt', sep='\t', index=False)











































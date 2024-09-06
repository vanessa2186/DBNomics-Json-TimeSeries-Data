# -*- coding: utf-8 -*-
"""
Created on Sun Sep  1 16:59:20 2024

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

# URL to get data
url = "https://api.db.nomics.world/v22/series/CEPII/CHELEM-TRADE-CHEL?facets=1&format=json&limit=1000&observations=1"

try:
    response = requests.get(url, timeout=120) # Request to API with timeout
    response.raise_for_status()               # Exception for HTTP errors
           
    # Parse JSON response to a Python dictionary
    json_file = response.json()
    print("Successfully fetched data!")
    
        
except requests.exceptions.HTTPError as http_err:
    if http_err.response:
        print(f"Failed to load JSON data. Error status code:\n\
              {http_err.response.status_code}, HTTP error: {http_err}")
    else:
        print(f"HTTP error occurred: {http_err}")

except requests.exceptions.ConnectionError as conn_err:
    if conn_err.response:
        print(f"Failed to load JSON data. Error status code:\n\
              {conn_err.response.status_code}, Connection error: {conn_err}")
    else: 
        print(f"Connection error occurred: {conn_err}")

except requests.exceptions.Timeout as timeout_err:
    if timeout_err.response:
        print(f"Failed to load JSON data. Error status code:\n\
              {timeout_err.response.status_code}, Connection error: {timeout_err}")
    else: 
        print(f"Timeout occurred: {timeout_err}")

except requests.exceptions.RequestException as req_err:
    if req_err.response:
        print(f"Failed to load JSON data. Error status code:\n\
              {req_err.response.status_code}, Connection error: {req_err}")
    else: 
        print(f"An error occurred: {req_err}")

except ValueError as json_err:
    if json_err.response:
        print(f"Failed to load JSON data. Error status code:\n\
              {json_err.response.status_code}, Connection error: {json_err}")
    else: 
        print(f"JSON decode error: {json_err}")


# Check data type    
type(json_file)


# TIMES SERIES DATA
# Extract data
data = json_file['series']['docs']

# Convert to dataframe
df = pd.json_normalize(data, max_level=0) # 1000 observations

# Checks
df.info()
df.head()

###############################################################################

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
    
##############################################################################

# Explode arrays (1000 obs for each 55 period)
df_full = df.explode(column=['period','period_start_day','value'])

df_full.info()
df_full.head()

col_list = list(df_full.columns)

# Check if a column contains dictionaries, if yes then unpack
#if df['dimensions'].apply(lambda x: isinstance(x, dict)).all():
    
    

# REFERENCE DATA

ref_data = json_file['dataset']['dimensions_values_labels']

df_ref = pd.json_normalize(ref_data, max_level=0)

df_ref.info(verbose=True)
df_ref.head()

df_ref_exp = df_ref.explode(column=['exporter', 'importer'])


df_ref_exp.info(verbose=True)
df_ref_exp.head()












































































































































































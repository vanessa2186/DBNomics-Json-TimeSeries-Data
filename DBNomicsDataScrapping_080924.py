# -*- coding: utf-8 -*-
"""
Created on Sun Sep  8 16:37:10 2024

@author: vanessa coudert
"""

# Import libraries
import requests
import json
import pandas as pd

# Set pandas display options (Optional, for checks)
pd.set_option('display.max_columns', None)
pd.set_option('display.max_colwidth', 100)
pd.set_option('display.width', 200) 


# FUNCTIONS

def read_json_api(url, key1, skey1):

    '''Reads and parses json data from an API and returns the
       json data as a dataframe. It also checks for errors.'''
    
    # Read json file and handle errors
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



def get_columns_to_unpack(df, dtype):
    '''
    Gather a list of columns containing either lists or dictionaries, 
    based on the specified data type.
    '''
    columns_to_unpack = []
    
    # Check if dtype is either list or dict
    if dtype not in [list, dict]:
        raise ValueError("The argument 'dtype' must be either 'list' or 'dict'.")

    # Iterate over columns and check for the specified dtype
    for col in df.columns:
        if df[col].apply(lambda x: isinstance(x, dtype)).all():
            columns_to_unpack.append(col)
    
    return columns_to_unpack



def unpack_json_dicts_col(df):
    ''' 
    Unpacks columns containing dictionaries, up to one level. 
    '''
    df_to_unpack = df
    
    # List of columns to unpack
    cols_to_unpack = get_columns_to_unpack(df_to_unpack, dict)
      
    # Unpack Dicts   
    df_unpacked =  df_to_unpack.explode(column=cols_to_unpack)
    
    return df_unpacked





# SCRAP JSON DATA
#################

# URL to get data
url = "https://api.db.nomics.world/v22/series/CEPII/CHELEM-TRADE-CHEL?facets=1&format=json&limit=1000&observations=1"


# Reda Json data
df_data = read_json_api(url, 'series', 'docs')                                # Time Series Data
df_ref_data = read_json_api(url, 'dataset', 'dimensions_values_labels')       # Reference data


# UNAPCKING DICTIONARIES
########################

df_exp = unpack_json_dicts_col(df_data)                                      # Time Series Data
df_ref_exp = unpack_json_dicts_col(df_ref_data)                              # Refernce Data
print("Successfully unpacked dictionaries!")

# Checks TS
# df_exp.info(verbose=True, show_counts=True)
# df_exp.head()

# Check Ref data
# df_ref_exp.info(verbose=True, show_counts=True)
# df_ref_exp.head()


# UNPACKING NESTED LISTS
########################

# NOTE: This step is not dynamic but bespoke for each set of data. The
#       TS data needs to unpack adding corresponding rows while Reference 
#       data needs to unpack adding the corresponding columns.


# TS DATA

# Lists of cols to explode
list_to_unpack1 = get_columns_to_unpack(df_exp, list)
#print(list_to_unpack1)


df_exp2 =  df_exp.explode(column=list_to_unpack1)                             # Explode lists
df_ts = df_exp2.reset_index(drop=True)                                        # Reset index

# Checks
# df_ts.info(verbose=True, show_counts=True)
# df_ts.head()

print("Successfully Cleaned nested lists in Time Series!")

# REF DATA

# Explode the nested lists first
list_to_unpack2 = get_columns_to_unpack(df_ref_exp, list)
#print(list_to_unpack2)

# Iterate through each column that contains lists

# NOTE: The 'for' loop below can't be applied to the time series as it runs out
#       of memory (i.e. Unable to allocate 4.96 GiB for an array with shape 
#       (665500000, ) and data type float64). This step would need parellel
#       processing (i.e. multiprocessing module) if applied to the time series. 


for col in list_to_unpack2:   
    # Check if the column contains lists
    if df_ref_exp[col].apply(lambda x: isinstance(x, list)).any():       
        # Explode column. For lists of different lengths setting ignore_index=True
        df_ref_exp = df_ref_exp.explode(col, ignore_index=True)

# Checks
# df_ref_exp.info(verbose=True, show_counts=True)
# df_ref_exp.head()


# Split the exploded lists into separate columns
df_ref_exp[['secgroup_code', 'secgroup_description']] = pd.DataFrame(df_ref_exp['secgroup'].tolist(), index=df_ref_exp.index)
df_ref_exp[['product_code', 'product_description']] = pd.DataFrame(df_ref_exp['product'].tolist(), index=df_ref_exp.index)

# Drop the original columns if no longer needed
df_ref_exp = df_ref_exp.drop(columns=['product', 'secgroup'])

# Checks
# df_ref_exp.info(verbose=True, show_counts=True)
# df_ref_exp.head()

print("Successfully Cleaned nested lists in Reference Data!")



# DATASET INFO
##############

# Get request to API 
response = requests.get(url, timeout=120) 
     
# Parse JSON response
json_file = response.json()
data_info = json_file['dataset']['description']

# Write to text file
with open('Dataset_Info.txt', 'w') as file:
    file.write(data_info)

print("Successfully fetched and saved dataset info!")



# SAVE DATAFRAMES TO CSV/TEXT FILES
###################################

# Time series to csv
df_ts.to_csv('CHELEM_TRADE_CHEL.csv')
# Ref data to csv
df_ref_exp.to_csv('CHELEM_TRADE_CHEL_REFDATA.csv')


print("Successfully saved csv files!")














































































































































































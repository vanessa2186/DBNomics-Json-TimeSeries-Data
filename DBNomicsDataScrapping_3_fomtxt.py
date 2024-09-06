# -*- coding: utf-8 -*-
"""
Created on Thu Sep  5 22:01:30 2024

@author: vanessa coudert
"""


# Import libraries
import requests
import json
import pandas as pd

# Set pandas display options
pd.set_option('display.max_columns', None)
pd.set_option('display.max_colwidth', 25)
pd.set_option('display.width', 200) 

file_path = 'D:\\1st Year - MSc Data Science 2023-24\\Python Scripts\\dbnomics_json.json'

with open('dbnomics_json.json', 'r') as file:
    json_file = json.load(file)

# Check data type    
type(json_file)


# TIMES SERIES DATA
# Extract data
data = json_file['series']['docs']

# Convert to dataframe
df = pd.json_normalize(data, max_level=0) # 1000 observations

# Checks
df.info(verbose=True, show_counts=True)
df.head()

###############################################################################

## FUNCTION

def read_json_from_txt(path, key1, skey1):

    '''Reads and parses json data from an API and returns the
       json data as a dataframe. '''
    
    # Reda json file and handle errors
    with open(path, 'r') as file:
        json_file = json.load(file)
                        
    # Get data in key1 and sub-key1 
    json_dict = json_file[key1][skey1]
    
    # Convert to dataframe
    df = pd.json_normalize(json_dict, max_level=0)
    
    
    return df
    
##############################################################################

# Explode arrays (1000 obs for each 55 period)
df_full = df.explode(column=['period','period_start_day','value'])

df_full.info()
df_full.head()

col_list = list(df_full.columns)



# Check if a column contains dictionaries, if yes then unpack
df1 = pd.json_normalize(data, max_level=0)
# Checks
df1.info(verbose=True, show_counts=True)
df1.head()

# Unpack columns 
df_cols = list(df1.columns)
#cols_to_unpack = ['exporter', 'importer']


 #Wrap this 'if' inside for loop for each col to unpack:
cols_to_unpack = []

for col in df_cols:   
    if df1[col].apply(lambda x: isinstance(x, dict)).all():  # Check if a column contains dictionaries, if yes then unpack
        cols_to_unpack.append(col)
         
print(cols_to_unpack)   
  
    
df1_exp =  df1.explode(column=cols_to_unpack)

# Checks
df1_exp.info(verbose=True, show_counts=True)
df1_exp.head()


# Lists to unpack
list_to_unpack = []

for col in df_cols:   
    if df1_exp[col].apply(lambda x: isinstance(x, list)).all():  # Check if a column contains dictionaries, if yes then unpack
        list_to_unpack.append(col)
         
print(list_to_unpack)


df1_exp2 =  df1.explode(column=list_to_unpack)

# REFERENCE DATA

ref_data = json_file['dataset']['dimensions_values_labels']

df_ref = pd.json_normalize(ref_data, max_level=0)
#df_ref1 = pd.json_normalize(ref_data, max_level=2)

df_ref.info(verbose=True, show_counts=True)
df_ref.head()



df_ref_exp = df_ref.explode(column=['exporter', 'importer'])

df_ref_exp.info(verbose=True, show_counts=True)
df_ref_exp.head()


# DATASET INFO

data_info = json_file['dataset']['description']

#################### CLEANING COLUMNS (FUNCTION) Im still working on this...
df_ref2 = pd.json_normalize(ref_data, max_level=0)

# Unpack columns 
df_cols = list(df_ref2.columns)
#cols_to_unpack = ['exporter', 'importer']



#Wrap this 'if' inside for loop for each col to unpack:
# for col in df_cols:   
#     if df_ref2[col].apply(lambda x: isinstance(x, dict)).all():  # Check if a column contains dictionaries, if yes then unpack:
#         df_ref2[f"{col}_expl"] = df_ref2.explode(column=[col])


#Wrap this 'if' inside for loop for each col to unpack:
cols_to_unpack = []
for col in df_cols:   
    if df_ref2[col].apply(lambda x: isinstance(x, dict)).all():  # Check if a column contains dictionaries, if yes then unpack
        cols_to_unpack.append(col)
        
print(cols_to_unpack)

df_ref_exp3 = df_ref2.explode(column=cols_to_unpack)


def explode_json_df_cols(df):
    
    '''Explodes a column that contains a json dictionary up to 1
       level. The resulting dataframe may containn nested lists'''
    
    # get columnslist
    df_cols = list(df.columns)
    
    # Get columns to unpack:
    cols_to_unpack = []
    
    # Check if a column contains dictionaries, if yes then unpack
    for col in df_cols:   
        if df[col].apply(lambda x: isinstance(x, dict)).all():       # Check if a column contains dictionaries, if yes then unpack
            cols_to_unpack.append(col)
    
    # Explode columns
    df_exp = df.explode(column=cols_to_unpack)
    
    # Lists to unpack
    list_to_unpack = []

    for col in df_cols:   
        if df_exp[col].apply(lambda x: isinstance(x, list)).all():  # Check if a column contains lists, if yes then unpack
            list_to_unpack.append(col)

    df_exp2 =  df_exp.explode(column=list_to_unpack)
    
    
    return df_exp2
    


##############################################################################

# Reda Json data
df_data = read_json_from_txt(file_path, 'series', 'docs')                                # Time Series
df_ref_data = read_json_from_txt(file_path, 'dataset', 'dimensions_values_labels')       # Reference data
data_info = read_json_from_txt(file_path, 'dataset', 'description')                      # Dataset Info


# Sort out columns with dictionaries (explode json columns)
df_exp = explode_json_df_cols(df_data)
df_ref_exp = explode_json_df_cols(df_ref)


# SAVE DATAFRAMES TO CSV/TEXT FILES

# Time series to csv
df_exp.to_csv('CHELEM_TRADE_CHEL.csv')

# Ref data to csv
df_ref_exp.to_csv('CHELEM_TRADE_CHEL_REFDATA.csv')

# Dataset Info to text
with open('Dataset_Info.txt', 'w') as file:
    file.write(data_info)




















































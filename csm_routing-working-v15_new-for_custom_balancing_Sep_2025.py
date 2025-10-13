#!/usr/bin/env python
# coding: utf-8

# In[1]:



# CSM Optimization 

# change log

# Aug 1st 2024 : Added initial optimization logic by developing the function based on constraints

# Aug 7th 2024: Added rebalacing logic based on additional feedback from Brett

# Aug 15 2024:  Added churn risk logic





# In[2]:


#pip install pulp


# In[3]:


from IPython.core.display import display, HTML

# Use CSS to set the width of the cells to 100%
display(HTML("<style>.container { width:80% !important; }</style>"))


# ### imports

# In[4]:


import pandas as pd
import numpy as np
import pulp
import time
import itertools
import random
import datetime

pd.set_option('display.max_rows', 500)


pd.options.display.max_columns = 500


# In[5]:


import json
# Function to load properties from a JSON file
def load_properties(filepath):
    with open(filepath) as file:
        props = json.load(file)
    return props



props = load_properties('properties.json')


# In[6]:



import psycopg2

from sqlalchemy import create_engine, types
from sqlalchemy.engine import create_engine
from urllib.parse import quote  

from snowflake.connector.pandas_tools import pd_writer
from snowflake.sqlalchemy import URL
import os
import snowflake.connector
import io
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.serialization import load_pem_private_key


def private_key_deserializer(private_key_str):
    key_file = io.StringIO(private_key_str)

    private_key = serialization.load_pem_private_key(
        key_file.read().encode(), 
        password=None,
    )
    private_key_pem = private_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    )
    return private_key_pem


def create_connection():
    """
    Creates the connection to Database. 

    """

    SNOWFLAKE_USER = props["SNOWFLAKE_USER"]
    SNOWFLAKE_PRIVATE_KEY = props["SNOWFLAKE_PRIVATE_KEY"]
    SNOWFLAKE_PRIVATE_KEY = private_key_deserializer(SNOWFLAKE_PRIVATE_KEY.replace('\\n', '\n'))
    snowflake_database = props['snowflake_database']
    snowflake_schema = props['snowflake_schema']
    snowflake_warehouse = props['snowflake_warehouse']
    snowflake_role = props['snowflake_role']


    try:
        print('Connect to snowflake to read initial data')

        ctx = snowflake.connector.connect(
          user=SNOWFLAKE_USER,
          private_key=SNOWFLAKE_PRIVATE_KEY,
          account=props["snowflake_account_prod"],
          warehouse=props["snowflake_warehouse"],
          database=props["snowflake_database"],
          schema=props["snowflake_ds_schema"],
            role=props["snowflake_role"],
            autocommit=True)

        return ctx
    except Exception as e:
        print("Failed to establish a connection with following config:")
        print(e, stack_info=True)
        raise


# In[7]:


def get_data(query):
    ctx = create_connection()


    cur = ctx.cursor()

    # Execute a statement that will generate a result set.
    
    cur.execute(query)

    # Fetch the result set from the cursor and deliver it as the Pandas DataFrame.
    df = cur.fetch_pandas_all()

    cur.close()
    return df


# In[ ]:






# ### Load the data and preprocessing

# In[8]:


#csv_file = 'Account_data_routing.csv'

# this file is provided by Brett (in google sheets, i have downloaded as .csv file, make sure you have all filters cleared before downloading the file. )

# https://docs.google.com/spreadsheets/d/1Y-mbOOrKWOztwCsf4u8cfU9TAnlTp0HOOr6N2NY6zt0/edit?gid=0#gid=0
# https://docs.google.com/spreadsheets/d/1V3U_XXX66caAQI93mLQKvY-41Td51IiyJrcwdbjuu5k/edit?gid=1846871448#gid=1846871448

#csv_file = 'Account Data with Courtney Follow Ups - Account Data with Courtney Follow Ups.csv'


# https://docs.google.com/spreadsheets/d/1KdewApzyds2Fk0H5iJ-q5yyfo3VYhDwKRjHV5v3d3kU/edit?gid=331582002#gid=331582002
#csv_file = 'Account Data and Neediness Scores 100124- Full Account Data - Sheet1 (1).csv'

#https://docs.google.com/spreadsheets/d/1_8kUwpo06FYzEXsY3GyUEnJ4nfPlAu2E9wJ7cb33d7M/edit?gid=83088624#gid=83088624
#csv_file = 'Account Data and Neediness Scores 100124 v2 - Full Account Data - Sheet1.csv'


#https://docs.google.com/spreadsheets/d/1gkr_bgBQ8K4BEN0p4pFaMsp-4oKhXI1o/edit?gid=1401174057#gid=1401174057
#csv_file = 'Account Data and Neediness Scores 110424.xlsx - Full Account Data.csv'


#csv_file = '_[confidential] Resi_Corp_Nov_5th_Routing_with_BoB - Output (1).csv' 

csv_file = 'CSM Reassignment Resi Corp 9_19_25 - Sheet1.csv'

# load the data into a daatframe
df = pd.read_csv(csv_file)




# accounts to keep
# sheets: https://docs.google.com/spreadsheets/d/1dHEPsKoGoUv5Iksl9RV5qBY5EAVwJ5l1-ot9g4e6wKY/edit?gid=0#gid=0

accounts_to_keep_csv_file = 'Accounts to Keep - Sheet1.csv'
accounts_to_keep = pd.read_csv(accounts_to_keep_csv_file)
accounts_to_keep = accounts_to_keep.dropna(subset = ['Account ID'])
accounts_to_keep = accounts_to_keep.rename({'Account ID':'account_id'}, axis=1)
accounts_to_keep = accounts_to_keep[['account_id']]
accounts_to_keep['account_to_keep'] = True

accounts_to_keep = accounts_to_keep.drop_duplicates(subset=['account_id'], keep='last')
print('accounts_to_keep shape ', accounts_to_keep.shape)

# https://docs.google.com/spreadsheets/d/1cP7TblwYw-t0mDxH2ijI8m-aBUXiDVMiD0ceBhTeLe8/edit?gid=0#gid=0
resi_accounts_to_keep = pd.read_csv('CS Corp Accounts - DO NOT MOVE - Sheet1.csv')

resi_accounts_to_keep = resi_accounts_to_keep.dropna(subset = ['18-digit ID'])
resi_accounts_to_keep = resi_accounts_to_keep.rename({'18-digit ID':'account_id'}, axis=1)
resi_accounts_to_keep = resi_accounts_to_keep[['account_id']]
resi_accounts_to_keep['account_to_keep'] = True
resi_accounts_to_keep = resi_accounts_to_keep.drop_duplicates(subset=['account_id'], keep='last')
print('resi_accounts_to_keep shape ', resi_accounts_to_keep.shape)

accounts_to_keep = pd.concat([accounts_to_keep, resi_accounts_to_keep], ignore_index=True)



accounts_to_keep = accounts_to_keep.drop_duplicates(subset=['account_id'], keep='last')


# In[ ]:





# In[ ]:





# In[9]:


csv_bob_mapping = 'Resi_CSM_BoB_Type_mapping_v0 - Sheet1.csv'

csv_bob_mapping_df = pd.read_csv(csv_bob_mapping)

mt_count_lt_5_csms = list(csv_bob_mapping_df[csv_bob_mapping_df['BoB Type'] == '0-4 MT']['CSM NAME'])

mt_count_gte_5_csms = list(csv_bob_mapping_df[csv_bob_mapping_df['BoB Type'] == '5+ MT']['CSM NAME'])


# In[ ]:





# In[ ]:





# In[10]:


# Load the parent id mapping data
# this is fetched from a query and downloaded as csv file for now, we can improve the logic to fetch directly from snowflake
#parent_child_account_mapping = pd.read_csv('parent_id_account_mapping.csv')

query = ''' select parent_id, id  from DSV_WAREHOUSE.PUBLIC.VW_SALESFORCE_ACCOUNT where parent_id is not null'''
parent_child_account_mapping = get_data(query)


# Create a dictionary where the keys are parent IDs and the values are lists of child IDs
parent_child_accounts_mapping = parent_child_account_mapping.groupby('PARENT_ID')['ID'].apply(list).to_dict()

# Remove parent IDs from their own child lists
parent_child_accounts_mapping = {k: v for k, v in parent_child_accounts_mapping.items() if v and k not in v}

# Remove parent IDs from the child lists of other rows
for parent_id in parent_child_accounts_mapping.keys():
    for child_list in parent_child_accounts_mapping.values():
        if parent_id in child_list:
            child_list.remove(parent_id)
            
parent_child_accounts_mapping = {k: v for k, v in parent_child_accounts_mapping.items() if v}




# In[11]:


def get_timezone(time_zone_c):
    if time_zone_c.startswith('Mountain'):
        return 'MST'
    elif time_zone_c.startswith('Eastern'):
        return 'EST'
    elif time_zone_c.startswith('Atlantic'):
        return 'EST'
    elif time_zone_c.startswith('Central'):
        return 'CST'
    elif time_zone_c.startswith('Pacific'):
        return 'PST'
    elif time_zone_c.startswith('Hawai'):
        return 'PST'
    elif time_zone_c.startswith('Alaska'):
        return 'PST'
    else:
        return 'EST'


# In[12]:


run_for_Segment  = 'Residential'
#run_for_Segment = 'Commercial & Construction'


run_for_account_level = 'Corporate'

#run_for_account_level = 'Enterprise'

num_csms_to_add = 0  # replace with the number of CSMs you want to add


def load_data():

    ''' Load the data from the csv file and perform some data cleaning and preprocessing'''
    df = pd.read_csv(csv_file)

    
    
    # Rename the columns
#     df = df.rename(columns={#'Account ID':'account_id',
#                             'Responsible CSM':'csm_name',
#                               'Neediness Score':'neediness_score', 
#                             'Health Score':'health_score', 
#                             'Total Related Tenants':'propensity_score', 
#                             'Total Products LOE':'revenue',
#                             'TAD Score': 'tad_score',
#                             'MTs+MIs' : 'tech_count', 
#                             #'Responsible CSM': 'Responsible CSM Old',      
#                   })
    
    df = df.rename(columns={'ACCOUNT_ID':'account_id',
                            'RESPONSIBLE_CSM_NAME':'csm_name',
                           # 'CORE_HEALTH_SCORE':'health_score', 
                            'TOTAL_MRR':'revenue',
                          #  'TITAN_ADVISOR_SCORE': 'tad_score',
                           # 'ACTIVE_MANAGED_TECH_COUNT' : 'tech_count',
                          #  'CUSTOMER_PRIMARY_INDUSTRY':'Industry',
                           # 'CORE_HEALTH_SCORE_COLOR':'Health Segment',
                         #   'CORE_HEALTH_SCORE':'health_score',
                         #   'Any churn risk': 'Churn Risk Status'
                            
                            #'Responsible CSM': 'Responsible CSM Old',      
                  })
    
    
    df = df[~df['csm_name'].isna()]
    
    print(df.columns)
    
    
    df = df.drop_duplicates(subset=['account_id'], keep='last')
    print('1 df.shape ', df.shape)
    
#     if 'tech_count' not in list(df.columns):
#         print('Tech count not in data')
#         mt_mi_counts_df = pd.read_csv('Account Data with Courtney Follow Ups - Account Data with Courtney Follow Ups.csv')[['Account ID', 'MTs+MIs']]
#         mt_mi_counts_df.columns = ['account_id', 'tech_count']
#         mt_mi_counts_df = mt_mi_counts_df.drop_duplicates(subset=['account_id'], keep='last')
        
#         df = pd.merge(df, mt_mi_counts_df, on='account_id', how='left')
       
    
    
    if run_for_Segment == '':
        csms_to_keep = [
                "Alla Poghosyan",
            "Warren Rogers",
            "Kieran Cockburn",
            "Andrew Guth",
            "Krister Karlsson",
            "Anna Hayrapetyan",
            "Davit Mkrtchyan",
            "Lesman Santrosyan",
            "Hovhannes Khachatryan",
            "Arman Danielyan",
            "Andre Tossunyan",
            "Jaque Reid",
            "Nicole Moore",
            "Maddie Millis",
            "Gohar Grigoryan",
            "Meg Cipriano",
            "David Murrow",
            "Hrag Jinbashian",
            "Elen Badalyan",
            "Edmon Brutyan",
            "Alex Janssens", 
            'new_csm_11',
             'new_csm_12',
             'new_csm_13',
             'new_csm_14',
             'new_csm_15',
            
            'Andrew Guth',
'Troy Jones'
            

                        ]
        print(df.shape)
        df = df[ df['csm_name'].isin(csms_to_keep) ]
        print('After filtering ', df.shape)
        
        
        
     
    #skipped for this custom run
    if run_for_Segment == '': #'Residential':
        
        # List of names to be replaced
        names_to_replace = ['Kirstie Porter', 'Ned Durrett', 'Sabrina Chacon', 'Lesmon Santrosyan', 'Meg Gulbin', 'Ryan Sterritt', 
                            "La'Daysha Johnson", 'Jeff Braxton']

        # Replace the names in the 'new_csm_name' column
        df['csm_name'] = df['csm_name'].replace(names_to_replace, 'unassigned')
        
        # remove this data  : somehow we got these CSMs data
        remove_csms_list = ['Lesman Santrosyan', 'Meg Cipriano', 'Cain Mitchell', 'Kei Washington', 'Kieran Cockburn' ]
        df = df[ ~ df['csm_name'].isin(remove_csms_list) ]
        
        df = df[ ~ df['new_csm_name'].isin(remove_csms_list) ]
        
        
#         df = df[df['csm_name'].isin(['Krister Karlsson',
#                                      'Andrew Guth',
#                                      'Vahagn Yaralian',
#                                      'Elen Badalyan',
#                                      'Gohar Grigoryan',
#                                      'Aleksandr Hakobyan',
#                                      'Arman Danielyan',
#                                      'Hrag Jinbashian',
#                                      'Armen Mardirossian',
#                                      'Elen Ghumashyan',
#                                      'Edmon Brutyan',
#                                      'Anna Hayrapetyan',
#                                      'Hovhannes Khachatryan',
#                                      'Gor Shahbazyan',
#                                      'Zarian Shuman',
#                                      'Kevin Martin',
#                                      'Adrienne Olszewski',
#                                      'Bernie Kosloski',
#                                      'Syd Croft',
#                                      'Alla Poghosyan',
#                                      'Davit Mkrtchyan',
#                                      'Maddie Millis',
#                                      'Daniel De Leon',
#                                      'Alex Forston',
#                                      'Alexa Castro',
#                                      'Alan Wong',
#                                      'Serro Park',
#                                      'Kyle Shinmoto', 
#                                      'unassigned'])]
        
    # remove manager/lead assignments
    # List of names to be replaced
    names_to_replace = ['Chantel Anderson', 'Courtney Askew']

    # Replace the names in the 'new_csm_name' column
    df['csm_name'] = df['csm_name'].replace(names_to_replace, 'unassigned')

    print('2 df.shape ', df.shape)
    
    df = df.drop_duplicates(subset=['account_id'], keep='last')
    
    
    
    df['Account Level'] = 'Corporate'
    df['Segment'] = 'Residential'
    df['Churn Risk Status'] = 'Not at risk'
    df['Market Category'] = 'Residential'
    
    
    # Remove the rows where the 'Account Level' column is 'Strategic'
    df = df[df['Account Level'] != 'Strategic']

    # Keep the rows where the 'Segment' column is  'Residential' and the 'Account Level' column is 'Corporate'
    df = df[(df['Segment'] == run_for_Segment) & (df['Account Level'] == run_for_account_level)]
    
    
    neediness_df = pd.read_csv('pod_model_v4 (14).csv')
    print('neediness_df columns ', neediness_df.columns)
    
    neediness_df = neediness_df[['ACCOUNT_ID', 'Neediness Score', 'Neediness Category', 'TAD Score', 'INDUSTRY_NEW', 'MTs+MIs', 'Health Score', 'Health Segment']]
    
   
    neediness_df.columns = ['account_id', 'neediness_score', 
                           'neediness_category', 'tad_score', 'Industry', 'tech_count', 'health_score', 'Health Segment']
    
    
    df = pd.merge(df, neediness_df, on='account_id', how='left')
    
    # fill the missing values with 0
    df['neediness_score'] = df['neediness_score'].fillna(0)
    
    
    
    
#     # Create a new column 'neediness_category' based on the 'neediness_score' column
#     df['neediness_category'] = pd.cut(df['neediness_score'],
#                                   bins=[0, 4, 7, float('inf')],
#                                   labels=['Low', 'Medium', 'High'],
#                                   include_lowest=True)
    
    # Convert the 'neediness_category' column to a string
    df['neediness_category'] = df['neediness_category'].fillna('Low')
    df['neediness_category'] = df['neediness_category'].astype(str)
    
    # only keep the columns we need
    df = df[['account_id', 'csm_name', 'neediness_score', 'neediness_category', 'tad_score', 'health_score', 'revenue', 'Account Level', 'Segment', 'Health Segment', 'Churn Risk Status', 'Market Category', 'Industry', 'tech_count']]

    # Remove the rows where the 'account_id' column is missing
    df = df[~df['account_id'].isna()]
    
    # Fill the missing values in the 'csm_name' column with 'unassigned'
    df['csm_name']  = df['csm_name'].fillna('unassigned')
    
    unassigined_indices = df.index[df['csm_name'] == 'unassigned'].tolist()
    valid_csms = df['csm_name'].unique().tolist()
    
    if 'unassigned' in valid_csms:
        valid_csms.remove('unassigned')
    #print('valid_csms ', valid_csms)
    
    for i in unassigined_indices:
        df.at[i, 'csm_name'] = random.choice(valid_csms)
    
    
    
    # Remove the rows where the 'revenue' column is missing
    #df = df[~df['revenue'].isna()]
    df['revenue'] = df['revenue'].fillna(0)
    df = df.fillna(0)
    
    # Create a dictionary where the keys are child IDs and the values are parent IDs
    child_parent_mapping = {child_id: parent_id for parent_id, child_ids in parent_child_accounts_mapping.items() for child_id in child_ids}


    df['parent_account_id'] = df['account_id'].map(child_parent_mapping)
    
    print('4 df.shape ', df.shape)
    
    # get Triage cases which are open and having a predicted churn date
    query = '''select distinct account_id from DSV_WAREHOUSE.PUBlIC.VW_SALESFORCE_CASE 
                    where closed_date is null
                    and PREDICTED_CHURN_DATE_C is not null
                    and initial_case_record_type_c = 'ST Internal - Triage Team' 
                    
                    and is_deleted = false
                    and termination_type_c in('Early Release Churn'
                           , 'Non-Renewal Churn'
                           ,'Downgrade at Renewal'
                           ,'Off-Cycle Downgrade')
                    -- and product_type_c ='Managed Tech'
                    and is_closed = false
                    and predicted_churn_date_c is not null
                    --and record_type_id = '0121P000000F42YQAS'
                    
                    '''
    accounts_with_open_cases_df = get_data(query)
    accounts_with_open_cases_df.columns = ['account_id']
    accounts_with_open_cases_df['open_case'] = 'yes'
    
    df = pd.merge(df, accounts_with_open_cases_df, on='account_id', how='left')
    
    
    print('5 df.shape ', df.shape)
    
    
    # customer tenure and tad threshold category
    
    
    query = '''select account_id, success_date,  datediff(day, success_date, current_date ) tenure
                from
                dsv_warehouse.public.agg_bireport_squad_tenant
                where success_date is not null
                '''

    cust_tenure_tad_df = get_data(query)

    cust_tenure_tad_df.columns = ['account_id', 'success_date', 'tenure']
    #cust_tenure_tad_df = cust_tenure_tad_df[cust_tenure_tad_df['tenure'] > 0]
    
    cust_tenure_tad_df = cust_tenure_tad_df.drop_duplicates(subset=['account_id'], keep='last')
    
    df = pd.merge(df, cust_tenure_tad_df, on='account_id', how='left')
    
    print('6 df.shape ', df.shape)
    query = '''select distinct account_id , True contacted_last_90_days from DSV_WAREHOUSE.PUBLIC.VW_GAINSIGHT_CSM_ACTIVITY 
            where activity_type_new  = 'Call'
            and activity_date > current_date - interval '90 day'
            and is_successful_call = True
            
            '''
    
    
    contacted_last_90_days_df = get_data(query)

    contacted_last_90_days_df.columns = ['account_id', 'contacted_last_90_days']
    
    contacted_last_90_days_df = contacted_last_90_days_df.drop_duplicates(subset=['account_id'], keep='last')
    
    df = pd.merge(df, contacted_last_90_days_df, on='account_id', how='left')
    print('7 df.shape ', df.shape)
    
    # Timezone Query:
    query = '''
    select distinct id account_id, 
    case 
        when time_zone_c like 'Mountain%' then 'MST'
        when time_zone_c like 'Eastern%' then 'EST'
        when time_zone_c like 'Central%' then 'CST'
        when time_zone_c like 'Pacific%' then 'PST'
        else 'EST'
    end timezone_snowflake

    from DSV_WAREHOUSE.PUBLIC.VW_SALESFORCE_ACCOUNT 
    where is_deleted = False
    -- and billing_country = 'United States'
    and time_zone_c is not null
    and customer_status_picklist_c in ('Live', 'Success')
    ;

    '''
    timezone_df = get_data(query)
    timezone_df.columns = ['account_id', 'timezone_snowflake']
    timezone_df = timezone_df.drop_duplicates(subset=['account_id'], keep='last')
    
    df = pd.merge(df, timezone_df, on='account_id', how='left')
       
    print('8 df.shape ', df.shape)
    # CSM Change query:

    query =  '''
        WITH csm_changes AS (
          SELECT account_id, 
                 calendar_date, 
                 preferred_csm_name,
                 LAG(preferred_csm_name) OVER (PARTITION BY account_id ORDER BY calendar_date) as previous_csm
          FROM DSV_WAREHOUSE.POST_SALES.VW_CUSTOMER_HISTORY_DAILY
          WHERE calendar_date > current_date - interval '180 day'
          and preferred_csm_role = 'Success Rep'
          AND preferred_csm_name IS NOT NULL
        )
        SELECT *
        FROM csm_changes
        WHERE preferred_csm_name != previous_csm;
        '''
    
    csm_change_df = get_data(query)
    csm_change_df['csm_changed_last_6mnths'] = 'yes'
    csm_change_df.columns = ['account_id', 'csm_changed_calendar_date', 'preferred_csm_name', 'previous_csm', 'csm_changed_last_6mnths']
    csm_change_df = csm_change_df.drop_duplicates(subset=['account_id'], keep='last')
    
    df = pd.merge(df, csm_change_df[['account_id', 'csm_changed_calendar_date', 'previous_csm', 'csm_changed_last_6mnths']], on='account_id', how='left')
    print('8 df.shape ', df.shape)
    
    # remove data access accounts
    query =  '''
        select id from DSV_WAREHOUSE.PUBLIC.VW_SALESFORCE_ACCOUNT 

        where SUB_DISPOSITION_C= 'Data Access Only';
        '''
    
    
    dataaccess_accounts_df = get_data(query)
    dataaccess_accounts_df.columns = ['account_id']
    dataaccess_accounts_df['data_access_account'] = 'yes'
    
    dataaccess_accounts_df = dataaccess_accounts_df.drop_duplicates(subset=['account_id'], keep='last')
    df = pd.merge(df, dataaccess_accounts_df, on='account_id', how='left')
    
    print('dataaccess_accounts_df counts ', df['data_access_account'].value_counts())
    
    #df = df[df['data_access_account'] != 'yes']
    
    print('9 df.shape ', df.shape)
    # new category for customer tenure and tad
    if 'Residential' in df['Segment'].values:
        
        df['cust_tenure_tad_category'] = df[df['Segment'] == 'Residential'].apply(
                        lambda row: 
                        'Customer under 90 days from Success Transition Date - under 100 TAD' if row['tenure'] < 90 and row['tad_score'] < 100 else 
                        ('Customer under 90 days from Success Transition Date - 100 to 124 TAD' if row['tenure'] < 90 and row['tad_score'] >= 100 else 
                        ('Customer over 90 days - under 125 TAD' if row['tenure'] >= 90 and row['tad_score'] < 125 else 
                        ('Customer over 90 days - 125 TAD and above' if row['tenure'] >= 90 and row['tad_score'] >= 125 else 'Other'))), 
                        axis=1)
    
    if 'Commercial & Construction' in df['Segment'].values:
        df['cust_tenure_tad_category'] = df[df['Segment'] == 'Commercial & Construction'].apply(
                        lambda row: 
                        'Customer under 365 days from Success Transition Date - under 100 TAD' if row['tenure'] < 365 and row['tad_score'] < 100 else 
                        ('Customer under 365 days from Success Transition Date - 100 to 124 TAD' if row['tenure'] < 365 and row['tad_score'] >= 100 else 
                        ('Customer over 365 days - under 125 TAD' if row['tenure'] >= 365 and row['tad_score'] < 125 else 
                        ('Customer over 365 days - 125 TAD and above' if row['tenure'] >= 365 and row['tad_score'] >= 125 else 'Other'))), 
                        axis=1)
        

        print(df['cust_tenure_tad_category'].value_counts())
        
    
    print('10 df.shape ', df.shape)
    
    
    # Roofing constraing for Ryan Sterritt :  Ryan Sterritt shouldn’t only have roofing accounts, but within his portfolio, he should have all of the roofing accounts
    
    #     If primary industry = roofing 
    # Residential CSM: Ryan Sterritt
    # Comm+ CSM: Sierra Fitzgerald
    
#     if 'Ryan Sterritt' in df['csm_name'].values:
#         df.loc[df['Industry'] == 'Roofing', 'csm_name'] = 'Ryan Sterritt'
        
#     if 'Sierra Fitzgerald' in df['csm_name'].values:
#         df.loc[df['Industry'] == 'Roofing', 'csm_name'] = 'Sierra Fitzgerald'
    
    
    
    df = pd.merge(df, accounts_to_keep[['account_id', 'account_to_keep']], on='account_id', how='left')
    
    print('11 df.shape ', df.shape)
    print(df['tech_count'].isna().value_counts())
    df['tech_count'] = df['tech_count'].fillna(10)
    df['tech_count'] = df['tech_count'].astype('int')
    
    df['industry_size'] = np.where(df['tech_count'] >= 5, '5+ MT', '0-4 MT')
    
    
    
    
    df= df.reset_index(drop=True)
    print('df length ', len(df))

    print('df shape ', df.shape)
    
    df = df.reset_index(drop=True)
    print(df['csm_name'].nunique())
    print(df['csm_name'].unique())
    
    return df

df = load_data()


# In[13]:


df['neediness_category'].value_counts()


# In[14]:


cols = ['account_id', 'Ultimate Parent Name', 'Parent Account Name',
       'Account Name', 'Account ID.1', 'Tenant Name', 'Tenant ID',
       'Team Account Level', 'Success Rep', 'Preferred CSM Name Old',
       'csm_name', 'Manager', 'Segment', 'Account Level', 'neediness_score',
       'Neediness category', 'Transition to Success Date', 'Months in Success',
       'Customer Status', 'Market Category',
       'Data Science Generated Market Category', 'DS Market Segment',
       'Parent Level Market Category', 'Business Focus', 'NEW DS MKT CAT',
       'NEW DS BIZ FOCUS', 'NEW DS Segment', 'NEW DS Sub Segment',
       'NEW PARENT MKT CAT', 'NEW PARENT BIZ FOCUS', 'NEW PARENT SEGMENT',
       'NEW PARENT SUB SEGMENT', 'NEW ADJ Segment', 'NEW ADJ ACCOUNT LEVEL',
       'Segment Change Classification', 'Account Level Change Classification',
       'Industry', 'Industry Classification', 'propensity_score',
       'Next Soonest Renewal Date', 'Ultimate Parent ARR (core+pro)',
       'tech_count', 'Potential Data Only', 'Total Calls Last 30 Days <5 Min',
       'Total Calls last 30 days >5 min', 'Had Call <5 Min', 'Had Call >5 min',
       'MT Grouping', 'tad_score', 'TAD Threshold', 'Customer dates',
       'Above TAD Threshold?', 'Churn Risk Status', 'health_score',
       'Health Segment', 'Product Penetration', 'revenue', 'Marketing Pro LOE',
       'Pricebook Pro LOE', 'Phones Pro LOE', 'Dispatch Pro LOE',
       'Fleet Pro LOE', 'Scheduling Pro LOE', 'Live Services LOE',
       'Total Support Cases last 120 days', 'Total Triage Cases last 120 days',
       'Total Emails last 120 days', 'Emails per week', 'Email Freq',
       'Total Calls last 120 days', 'Calls per week', 'Call Freq',
       'account_id', 'Preferred CSM Name', 'csm_changed_calendar_date',
       'previous_csm', 'csm_changed_last_6mnths', 'Above TAD?', 'BoB Type',
       'New CSM?', 'Initial Roster by Type', 'New hire account acquisition?']


duplicates = [item for item in set(cols) if cols.count(item) > 1]
duplicates


# In[15]:


df[df['csm_name'] == 'Gor Shahbazyan']['industry_size'].value_counts().sort_index()


# In[16]:


df['industry_size'].value_counts()


# In[17]:


df['account_to_keep'].value_counts()


# In[ ]:





# In[18]:


df.groupby(['industry_size', 'Health Segment']).size()
# 12 CSMs in 0-4 MT
# 27 CSMs in 5+


# In[19]:


df['csm_name'].value_counts()


# In[20]:


df['csm_name'].value_counts().sum()


# In[ ]:





# In[ ]:





# In[ ]:





# In[21]:


df['cust_tenure_tad_category'].value_counts()


# In[22]:


df['csm_changed_last_6mnths'].value_counts()


# In[23]:


df.groupby(['Segment', 'Account Level'])['csm_name'].nunique().reset_index(name='Count').sort_values('Segment', ascending=False)


# In[24]:


csm_timezone_mapping = {}
# Handle timezone:
if list(df['Segment'].unique())[0] == 'Commercial & Construction' :
    # Penalty for violating timezone constraints
    csm_timezone_mapping = {

        'Sam Rains':['CST', 'EST'],
        'Sierra Fitzgerald':['CST', 'EST'],
        'Susanna Grigoryan' : ['EST'],
        'Devonte Durr' : ['EST'],
        'Emilya Nazinyan' : ['EST'],
        'Marieta Danielyan' : ['EST'],
        'Levon Ghazaryan' : ['EST'],
        'Kima Galstyan' : ['EST'],
        'Lidia Zhamharyan' : ['EST'],
        'Alisa Petrosyan' : ['EST'],
        'Maria Atayan' : ['EST'],
        'Trey Simpson ' : ['EST'],
        'Grigory Baghdasaryan' : ['EST'],
        'Audrey Sapoznik' : ['EST'],
        'Brandon Davis' : ['EST'],
        'Jai Moore' : ['EST'],
        'Arman Terteryan' : ['EST'],
        'Syuzanna Markosyan' : ['EST'],
        'Elizabeth Sargis' : ['EST'],
        'Katharine Castillo' : ['EST'],
        'Razmik Khachatryan' : ['EST'],
        'Stormie Schaible' : ['EST'],
        'Charlie Wyrick' : ['PST', 'MST', 'CST'],
        'Juliana Gordon' : ['PST', 'MST', 'CST'],
        'Matthew Kang' : ['PST', 'MST'],
        'Cody Wagner' : ['PST', 'MST'],
        'Brendan Robb' : ['PST', 'MST'], 


    }


if list(df['Segment'].unique())[0] == 'Residential' :
    csm_timezone_mapping = {

        'Krister Karlsson': ['CST', 'EST'],
        'Andrew Guth': ['CST','EST'],
        'Vahagn Yaralian': ['EST'],
        'Elen Badalyan': ['EST'],
        'Gohar Grigoryan': ['EST'],
        'Aleksandr Hakobyan': ['EST'],
        'Arman Danielyan': ['EST'],
        'Hrag Jinbashian': ['EST'],
        'Armen Mardirossian': ['EST'],
        'Elen Ghumashyan': ['EST'],
        'Edmon Brutyan': ['EST'],
        'Anna Hayrapetyan': ['EST'],
        'Hovhannes Khachatryan': ['EST'],
        'Gor Shahbazyan': ['CST','EST'],
        "La'Daysha Johnson": ['CST','EST'],
        'Zarian Shuman': ['CST','EST'],
        'Kevin Martin': ['CST','EST'],
        'Ryan Sterritt': ['CST','EST'],
        'Adrienne Olszewski': ['CST','EST'],
        'Bernie Kosloski': ['CST','EST'],
        'Syd Croft': ['CST','EST'],
        'Alla Poghosyan': ['EST'],
        'Davit Mkrtchyan': ['EST'],
        'Maddie Millis': ['PST', 'MST'],
        'Daniel De Leon': ['PST', 'MST'],
        'Alex Forston': ['MST'],
        'Alexa Castro': ['MST'],
        'Alan Wong': ['PST', 'MST'],
        'Serro Park': ['PST', 'MST'],
        'Kyle Shinmoto': ['PST', 'MST'],


    }
    
   


# ### some data validation checks

# In[25]:


df['health_score'].isna().value_counts()


# In[26]:


df['revenue'].isna().value_counts()


# In[27]:


df


# In[28]:


df['industry_size'].value_counts()


# In[29]:


df['Account Level'].value_counts()


# In[30]:


df['Segment'].value_counts()


# In[31]:


df['csm_name'].nunique()


# In[32]:


df['csm_name'].value_counts()


# In[ ]:





# In[33]:


df[df['csm_name'] == 'Gohar Grigoryan']['csm_changed_last_6mnths'].value_counts()


# In[34]:


sum(df['csm_name'].value_counts().values)


# In[35]:


df['csm_name'].value_counts().mean()


# In[36]:


datetime.datetime.now()


# In[37]:


df


# In[38]:


# import pandas as pd
# import random

# def redistribute_accounts(df):
#     # Make a copy to avoid modifying original
#     df_copy = df.copy()
    
#     # Define new CSMs and target CSMs for removal
#     new_csms = ['new_csm_11', 'new_csm_12', 'new_csm_13', 'new_csm_14', 'new_csm_15', 'new_csm_16']
    
#     csms_to_reduce = [
#         'Alla Poghosyan', 'Warren Rogers', 'Kieran Cockburn', 'Andrew Guth', 
#         'Krister Karlsson', 'Anna Hayrapetyan', 'Davit Mkrtchyan', 'Lesman Santrosyan',
#         'Hovhannes Khachatryan', 'Arman Danielyan', 'Andre Tossunyan', 'Jaque Reid',
#         'Nicole Moore', 'Maddie Millis', 'Gohar Grigoryan', 'Meg Cipriano',
#         'David Murrow', 'Hrag Jinbashian', 'Elen Badalyan', 'Edmon Brutyan'
#     ]
    
#     # Get current account counts
#     current_counts = df_copy['csm_name'].value_counts()
#     print("Current account counts:")
#     for csm in csms_to_reduce + ['Alex Janssens'] + new_csms:
#         count = current_counts.get(csm, 0)
#         print(f"{csm}: {count}")
    
#     # Find removable accounts from the specified CSMs (70 total)
#     removable_from_list = df_copy[
#         (df_copy['csm_name'].isin(csms_to_reduce)) & 
#         (df_copy['account_to_keep'] != True)
#     ].index.tolist()
    
#     print(f"\nRemovable accounts from specified CSMs: {len(removable_from_list)}")
    
#     if len(removable_from_list) >= 70:
#         accounts_to_remove_from_list = random.sample(removable_from_list, 70)
#         print(f"Removing 70 accounts from the specified CSM list")
#     else:
#         accounts_to_remove_from_list = removable_from_list
#         print(f"Only {len(removable_from_list)} removable accounts available from the CSM list")
    
#     # Remove 5 accounts from Alex Janssens
#     alex_removable = df_copy[
#         (df_copy['csm_name'] == 'Alex Janssens') & 
#         (df_copy['account_to_keep'] != True)
#     ].index.tolist()
    
#     if len(alex_removable) >= 5:
#         accounts_to_remove_from_alex = random.sample(alex_removable, 5)
#         print(f"Removing 5 accounts from Alex Janssens")
#     else:
#         accounts_to_remove_from_alex = alex_removable
#         print(f"Only {len(alex_removable)} removable accounts available from Alex Janssens")
    
#     # Calculate how many more accounts we need for new CSMs
#     current_new_csm_counts = {}
#     total_current_new_csm_accounts = 0
    
#     for new_csm in new_csms:
#         count = len(df_copy[df_copy['csm_name'] == new_csm])
#         current_new_csm_counts[new_csm] = count
#         total_current_new_csm_accounts += count
    
#     target_total_new_csm_accounts = len(new_csms) * 80  # 480 total
#     additional_needed = target_total_new_csm_accounts - total_current_new_csm_accounts
    
#     print(f"\nCurrent new CSM accounts: {total_current_new_csm_accounts}")
#     print(f"Target new CSM accounts: {target_total_new_csm_accounts}")
#     print(f"Additional accounts needed: {additional_needed}")
    
#     # Combine accounts to redistribute
#     accounts_to_redistribute = accounts_to_remove_from_list + accounts_to_remove_from_alex
    
#     # If we need more accounts, find additional ones
#     if len(accounts_to_redistribute) < additional_needed:
#         more_needed = additional_needed - len(accounts_to_redistribute)
#         print(f"Need {more_needed} more accounts. Looking for additional removable accounts...")
        
#         # Get other removable accounts (excluding already processed CSMs)
#         already_processed = csms_to_reduce + ['Alex Janssens'] + new_csms
#         other_removable = df_copy[
#             (~df_copy['csm_name'].isin(already_processed)) & 
#             (df_copy['account_to_keep'] != True)
#         ].index.tolist()
        
#         if len(other_removable) >= more_needed:
#             additional_accounts = random.sample(other_removable, more_needed)
#             accounts_to_redistribute.extend(additional_accounts)
#             print(f"Added {len(additional_accounts)} more accounts from other CSMs")
#         else:
#             accounts_to_redistribute.extend(other_removable)
#             print(f"Only {len(other_removable)} additional removable accounts available")
    
#     print(f"Total accounts to redistribute: {len(accounts_to_redistribute)}")
    
#     # Calculate how many accounts each new CSM needs to reach 80
#     new_csm_targets = {}
#     total_to_assign = min(len(accounts_to_redistribute), additional_needed)
    
#     # Shuffle accounts for random distribution
#     random.shuffle(accounts_to_redistribute[:total_to_assign])
    
#     # Assign accounts to new CSMs to bring each to 80
#     account_index = 0
#     for new_csm in new_csms:
#         current_count = current_new_csm_counts[new_csm]
#         needed = 80 - current_count
        
#         if needed > 0 and account_index < total_to_assign:
#             # Get accounts for this CSM
#             end_idx = min(account_index + needed, total_to_assign)
#             accounts_for_csm = accounts_to_redistribute[account_index:end_idx]
            
#             # Assign these accounts to the new CSM
#             df_copy.loc[accounts_for_csm, 'csm_name'] = new_csm
            
#             print(f"Assigned {len(accounts_for_csm)} accounts to {new_csm} (was {current_count}, now {current_count + len(accounts_for_csm)})")
#             account_index = end_idx
    
#     # Verify no protected accounts were moved
#     protected_moved = df_copy[
#         (df_copy['account_to_keep'] == True) & 
#         (df_copy['csm_name'].isin(new_csms))
#     ]
#     if len(protected_moved) > 0:
#         print(f"WARNING: {len(protected_moved)} protected accounts were moved!")
#     else:
#         print("✓ No protected accounts were moved")
    
#     # Show final results
#     print("\n=== FINAL ACCOUNT COUNTS ===")
#     final_counts = df_copy['csm_name'].value_counts()
    
#     print("\nSpecified CSMs (after removal):")
#     for csm in csms_to_reduce:
#         count = final_counts.get(csm, 0)
#         print(f"{csm}: {count}")
    
#     print(f"\nAlex Janssens: {final_counts.get('Alex Janssens', 0)}")
    
#     print("\nNew CSMs:")
#     for new_csm in new_csms:
#         count = final_counts.get(new_csm, 0)
#         print(f"{new_csm}: {count}")
    
#     return df_copy

# # Load your data


# # Execute the redistribution
# df_redistributed = redistribute_accounts(df)

# # Final verification
# print("\n=== VERIFICATION ===")
# final_counts = df_redistributed['csm_name'].value_counts()

# alex_final = final_counts.get('Alex Janssens', 0)
# print(f"Alex Janssens final count: {alex_final}")

# new_csm_total = 0
# for new_csm in ['new_csm_11', 'new_csm_12', 'new_csm_13', 'new_csm_14', 'new_csm_15', 'new_csm_16']:
#     count = final_counts.get(new_csm, 0)
#     new_csm_total += count
#     print(f"{new_csm}: {count}")

# print(f"Total new CSM accounts: {new_csm_total}")

# # Check protected accounts
# protected_in_new_csms = df_redistributed[
#     (df_redistributed['account_to_keep'] == True) & 
#     (df_redistributed['csm_name'].isin(['new_csm_11', 'new_csm_12', 'new_csm_13', 'new_csm_14', 'new_csm_15', 'new_csm_16']))
# ]
# print(f"Protected accounts in new CSMs: {len(protected_in_new_csms)}")


# In[39]:


# df_redistributed['csm_name'].value_counts()
# df_redistributed = df_redistributed.rename({'csm_name': 'new_csm_name'}, axis=1)
# df_redistributed


# In[ ]:





# In[ ]:





# In[ ]:





# In[40]:


def add_balanced_numerical_constraints(
    prob, x, df, num_csms, num_accounts, numerical_column, 
    tolerance_percentage=20, excluded_csm_indices: list = None # <--- NEW PARAMETER
):
    """
    Add constraints to ensure balanced distribution of numerical column values across CSMs.
    Excludes specified CSMs from these balancing constraints.
    """
    print(f"\nBalancing numerical column '{numerical_column}' across CSMs")
    
    if excluded_csm_indices is None:
        excluded_csm_indices = []

    num_csms_to_balance = num_csms - len(excluded_csm_indices)
    
    if num_csms_to_balance <= 0:
        print(f"All CSMs are excluded or no CSMs to balance for {numerical_column}. Skipping constraints.")
        return prob

    # Calculate statistics for the numerical column (for all accounts, then distribute)
    column_values = df[numerical_column].fillna(0)
    total_value = column_values.sum()
    
    # Calculate mean based on CSMs *included* in balancing
    mean_value_per_csm = total_value / num_csms_to_balance 
    
    print(f"Total {numerical_column}: {total_value}")
    print(f"Mean {numerical_column} per CSM (for balanced CSMs): {mean_value_per_csm:.2f}")
    
    # Calculate bounds using tolerance percentage
    tolerance_factor = tolerance_percentage / 100.0
    upper_bound = mean_value_per_csm * (1 + tolerance_factor)
    lower_bound = max(0, mean_value_per_csm * (1 - tolerance_factor))
    
    print(f"Setting bounds for {numerical_column} for balanced CSMs: {lower_bound:.2f} to {upper_bound:.2f}")
    
    # Add constraints for each CSM, but skip if CSM is excluded
    for j in range(num_csms):
        # <--- THE KEY CHANGE IS HERE: Skip if CSM is in excluded_csm_indices
        if j in excluded_csm_indices:
            continue

        # Upper bound constraint
        prob += pulp.lpSum(
            x[i, j] * (df.iloc[i][numerical_column] if pd.notna(df.iloc[i][numerical_column]) else 0)
            for i in range(num_accounts)
        ) <= upper_bound, f"max_{numerical_column}_csm_{j}_numerical"
        
        # Lower bound constraint
        prob += pulp.lpSum(
            x[i, j] * (df.iloc[i][numerical_column] if pd.notna(df.iloc[i][numerical_column]) else 0)
            for i in range(num_accounts)
        ) >= lower_bound, f"min_{numerical_column}_csm_{j}_numerical"
        
    print(f"Added numerical balancing constraints for {numerical_column} (excluding specific CSMs)")
    return prob


# ### Optimization logic

# In[41]:


##### 
def initialize_problem(df, num_csms, new_csms, csms_to_remove, removed_csm_accounts, csm_mapping, fixed_assignments, restricted_assignments, parent_child_accounts, csm_groups, category_limits, industry_csm_mapping,  mt_count_lt_5_csms, mt_count_gte_5_csms, csm_timezone_mapping,  max_accounts_per_csm=1000):
    ''' Function to initialize the optimization problem '''
   
    new_accounts = ['abc']
    num_accounts = len(df)
    # Define the optimization problem
    prob = pulp.LpProblem("CSM_Routing_Optimization", pulp.LpMinimize)
    
    # Define the binary decision variables, here i represents the account index and j represents the CSM index
    # this is used to assign each account to a CSM
    x = pulp.LpVariable.dicts("x", [(i, j) for i in range(num_accounts) for j in range(num_csms)], cat='Binary')

    print('Define totals')
    print()
    # Calculate total scores for each account
    total_scores = df['neediness_score']
    
    
    print('Calculate total scores assigned ')
    print()
    
    # Auxiliary variables for the total score assigned to each CSM
    total_csm_scores = [pulp.LpVariable(f"total_score_{j}", cat='Continuous') for j in range(num_csms)]
    
    
    print('Add totals to the problem')
    print()
    
    for j in range(num_csms):
        prob += total_csm_scores[j] == pulp.lpSum(x[i, j] * total_scores.iloc[i] for i in range(num_accounts))
        
    
    print('Calculate avg values')
    print()
    
    # Calculate the average total score
    avg_total_score = pulp.lpSum(total_csm_scores[j] for j in range(num_csms)) / num_csms
    
    
    # Auxiliary variables for the differences
    diff = [pulp.LpVariable(f"diff_{j}", cat='Continuous') for j in range(num_csms)]
    
    # Add constraints to calculate the differences between the total score and the average total score
    for j in range(num_csms):
        prob += diff[j] == total_csm_scores[j] - avg_total_score
        
    
    
    print('Calculate differences')
    print()
    
    # Auxiliary variables for squared differences using absolute value and linear constraints
    diff_squared = [pulp.LpVariable(f"diff_squared_{j}", lowBound=0, cat='Continuous') for j in range(num_csms)]
    abs_diff = [pulp.LpVariable(f"abs_diff_{j}", lowBound=0, cat='Continuous') for j in range(num_csms)]
    
    
    
    
    print('add abs diff constaints')
    print()
    
    for j in range(num_csms):
        prob += abs_diff[j] >= diff[j]
        prob += abs_diff[j] >= -diff[j]
        prob += diff_squared[j] >= abs_diff[j]
        
       
    print('Define objective function')
    print()
    
    # Objective function: minimize the sum of squared differences
    prob += pulp.lpSum(diff_squared[j] for j in range(num_csms)) 

    
    print('Add constraints')
    print()
    
    # Constraint 1: Each account is assigned to exactly one CSM
    for i in range(num_accounts):
        prob += pulp.lpSum(x[i, j] for j in range(num_csms)) == 1

    # Constraint 2: Each CSM is assigned no more than max_accounts_per_csm accounts
    for j in range(num_csms):
        prob += pulp.lpSum(x[i, j] for i in range(num_accounts)) <= max_accounts_per_csm
        
        
    # new csms should receive atleast avg number of accounts of the existing roster
    mean_assignments = num_accounts / num_csms
    
    print('mean_assignments ', mean_assignments)

    
    # if we are adding new csms then use this block to add constraints. 
    all_csms = list(df['csm_name'].unique())
    print('Balancing all csms in Neediness categories ')
    new_csms_indices = [csm_mapping[csm] for csm in all_csms]
    for j in new_csms_indices:
        pass


    # Balancing need_category only for new CSMs
    need_categories = df['neediness_category'].unique()
    category_counts = df['neediness_category'].value_counts().to_dict()
    mean_category_counts = {category: category_counts[category] / num_csms for category in need_categories}

    # Debugging prints
    print("Need categories:", need_categories)
    print("Category counts:", category_counts)
    print("Mean category counts:", mean_category_counts)

#     # Constraints for new CSMs to get a balanced number of assignments based on need_category
#     for category in need_categories:
#         for j in new_csms_indices:
#             #prob += pulp.lpSum(x[i, j] for i in range(num_accounts) if df['neediness_category'].iloc[i] == category) <= mean_category_counts[category] * 1.5
#             prob += pulp.lpSum(x[i, j] for i in range(num_accounts) if df['neediness_category'].iloc[i] == category) >= mean_category_counts[category] * 0.2

    
    print('handle to reduce shuffling.')
    
    # Define current assignment (for reducing shuffling)
    current_assignment = df['csm_name'].map(csm_mapping)


    
    print('Prevent fixed assignments.')
    
    # Prevent reassigning specific accounts for fixed CSMs
    for fixed_csm, account_ids in fixed_assignments.items():
        fixed_csm_index = csm_mapping[fixed_csm]
        for account_id in account_ids:
            i = df.index[df['account_id'] == account_id].tolist()[0]
            prob += x[i, fixed_csm_index] == 1

    print('Prevent specific assignments.')
    # Prevent specific assignments
    for restricted_csm, account_ids in restricted_assignments.items():
        restricted_csm_index = csm_mapping[restricted_csm]
        for account_id in account_ids:
            i = df.index[df['account_id'] == account_id].tolist()[0]
            prob += x[i, restricted_csm_index] == 0


    print('Handle parent-child accounts.')
    # Dictionary to store which accounts should have their assignments preserved for parent child accounts
    preserve_assignments = {}

#     # Iterate through each row to determine if assignments should be preserved
#     for index, row in df.iterrows():
        
#         parent_id = row['parent_account_id']
        
#         if pd.notna(parent_id):
#             csm_name = row['csm_name']

#             if pd.notna(parent_id) and pd.notna(csm_name) and csm_name in csm_mapping:
#                 csm_index = csm_mapping[csm_name]
#                 # Find other accounts with the same parent and CSM
#                 sibling_indices = df[(df['parent_account_id'] == parent_id) & (df['csm_name'] == csm_name)].index.tolist()
#                 if len(sibling_indices) > 1:
#                     for sibling_index in sibling_indices:
#                         preserve_assignments[sibling_index] = csm_index
        

#     # Apply constraints to preserve the existing assignments
#     for account_index, csm_index in preserve_assignments.items():
#         prob += x[account_index, csm_index] == 1


    print('category_limits ', category_limits)
        
    if list(df['Segment'].unique())[0] == 'Residential' :
        # Category-based constraints for new CSMs with specific category limits
        
        all_csms = list(df['csm_name'].unique()) + new_csms
        print('all_csms ', all_csms)
        print('Balancing all csms in Residential Category-based')
        new_csms_indices = [csm_mapping[csm] for csm in all_csms]   # this new_csm_indices is different
        for category_key, (csm_list, limit) in category_limits.items():
            #print('category_key ', category_key)
            category_1, category_2 = category_key

            for j in new_csms_indices:
                csm_name = all_csms[j - new_csms_indices[0]]
                
                if csm_name in csm_list:
                    print('balancing residential limit count for csm_name ', csm_name)
                    
                    custom_count_list = ['Alex Janssens',
                                         'new_csm_11',
                                         'new_csm_12',
                                         'new_csm_13',
                                         'new_csm_14',
                                         'new_csm_15',
                                         'new_csm_16'
                                        ]
                    
                    if csm_name not in custom_count_list:
                        print('constraint for cat limit ', csm_name)
                        # Category-based constraints for regular CSMs
                        prob += pulp.lpSum(x[i, j] for i in range(num_accounts) if df['Segment'].iloc[i] == category_1 and df['Account Level'].iloc[i] == category_2 ) <= limit + 2
                        prob += pulp.lpSum(x[i, j] for i in range(num_accounts) if df['Segment'].iloc[i] == category_1 and df['Account Level'].iloc[i] == category_2 ) >= limit - 2
                        prob += pulp.lpSum(x[i, j] for i in range(num_accounts) if df['Segment'].iloc[i] == category_1 and df['Account Level'].iloc[i] == category_2 ) == pulp.lpSum(x[i, j] for i in range(num_accounts))
                    else:
                        # Custom constraints for CSMs in the custom_count_list
                        if csm_name == 'Alex Janssens':
                            print('constraint for Alex Janssens cat limit ', csm_name)
                            print('J ', j)
                            # Assign exactly 72 accounts to Alex Janssens
                            prob += pulp.lpSum(x[i, j] for i in range(num_accounts)) == 72

#                         new_csm_custom_count_list = ['new_csm_11', 'new_csm_12', 'new_csm_13', 'new_csm_14', 'new_csm_15', 'new_csm_16']
#                         if csm_name in new_csm_custom_count_list:
#                             print('constraint for new csm cat limit ', csm_name)
#                             print('j ', j)
#                             prob += pulp.lpSum(x[i, j] for i in range(num_accounts)) == 80
                        

    
            
    ### Health Segment : Balancing 'red' and 'green' Across New CSMs
    
    category_column = 'Health Segment'
    value_1 = 'Red'
    value_2 = 'Green'
    value_3 = 'Yellow'


    
    # balance csms for red, green categories

    
    
    for category_key, (csm_list, limit) in category_limits.items():
        #print('category_key ', category_key)
        category_1, category_2 = category_key

        print('category_1 ', category_1)
        print('category_2 ', category_2)


        num_csms_in_categorys = df[ (df['Segment'] == category_1) & (df['Account Level'] == category_2)]['csm_name'].nunique() + len(new_csms)  

        print('num_csms_in_categorys ', num_csms_in_categorys )

        # Calculate the total number of 'red' and 'green' accounts
        total_red_accounts = sum( (df[category_column] == value_1) & (df['Segment'] == category_1) & (df['Account Level'] == category_2 ))
        print('total_red_accounts ', total_red_accounts)

        total_green_accounts = sum( (df[category_column] == value_2) & (df['Segment'] == category_1) & (df['Account Level'] == category_2 ))
        print('total_green_accounts ', total_green_accounts)
        
        total_yellow_accounts = sum( (df[category_column] == value_3) & (df['Segment'] == category_1) & (df['Account Level'] == category_2 ))
        print('total_yellow_accounts ', total_yellow_accounts)

        # Calculate the mean number of 'red' and 'green' accounts per new CSM
        mean_red_accounts = total_red_accounts / num_csms_in_categorys
        print('mean_red_accounts ', mean_red_accounts)
        mean_green_accounts = total_green_accounts / num_csms_in_categorys
        print('mean_green_accounts ', mean_green_accounts)
        mean_yellow_accounts = total_yellow_accounts / num_csms_in_categorys
        print('mean_yellow_accounts ', mean_yellow_accounts)
        print()

        
        all_csms = list(df['csm_name'].unique()) + new_csms
        print('Balancing all csms for TAD')
        new_csms_indices = [csm_mapping[csm] for csm in all_csms]
        print('new_csms_indices ', new_csms_indices)
        
        
        
#         # only balancing new csms
#         all_csms =  new_csms
#         print('Balancing all csms for TAD')
#         new_csms_indices = [csm_mapping[csm] for csm in all_csms]
#         print('new_csms_indices ', new_csms_indices)

        
        
        
        if list(df['Segment'].unique())[0] == 'Residential' :
            

            # Constraints to ensure balanced distribution of 'red' and 'green' accounts across new CSMs
            for j in new_csms_indices:
                print('Balance based on health color')
                
                # print('balance health segment')
                #Balance 'red' accounts

                prob += pulp.lpSum(
                    x[i, j] for i in range(num_accounts) if df[category_column].iloc[i] == value_1 and df['Segment'].iloc[i] == category_1 and df['Account Level'].iloc[i] == category_2 
                ) <= mean_red_accounts * 1.2

                prob += pulp.lpSum(
                    x[i, j] for i in range(num_accounts) if df[category_column].iloc[i] == value_1 and df['Segment'].iloc[i] == category_1 and df['Account Level'].iloc[i] == category_2 
                ) >= mean_red_accounts * 0.8

                #Balance 'green' accounts

                prob += pulp.lpSum(
                    x[i, j] for i in range(num_accounts) if df[category_column].iloc[i] == value_2 and df['Segment'].iloc[i] == category_1 and df['Account Level'].iloc[i] == category_2 
                ) <= mean_green_accounts * 1.2

                prob += pulp.lpSum(
                    x[i, j] for i in range(num_accounts) if df[category_column].iloc[i] == value_2 and df['Segment'].iloc[i] == category_1 and df['Account Level'].iloc[i] == category_2 
                ) >= mean_green_accounts * 0.8
                
                # Balance 'yellow' accounts

                prob += pulp.lpSum(
                    x[i, j] for i in range(num_accounts) if df[category_column].iloc[i] == value_3 and df['Segment'].iloc[i] == category_1 and df['Account Level'].iloc[i] == category_2 
                ) <= mean_yellow_accounts * 1.1

                prob += pulp.lpSum(
                    x[i, j] for i in range(num_accounts) if df[category_column].iloc[i] == value_3 and df['Segment'].iloc[i] == category_1 and df['Account Level'].iloc[i] == category_2 
                ) >= mean_yellow_accounts * 0.9
    
    
    
    ### cust_tenure_tad_category : Balancing cust_tenure_tad_category Across New CSMs
    
    category_column = 'cust_tenure_tad_category'
    value_1 = ''
    value_2 = ''
    
    if list(df['Segment'].unique())[0] == 'Residential' :
        value_1 = 'Customer over 90 days - under 125 TAD'
        value_2 = 'Customer over 90 days - 125 TAD and above'
                                 
    if list(df['Segment'].unique())[0] == 'Commercial & Construction' :
        value_1 = 'Customer over 365 days - under 125 TAD'
        value_2 = 'Customer under 365 days from Success Transition Date - under 100 TAD' 
         
    
    # balance csms for cust_tenure_tad_category
    
        
    for category_key, (csm_list, limit) in category_limits.items():
        #print('category_key ', category_key)
        category_1, category_2 = category_key

        print('category_1 ', category_1)
        print('category_2 ', category_2)


        num_csms_in_categorys = df[ (df['Segment'] == category_1) & (df['Account Level'] == category_2)]['csm_name'].nunique() + len(new_csms)

        print('num_csms_in_categorys ', num_csms_in_categorys )

        # Calculate the total number of 'value_1' and 'value_2' accounts
        total_value_1_accounts = sum( (df[category_column] == value_1) & (df['Segment'] == category_1) & (df['Account Level'] == category_2 ))
        print('total_value_1_accounts ', total_value_1_accounts)

        total_value_2_accounts = sum( (df[category_column] == value_2) & (df['Segment'] == category_1) & (df['Account Level'] == category_2 ))
        print('total_value_2_accounts ', total_value_2_accounts)

        # Calculate the mean number of 'value_1' and 'value_2' accounts per new CSM
        mean_value_1_accounts = total_value_1_accounts / num_csms_in_categorys
        print('mean_value_1_accounts ', mean_value_1_accounts)
        mean_value_2_accounts = total_value_2_accounts / num_csms_in_categorys
        print('mean_value_2_accounts ', mean_value_2_accounts)
        print()


        all_csms = list(df['csm_name'].unique()) + new_csms
        print('Balancing all csms for TAD')
        new_csms_indices = [csm_mapping[csm] for csm in all_csms]
        print('TAD new_csms_indices ', new_csms_indices)
        
        
        
        if list(df['Segment'].unique())[0] == 'Residential' :
            
            #Constraints to ensure balanced distribution of 'value_1' and 'value_2' accounts across new CSMs
            for j in new_csms_indices:

                # Balance 'value_1' accounts

                prob += pulp.lpSum(
                    x[i, j] for i in range(num_accounts) if df[category_column].iloc[i] == value_1 and df['Segment'].iloc[i] == category_1 and df['Account Level'].iloc[i] == category_2 
                ) <= mean_value_1_accounts * 1.5

                prob += pulp.lpSum(
                    x[i, j] for i in range(num_accounts) if df[category_column].iloc[i] == value_1 and df['Segment'].iloc[i] == category_1 and df['Account Level'].iloc[i] == category_2 
                ) >= mean_value_1_accounts * 0.4

                # Balance 'value_2' accounts

                prob += pulp.lpSum(
                    x[i, j] for i in range(num_accounts) if df[category_column].iloc[i] == value_2 and df['Segment'].iloc[i] == category_1 and df['Account Level'].iloc[i] == category_2 
                ) <= mean_value_2_accounts * 1.5

                prob += pulp.lpSum(
                    x[i, j] for i in range(num_accounts) if df[category_column].iloc[i] == value_2 and df['Segment'].iloc[i] == category_1 and df['Account Level'].iloc[i] == category_2 
                ) >= mean_value_2_accounts * 0.4


    
    category_column = 'Health Segment'
    value_1 = 'Red'
    value_2 = 'Green'
    value_3 = 'Yellow'
   


    # Handle industry mapping
    
    print('industry_csm_mapping ', industry_csm_mapping)
    #print('csm_mapping ', csm_mapping)
    print('num_csms ', num_csms)
        
    excluded_csm_indices = None
    
    prob = add_balanced_numerical_constraints(
        prob=prob, 
        x=x, 
        df=df, 
        num_csms=num_csms, 
        num_accounts=num_accounts,
        numerical_column='revenue',
        tolerance_percentage=5, # ±25% from mean
        excluded_csm_indices=excluded_csm_indices # <--- PASS EXCLUSION LIST
    )
    
    
    
    penalty_for_shuffling = 10 
    prob += pulp.lpSum(penalty_for_shuffling * x[i, j] * (current_assignment[i] != j)
                      for i in range(num_accounts)
                       for j in range(num_csms)
                      )
  
    
    print('return the problem.')
    return prob, x



def solve_problem(df, new_csm_list, new_csms, csms_to_remove, fixed_assignments, restricted_assignments, parent_child_accounts, csm_groups, removed_csm_accounts, category_limits, industry_csm_mapping,  mt_count_lt_5_csms, mt_count_gte_5_csms,  csm_timezone_mapping, max_accounts_per_csm=1000, ):
    ''' Function to solve the optimization problem '''
    
    csm_mapping = {csm: i for i, csm in enumerate(new_csm_list)}
    
    num_csms = len(new_csm_list)
    prob, x = initialize_problem(df, num_csms, new_csms, csms_to_remove, removed_csm_accounts, csm_mapping, fixed_assignments, restricted_assignments, parent_child_accounts, csm_groups, category_limits, industry_csm_mapping, mt_count_lt_5_csms, mt_count_gte_5_csms, csm_timezone_mapping,  max_accounts_per_csm)
    #prob.solve(pulp.PULP_CBC_CMD( timeLimit=300))
    prob.solve()
    num_accounts = len(df)
    assignments = [(i, j) for i in range(num_accounts) for j in range(num_csms) if pulp.value(x[i, j]) == 1]
    optimized_df = df.copy()
    optimized_df['new_csm_name'] = [new_csm_list[j] for i, j in assignments]
    return optimized_df

def modify_csm_list(df, csms_to_add=[], csms_to_remove=[], fixed_assignments={}, restricted_assignments={}, parent_child_accounts={}, csm_groups={}, category_limits={}, industry_csm_mapping={}, mt_count_lt_5_csms={}, mt_count_gte_5_csms={}, csm_timezone_mapping={}):
    ''' Function to modify the CSM list '''
    removed_csm_accounts = df['csm_name'].isin(csms_to_remove).astype(int)
    
    # Mark the accounts with removed CSMs as unassigned (NaN)
    df.loc[df['csm_name'].isin(csms_to_remove), 'csm_name'] = np.nan 
    
    # Create a list of current CSMs excluding the ones to be removed
    current_csms = [csm for csm in df['csm_name'].dropna().unique() if csm not in csms_to_remove]
    
    # Add new CSMs to the pool
    new_csm_list = current_csms + csms_to_add 
    if 'unassigned' in new_csm_list:
        new_csm_list.remove('unassigned')
    #print('new_csm_list ', new_csm_list)
    
    # Solve the problem with the updated CSM list
    return solve_problem(df, new_csm_list, csms_to_add, csms_to_remove, fixed_assignments, restricted_assignments, parent_child_accounts, csm_groups, removed_csm_accounts, category_limits, industry_csm_mapping, mt_count_lt_5_csms, mt_count_gte_5_csms, csm_timezone_mapping,  max_accounts_per_csm=1000)

def add_new_accounts(df, new_accounts):
    new_accounts['csm_name'] = np.nan
    new_df = pd.concat([df, new_accounts], ignore_index=True)
    current_csms = new_df['csm_name'].dropna().unique().tolist()
    return solve_problem(new_df, current_csms, [],[], None, max_accounts_per_csm=1000)

def remove_accounts(df, accounts_to_remove):
    df = df[~df['account_id'].isin(accounts_to_remove)]
    current_csms = df['csm_name'].unique().tolist()
    return solve_problem(df, current_csms, [], [], None, max_accounts_per_csm=1000)

def get_total_scores(optimized_df):
    total_scores = optimized_df.groupby('new_csm_name')[['neediness_score']].sum()
    total_scores['total_score'] = total_scores.sum(axis=1)
    total_scores['total_accounts'] = optimized_df.groupby('new_csm_name').size()
    return total_scores



# Example usage:

start_index = 11  # replace with the starting index for the new CSMs

csms_to_add = [f'new_csm_{i}' for i in range(start_index, start_index + num_csms_to_add)] 



csms_to_remove = []

# fixed_assignments = { 'Andrew Guth': ['0011a00000CYkPbAAL'],  # Example of specific accounts that should remain assigned to csm_1
#                         }

fixed_assignments = {}


restricted_assignments = {}

category_limits = {}

category_limits_for_csms = df['csm_name'].unique().tolist()

if (run_for_Segment == 'Residential') & (run_for_account_level == 'Corporate'):
    category_limits = {

                        ('Residential','Corporate'): (csms_to_add + category_limits_for_csms, 84),

                        }

    
# Example of parent-child relationships dictionary {'0011a00000cDmfcAAC': ['0011a00000CYkHOAA1', '0011a00000e2TOZAA2']  }


parent_child_accounts =  parent_child_accounts_mapping


csm_groups = {
                'group_1': csms_to_add,  # Example of CSM groups
                
            }


#industry_csm_mapping = {'Ryan Sterritt':'Roofing'}
industry_csm_mapping = {}



# this is the main loop that will run the optimization problem, i am running it only once for now. ( we can run multiple times to see if the results are consistent)
for i in range(1):

    try :
        print('Iteration ', str(i))
        
        # load the data
        df = load_data()
        print()
        print('df churn risk accounts ', df['Churn Risk Status'].value_counts())
        print()
        
        # update fixed assignments for churn risk , the accounts with churn risk should not be changed
        for index, row in df.iterrows():
            if row['Churn Risk Status'] != 'Not at risk':
                
                if row['csm_name'] in fixed_assignments:
                    fixed_assignments[row['csm_name']].append(row['account_id'])
                else:
                    fixed_assignments[row['csm_name']] = [row['account_id']]
        
        

        
        print('Add accounts to keep data to fixed assignments dictionary')
        
        for index, row in df.iterrows():
            if row['account_to_keep'] == True:
                
                if row['csm_name'] in fixed_assignments:
                    fixed_assignments[row['csm_name']].append(row['account_id'])
                else:
                    fixed_assignments[row['csm_name']] = [row['account_id']]
        
        
        
        #print('fixed_assignments ', fixed_assignments)
        print()
        
        # randomly adjust the MT >=5 and MT < 5 to csms of those categories. 
        mt_gte_5_accounts = df.index[df['tech_count'] >= 5].tolist()  
        mt_lt_5_accounts = df.index[df['tech_count'] < 5].tolist()  
        
            
        
        
        mt_count_lt_5_csms = mt_count_lt_5_csms 
        
        mt_count_gte_5_csms = mt_count_gte_5_csms #+ ['new_csm_11', 'new_csm_12', 'new_csm_13'] #['Krister Karlsson', 'Vahagn Yaralian']

        optimized_df = modify_csm_list(df, csms_to_add=csms_to_add, csms_to_remove=csms_to_remove, fixed_assignments=fixed_assignments, restricted_assignments=restricted_assignments, parent_child_accounts=parent_child_accounts, csm_groups=csm_groups, category_limits=category_limits, industry_csm_mapping=industry_csm_mapping, mt_count_lt_5_csms=mt_count_lt_5_csms, mt_count_gte_5_csms=mt_count_gte_5_csms, csm_timezone_mapping=csm_timezone_mapping )

        
        
        optimized_df[optimized_df['new_csm_name'].isin(csms_to_add)].to_csv('optimized_df_new_assignments_' + str(i) + '.csv')
        
        
        print()
    except Exception as e: 
        print(e)
        print('Iteration failed ', str(i))
        continue
        time.sleep(10)


# In[ ]:





# In[42]:


fixed_assignment_count = sum(len(accounts) for accounts in fixed_assignments.values())
fixed_assignment_count


# In[ ]:





# In[43]:


df[df['account_id'] =='0011P00000xViOJQA0']


# In[44]:


optimized_df[optimized_df['account_id'] =='0011P00000xViOJQA0']


# In[45]:


optimized_df[optimized_df['new_csm_name'].isin(csms_to_add)]


# In[ ]:





# In[46]:


get_total_scores(optimized_df)


# In[ ]:





# In[47]:


def get_total_scores_for_analysis(df, csm_column):
    total_scores = df.groupby(csm_column)[[ 'neediness_score']].sum()
    total_scores['total_score'] = total_scores.sum(axis=1)
    
    total_scores['total_revenue'] = df.groupby(csm_column)['revenue'].sum()
    
    # Count of 'Red' health segment accounts
    total_scores['red_health_count'] = df[df['Health Segment'] == 'Red'].groupby(csm_column)['Health Segment'].count()
    
    return total_scores

def analyze_assignments(original_df, optimized_df):
    # Calculate total scores before and after optimization
    original_scores = get_total_scores_for_analysis(original_df, 'csm_name')
    optimized_scores = get_total_scores_for_analysis(optimized_df, 'new_csm_name')
    
    # Calculate the number of accounts before and after optimization
    original_account_counts = original_df['csm_name'].value_counts()
    optimized_account_counts = optimized_df['new_csm_name'].value_counts()
    
    # Merge the original and optimized dataframes on 'account_id'
    merged_df = pd.merge(original_df, optimized_df, on='account_id', how='outer', suffixes=('_original', '_optimized'))
    
    # Identify the accounts that have been reassigned
    reassigned_accounts = merged_df[merged_df['csm_name_original'] != merged_df['new_csm_name']]['new_csm_name'].value_counts()
    
    # Create a summary dataframe
    summary_df = pd.DataFrame({
        'original_total_score': original_scores['total_score'],
        'optimized_total_score': optimized_scores['total_score'],
        'original_total_revenue': original_scores['total_revenue'],
        'optimized_total_revenue': optimized_scores['total_revenue'],
        'original_account_count': original_account_counts,
        'optimized_account_count': optimized_account_counts,
        'reassigned_account_count': reassigned_accounts,
        'original_red_health_count': original_scores['red_health_count'],
        'optimized_red_health_count': optimized_scores['red_health_count']
    }).fillna(0)  # Fill NaNs with 0 for CSMs that may not have assignments

    # Calculate changes
    summary_df['score_change'] = summary_df['optimized_total_score'] - summary_df['original_total_score']
    summary_df['account_count_change'] = summary_df['optimized_account_count'] - summary_df['original_account_count']
    summary_df['red_health_count_change'] = summary_df['optimized_red_health_count'] - summary_df['original_red_health_count']
    
    return summary_df

# Generate the summary analysis
summary_df = analyze_assignments(df, optimized_df)
print("Summary of Changes in Assignments:")
summary_df


# In[48]:


df


# In[ ]:





# In[49]:


#df_redistributed['csm_name'] = df_redistributed['new_csm_name']


# In[50]:




# def get_total_scores_for_analysis(df, csm_column):
#     total_scores = df.groupby(csm_column)[[ 'neediness_score']].sum()
#     total_scores['total_score'] = total_scores.sum(axis=1)
    
#     total_scores['total_revenue'] = df.groupby(csm_column)['revenue'].sum()
    
#     # Count of 'Red' health segment accounts
#     total_scores['red_health_count'] = df[df['Health Segment'] == 'Red'].groupby(csm_column)['Health Segment'].count()
    
#     return total_scores

# def analyze_assignments(original_df, optimized_df):
#     # Calculate total scores before and after optimization
#     original_scores = get_total_scores_for_analysis(original_df, 'csm_name')
#     optimized_scores = get_total_scores_for_analysis(optimized_df, 'new_csm_name')
    
#     # Calculate the number of accounts before and after optimization
#     original_account_counts = original_df['csm_name'].value_counts()
#     optimized_account_counts = optimized_df['new_csm_name'].value_counts()
    
#     # Merge the original and optimized dataframes on 'account_id'
#     merged_df = pd.merge(original_df, optimized_df, on='account_id', how='outer', suffixes=('_original', '_optimized'))
    
#     # Identify the accounts that have been reassigned
#     reassigned_accounts = merged_df[merged_df['csm_name_original'] != merged_df['new_csm_name']]['new_csm_name'].value_counts()
    
#     # Create a summary dataframe
#     summary_df = pd.DataFrame({
#         'original_total_score': original_scores['total_score'],
#         'optimized_total_score': optimized_scores['total_score'],
#         'original_total_revenue': original_scores['total_revenue'],
#         'optimized_total_revenue': optimized_scores['total_revenue'],
#         'original_account_count': original_account_counts,
#         'optimized_account_count': optimized_account_counts,
#         'reassigned_account_count': reassigned_accounts,
#         'original_red_health_count': original_scores['red_health_count'],
#         'optimized_red_health_count': optimized_scores['red_health_count']
#     }).fillna(0)  # Fill NaNs with 0 for CSMs that may not have assignments

#     # Calculate changes
#     summary_df['score_change'] = summary_df['optimized_total_score'] - summary_df['original_total_score']
#     summary_df['account_count_change'] = summary_df['optimized_account_count'] - summary_df['original_account_count']
#     summary_df['red_health_count_change'] = summary_df['optimized_red_health_count'] - summary_df['original_red_health_count']
    
#     return summary_df

# # Generate the summary analysis
# summary_df = analyze_assignments(df, df_redistributed)
# print("Summary of Changes in Assignments:")
# summary_df


# In[51]:


# df_redistributed.to_csv('small_df_redistributed.csv')
# df_redistributed['new_csm_name_v1'] = df_redistributed['new_csm_name']
# df_redistributed[['account_id', 'new_csm_name_v1']]


# In[ ]:





# In[ ]:





# In[ ]:





# In[52]:


# input_df = pd.read_csv(csv_file)
# input_df.shape


# merge_df = input_df.merge(df_redistributed[['account_id', 'new_csm_name_v1']], how = 'left', left_on = 'ACCOUNT_ID', right_on = 'account_id')
# merge_df['new_csm_name_updated'] = merge_df['new_csm_name_v1'].fillna(merge_df['new_csm_name'])
# merge_df.to_csv('small_df_redistributed.csv')


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[53]:



import matplotlib.pyplot as plt

# Plotting the changes for better visualization
fig, axes = plt.subplots(2, 1, figsize=(12, 10))

# Change in Total Scores
summary_df['score_change'].plot(kind='bar', ax=axes[0], color='skyblue')
axes[0].set_title('Change in Total Scores per CSM')
axes[0].set_xlabel('CSM')
axes[0].set_ylabel('Score Change')

# Change in Number of Accounts
summary_df['account_count_change'].plot(kind='bar', ax=axes[1], color='lightgreen')
axes[1].set_title('Change in Number of Accounts per CSM')
axes[1].set_xlabel('CSM')
axes[1].set_ylabel('Account Count Change')

plt.tight_layout()
plt.show()


# In[ ]:





# In[ ]:





# In[ ]:





# ### save results to output file 

# In[54]:


import datetime

# Get the current date
current_date = datetime.datetime.now()

# Format the date as Aug_22_2024
formatted_date = current_date.strftime('%b_%d_%Y')

# Create the file name
file_name = run_for_Segment + f'_csm_routing_results_for_review_{formatted_date}_v0.csv'

print('output file_name ', file_name)

# Save the DataFrame to a CSV file
optimized_df.to_csv(file_name)





# ## Use the following file as final output

# In[55]:




full_file_name = run_for_Segment +  f'_full_csm_routing_results_for_review_{formatted_date}_v0.csv'

merge_input_df = pd.read_csv(csv_file)



merge_input_df.merge(optimized_df[['account_id', 'new_csm_name', 'csm_changed_calendar_date', 'previous_csm', 'csm_changed_last_6mnths']], how='left', 
                     left_on = 'ACCOUNT_ID', 
                     right_on = 'account_id').to_csv(full_file_name)


# In[ ]:





# In[56]:


optimized_df


# In[57]:


csv_bob_mapping = 'Resi_CSM_BoB_Type_mapping_v0 - Sheet1.csv'

csv_bob_mapping_df = pd.read_csv(csv_bob_mapping)

mt_count_lt_5_csms = list(csv_bob_mapping_df[csv_bob_mapping_df['BoB Type'] == '0-4 MT']['CSM NAME'])

mt_count_gte_5_csms = list(csv_bob_mapping_df[csv_bob_mapping_df['BoB Type'] == '5+ MT']['CSM NAME'])


# In[58]:


for csm in list(optimized_df['new_csm_name'].unique()):
    print()
    print('CSM ', csm)
    if csm in mt_count_gte_5_csms:
        print('5+ MT CSM')
    if csm in mt_count_lt_5_csms:
        print('0-4 MT CSM')
        
    print(optimized_df[optimized_df['new_csm_name'] == csm]['industry_size'].value_counts().sort_index())
    print()
    


# In[59]:


optimized_df[optimized_df['new_csm_name'] == 'Alexa Castro']['industry_size'].value_counts().sort_index()


# In[60]:


df[df['csm_name'] == 'Gor Shahbazyan']['industry_size'].value_counts().sort_index()


# In[61]:


optimized_df[optimized_df['new_csm_name'] == 'new_csm_11']['tech_count'].value_counts().sort_index()


# In[ ]:





# In[62]:


optimized_df['new_csm_name'].isna().value_counts()


# In[ ]:





# In[63]:


import plotly.graph_objects as go


# In[64]:


grouped_df = optimized_df.groupby(['new_csm_name', 'Health Segment']).size().reset_index(name='counts')


# In[65]:


grouped_df


# In[66]:


optimized_df['Health Segment'].value_counts()


# In[67]:


optimized_df[optimized_df['new_csm_name'] == 'Krister Karlsson']


# In[68]:


grouped_df = optimized_df[optimized_df['industry_size'] == '5+ MT'].groupby(['new_csm_name', 'Health Segment']).size().reset_index(name='counts')
grouped_df


# In[69]:


import plotly.graph_objects as go
import pandas as pd

# Assuming optimized_df is your DataFrame
# Group the data by 'new_csm_name' and count the occurrences
grouped_df = optimized_df.groupby('new_csm_name').size().reset_index(name='counts')

# Create a bar for each 'new_csm_name'
bars = [go.Bar(y=grouped_df['new_csm_name'], x=grouped_df['counts'], text=grouped_df['counts'], textposition='auto', orientation='h')]

# Create the figure with the bars
fig = go.Figure(data=bars)

# Change the bar mode to stack
fig.update_layout(
    yaxis={'categoryorder':'total ascending'},
    autosize=False,
    width=1000,
    height=800,
    margin=dict(
        l=50,  # left margin
        r=50,  # right margin
        b=100,  # bottom margin
        t=100,  # top margin
        pad=10
    ),
)

fig.show()


# In[70]:


import plotly.graph_objects as go
import pandas as pd

# Assuming optimized_df is your DataFrame
# Group the data by 'csm' and 'Health Segment' and count the occurrences
grouped_df = optimized_df[optimized_df['industry_size'] == '5+ MT'].groupby(['new_csm_name', 'Health Segment']).size().reset_index(name='counts')

# Create a dictionary mapping 'Health Segment' values to colors
color_dict = {'Red': 'red', 'Green': 'green', 'Yellow': 'yellow'}

# Create a bar for each 'Health Segment'
bars = []
for health_segment in ['Red', 'Green', 'Yellow']:
    subset = grouped_df[grouped_df['Health Segment'] == health_segment]
    bars.append(go.Bar(name=health_segment, y=subset['new_csm_name'], x=subset['counts'], text=subset['counts'], textposition='auto', orientation='h', marker_color=color_dict[health_segment]))

# Create the figure with the bars
fig = go.Figure(data=bars)

# Change the bar mode to stack
fig.update_layout(
    barmode='stack', 
    yaxis={'categoryorder':'total ascending'},
    autosize=False,
    width=1000,
    height=800,
    margin=dict(
        l=50,  # left margin
        r=50,  # right margin
        b=100,  # bottom margin
        t=100,  # top margin
        pad=10
    ),
)

fig.show()


# In[71]:


import plotly.graph_objects as go
import pandas as pd

# Assuming optimized_df is your DataFrame
# Group the data by 'csm' and 'Health Segment' and count the occurrences
grouped_df = optimized_df[optimized_df['industry_size'] == '0-4 MT'].groupby(['new_csm_name', 'Health Segment']).size().reset_index(name='counts')

# Create a dictionary mapping 'Health Segment' values to colors
color_dict = {'Red': 'red', 'Green': 'green', 'Yellow': 'yellow'}

# Create a bar for each 'Health Segment'
bars = []
for health_segment in ['Red', 'Green', 'Yellow']:
    subset = grouped_df[grouped_df['Health Segment'] == health_segment]
    bars.append(go.Bar(name=health_segment, y=subset['new_csm_name'], x=subset['counts'], text=subset['counts'], textposition='auto', orientation='h', marker_color=color_dict[health_segment]))

# Create the figure with the bars
fig = go.Figure(data=bars)

# Change the bar mode to stack
fig.update_layout(
    barmode='stack', 
    yaxis={'categoryorder':'total ascending'},
    autosize=False,
    width=1000,
    height=800,
    margin=dict(
        l=50,  # left margin
        r=50,  # right margin
        b=100,  # bottom margin
        t=100,  # top margin
        pad=10
    ),
)

fig.show()


# In[72]:


import plotly.graph_objects as go
import pandas as pd

# Assuming optimized_df is your DataFrame
# Group the data by 'csm' and 'Health Segment' and count the occurrences
grouped_df = optimized_df.groupby(['new_csm_name', 'Health Segment']).size().reset_index(name='counts')

# Create a dictionary mapping 'Health Segment' values to colors
color_dict = {'Red': 'red', 'Green': 'green', 'Yellow': 'yellow'}

# Create a bar for each 'Health Segment'
bars = []
for health_segment in ['Red', 'Green', 'Yellow']:
    subset = grouped_df[grouped_df['Health Segment'] == health_segment]
    bars.append(go.Bar(name=health_segment, y=subset['new_csm_name'], x=subset['counts'], text=subset['counts'], textposition='auto', orientation='h', marker_color=color_dict[health_segment]))

# Create the figure with the bars
fig = go.Figure(data=bars)

# Change the bar mode to stack
fig.update_layout(
    barmode='stack', 
    yaxis={'categoryorder':'total ascending'},
    autosize=False,
    width=1000,
    height=800,
    margin=dict(
        l=50,  # left margin
        r=50,  # right margin
        b=100,  # bottom margin
        t=100,  # top margin
        pad=10
    ),
)

fig.show()


# In[73]:


optimized_df


# In[74]:


optimized_df[['account_id', 'csm_name', 'neediness_score', 'neediness_category', 'Health Segment', 'tad_score', 'new_csm_name' ]].to_csv('Resi_corp_custom_run_feb_2025.csv')


# In[75]:


optimized_df.shape


# In[76]:


print('Final balance for any remaining gaps.')
print()
print('mt_count_lt_5_csms ', mt_count_lt_5_csms)
print()

print('mt_count_gte_5_csms ', mt_count_gte_5_csms)


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[77]:


df['industry_size'].value_counts()


# In[78]:


for csm in list(optimized_df['new_csm_name'].unique()):
    print()
    print('CSM ', csm)
    if csm in mt_count_gte_5_csms:
        print('5+ MT CSM')
    if csm in mt_count_lt_5_csms:
        print('0-4 MT CSM')
        
    print(optimized_df[optimized_df['new_csm_name'] == csm]['industry_size'].value_counts().sort_index())
    print()
    


# In[79]:


df.groupby(['industry_size', 'Health Segment']).size()


# In[80]:


# 0-4 MT   -- 12 CSMs
# 5 MT     -- 27 CSMs


# In[81]:


# 5+ MT  
414/26


# In[82]:


# 0-4 MT 
477/11


# In[83]:


# 0-4 MT 
187/11


# In[84]:


# 5+ MT  
89/26


# In[85]:


df


# ## Existing distribution

# In[86]:


import plotly.graph_objects as go
import pandas as pd

# Assuming optimized_df is your DataFrame
# Group the data by 'csm' and 'Health Segment' and count the occurrences
grouped_df = df.groupby(['csm_name', 'Health Segment']).size().reset_index(name='counts')

# Create a dictionary mapping 'Health Segment' values to colors
color_dict = {'Red': 'red', 'Green': 'green', 'Yellow': 'yellow'}

# Create a bar for each 'Health Segment'
bars = []
for health_segment in ['Red', 'Green', 'Yellow']:
    subset = grouped_df[grouped_df['Health Segment'] == health_segment]
    bars.append(go.Bar(name=health_segment, y=subset['csm_name'], x=subset['counts'], text=subset['counts'], textposition='auto', orientation='h', marker_color=color_dict[health_segment]))

# Create the figure with the bars
fig = go.Figure(data=bars)

# Change the bar mode to stack
fig.update_layout(
    barmode='stack', 
    yaxis={'categoryorder':'total ascending'},
    autosize=False,
    width=1000,
    height=800,
    margin=dict(
        l=50,  # left margin
        r=50,  # right margin
        b=100,  # bottom margin
        t=100,  # top margin
        pad=10
    ),
)

fig.show()


# ## new distribution

# In[87]:


import plotly.graph_objects as go
import pandas as pd

# Assuming optimized_df is your DataFrame
# Group the data by 'csm' and 'Health Segment' and count the occurrences
grouped_df = optimized_df.groupby(['new_csm_name', 'Health Segment']).size().reset_index(name='counts')

# Create a dictionary mapping 'Health Segment' values to colors
color_dict = {'Red': 'red', 'Green': 'green', 'Yellow': 'yellow'}

# Create a bar for each 'Health Segment'
bars = []
for health_segment in ['Red', 'Green', 'Yellow']:
    subset = grouped_df[grouped_df['Health Segment'] == health_segment]
    bars.append(go.Bar(name=health_segment, y=subset['new_csm_name'], x=subset['counts'], text=subset['counts'], textposition='auto', orientation='h', marker_color=color_dict[health_segment]))

# Create the figure with the bars
fig = go.Figure(data=bars)

# Change the bar mode to stack
fig.update_layout(
    barmode='stack', 
    yaxis={'categoryorder':'total ascending'},
    autosize=False,
    width=1000,
    height=800,
    margin=dict(
        l=50,  # left margin
        r=50,  # right margin
        b=100,  # bottom margin
        t=100,  # top margin
        pad=10
    ),
)

fig.show()


# In[ ]:





# In[ ]:





# In[ ]:





# In[88]:


stop


# In[ ]:





# In[ ]:


import plotly.graph_objects as go
import pandas as pd

# Assuming optimized_df is your DataFrame
# Group the data by 'csm' and 'Health Segment' and count the occurrences
grouped_df = optimized_df[optimized_df['industry_size'] == '0-4 MT'].groupby(['new_csm_name', 'Health Segment']).size().reset_index(name='counts')

# Create a dictionary mapping 'Health Segment' values to colors
color_dict = {'Red': 'red', 'Green': 'green', 'Yellow': 'yellow'}

# Create a bar for each 'Health Segment'
bars = []
for health_segment in ['Red', 'Green', 'Yellow']:
    subset = grouped_df[grouped_df['Health Segment'] == health_segment]
    bars.append(go.Bar(name=health_segment, y=subset['new_csm_name'], x=subset['counts'], text=subset['counts'], textposition='auto', orientation='h', marker_color=color_dict[health_segment]))

# Create the figure with the bars
fig = go.Figure(data=bars)

# Change the bar mode to stack
fig.update_layout(
    barmode='stack', 
    yaxis={'categoryorder':'total ascending'},
    autosize=False,
    width=1000,
    height=800,
    margin=dict(
        l=50,  # left margin
        r=50,  # right margin
        b=100,  # bottom margin
        t=100,  # top margin
        pad=10
    ),
)

fig.show()


# In[ ]:


import plotly.graph_objects as go
import pandas as pd

# Assuming optimized_df is your DataFrame
# Group the data by 'csm' and 'Health Segment' and count the occurrences
grouped_df = optimized_df[optimized_df['industry_size'] == '5+ MT'].groupby(['new_csm_name', 'Health Segment']).size().reset_index(name='counts')

# Create a dictionary mapping 'Health Segment' values to colors
color_dict = {'Red': 'red', 'Green': 'green', 'Yellow': 'yellow'}

# Create a bar for each 'Health Segment'
bars = []
for health_segment in ['Red', 'Green', 'Yellow']:
    subset = grouped_df[grouped_df['Health Segment'] == health_segment]
    bars.append(go.Bar(name=health_segment, y=subset['new_csm_name'], x=subset['counts'], text=subset['counts'], textposition='auto', orientation='h', marker_color=color_dict[health_segment]))

# Create the figure with the bars
fig = go.Figure(data=bars)

# Change the bar mode to stack
fig.update_layout(
    barmode='stack', 
    yaxis={'categoryorder':'total ascending'},
    autosize=False,
    width=1000,
    height=800,
    margin=dict(
        l=50,  # left margin
        r=50,  # right margin
        b=100,  # bottom margin
        t=100,  # top margin
        pad=10
    ),
)

fig.show()


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:


def get_total_scores_for_analysis(df, csm_column):
    total_scores = df.groupby(csm_column)[[ 'neediness_score']].sum()
    total_scores['total_score'] = total_scores.sum(axis=1)
    
    total_scores['total_revenue'] = df.groupby(csm_column)['revenue'].sum()
    
    # Count of 'Red' health segment accounts
    total_scores['red_health_count'] = df[df['Health Segment'] == 'Red'].groupby(csm_column)['Health Segment'].count()
    
    return total_scores

def analyze_assignments(original_df, balanced_df):
    # Calculate total scores before and after optimization
    original_scores = get_total_scores_for_analysis(original_df, 'csm_name')
    optimized_scores = get_total_scores_for_analysis(balanced_df, 'new_csm_name')
    
    # Calculate the number of accounts before and after optimization
    original_account_counts = original_df['csm_name'].value_counts()
    optimized_account_counts = balanced_df['new_csm_name'].value_counts()
    
    # Merge the original and optimized dataframes on 'account_id'
    merged_df = pd.merge(original_df, balanced_df, on='account_id', how='outer', suffixes=('_original', '_optimized'))
    
    # Identify the accounts that have been reassigned
    reassigned_accounts = merged_df[merged_df['csm_name_original'] != merged_df['new_csm_name']]['new_csm_name'].value_counts()
    
    # Create a summary dataframe
    summary_df = pd.DataFrame({
        'original_total_score': original_scores['total_score'],
        'optimized_total_score': optimized_scores['total_score'],
        'original_total_revenue': original_scores['total_revenue'],
        'optimized_total_revenue': optimized_scores['total_revenue'],
        'original_account_count': original_account_counts,
        'optimized_account_count': optimized_account_counts,
        'reassigned_account_count': reassigned_accounts,
        'original_red_health_count': original_scores['red_health_count'],
        'optimized_red_health_count': optimized_scores['red_health_count']
    }).fillna(0)  # Fill NaNs with 0 for CSMs that may not have assignments

    # Calculate changes
    summary_df['score_change'] = summary_df['optimized_total_score'] - summary_df['original_total_score']
    summary_df['account_count_change'] = summary_df['optimized_account_count'] - summary_df['original_account_count']
    summary_df['red_health_count_change'] = summary_df['optimized_red_health_count'] - summary_df['original_red_health_count']
    
    return summary_df

# Generate the summary analysis
summary_df = analyze_assignments(df, balanced_df)
print("Summary of Changes in Assignments:")
summary_df


# In[ ]:





# In[ ]:


import plotly.graph_objects as go
import pandas as pd

# Assuming optimized_df is your DataFrame
# Group the data by 'csm' and 'Health Segment' and count the occurrences
grouped_df = balanced_df[balanced_df['industry_size'] == '0-4 MT'].groupby(['new_csm_name', 'Health Segment']).size().reset_index(name='counts')

# Create a dictionary mapping 'Health Segment' values to colors
color_dict = {'Red': 'red', 'Green': 'green', 'Yellow': 'yellow'}

# Create a bar for each 'Health Segment'
bars = []
for health_segment in ['Red', 'Green', 'Yellow']:
    subset = grouped_df[grouped_df['Health Segment'] == health_segment]
    bars.append(go.Bar(name=health_segment, y=subset['new_csm_name'], x=subset['counts'], text=subset['counts'], textposition='auto', orientation='h', marker_color=color_dict[health_segment]))

# Create the figure with the bars
fig = go.Figure(data=bars)

# Change the bar mode to stack
fig.update_layout(
    barmode='stack', 
    yaxis={'categoryorder':'total ascending'},
    autosize=False,
    width=1000,
    height=800,
    margin=dict(
        l=50,  # left margin
        r=50,  # right margin
        b=100,  # bottom margin
        t=100,  # top margin
        pad=10
    ),
)

fig.show()


# In[ ]:


import plotly.graph_objects as go
import pandas as pd

# Assuming optimized_df is your DataFrame
# Group the data by 'csm' and 'Health Segment' and count the occurrences
grouped_df = balanced_df[balanced_df['industry_size'] == '5+ MT'].groupby(['new_csm_name', 'Health Segment']).size().reset_index(name='counts')

# Create a dictionary mapping 'Health Segment' values to colors
color_dict = {'Red': 'red', 'Green': 'green', 'Yellow': 'yellow'}

# Create a bar for each 'Health Segment'
bars = []
for health_segment in ['Red', 'Green', 'Yellow']:
    subset = grouped_df[grouped_df['Health Segment'] == health_segment]
    bars.append(go.Bar(name=health_segment, y=subset['new_csm_name'], x=subset['counts'], text=subset['counts'], textposition='auto', orientation='h', marker_color=color_dict[health_segment]))

# Create the figure with the bars
fig = go.Figure(data=bars)

# Change the bar mode to stack
fig.update_layout(
    barmode='stack', 
    yaxis={'categoryorder':'total ascending'},
    autosize=False,
    width=1000,
    height=800,
    margin=dict(
        l=50,  # left margin
        r=50,  # right margin
        b=100,  # bottom margin
        t=100,  # top margin
        pad=10
    ),
)

fig.show()


# In[ ]:


optimized_df['industry_size'].value_counts()


# In[ ]:





# In[ ]:


import plotly.graph_objects as go
import pandas as pd

# Assuming optimized_df is your DataFrame
# Group the data by 'csm' and 'Health Segment' and count the occurrences
grouped_df = balanced_df[balanced_df['new_csm_name'].isin(mt_count_lt_5_csms)].groupby(['new_csm_name', 'Health Segment']).size().reset_index(name='counts')

# Create a dictionary mapping 'Health Segment' values to colors
color_dict = {'Red': 'red', 'Green': 'green', 'Yellow': 'yellow'}

# Create a bar for each 'Health Segment'
bars = []
for health_segment in ['Red', 'Green', 'Yellow']:
    subset = grouped_df[grouped_df['Health Segment'] == health_segment]
    bars.append(go.Bar(name=health_segment, y=subset['new_csm_name'], x=subset['counts'], text=subset['counts'], textposition='auto', orientation='h', marker_color=color_dict[health_segment]))

# Create the figure with the bars
fig = go.Figure(data=bars)

# Change the bar mode to stack
fig.update_layout(
    barmode='stack', 
    yaxis={'categoryorder':'total ascending'},
    autosize=False,
    width=1000,
    height=800,
    margin=dict(
        l=50,  # left margin
        r=50,  # right margin
        b=100,  # bottom margin
        t=100,  # top margin
        pad=10
    ),
)

fig.show()


# In[ ]:





# In[ ]:





# In[ ]:


import plotly.graph_objects as go
import pandas as pd

# Assuming optimized_df is your DataFrame
# Group the data by 'csm' and 'Health Segment' and count the occurrences
grouped_df = balanced_df[balanced_df['new_csm_name'].isin(mt_count_gte_5_csms)].groupby(['new_csm_name', 'Health Segment']).size().reset_index(name='counts')

# Create a dictionary mapping 'Health Segment' values to colors
color_dict = {'Red': 'red', 'Green': 'green', 'Yellow': 'yellow'}

# Create a bar for each 'Health Segment'
bars = []
for health_segment in ['Red', 'Green', 'Yellow']:
    subset = grouped_df[grouped_df['Health Segment'] == health_segment]
    bars.append(go.Bar(name=health_segment, y=subset['new_csm_name'], x=subset['counts'], text=subset['counts'], textposition='auto', orientation='h', marker_color=color_dict[health_segment]))

# Create the figure with the bars
fig = go.Figure(data=bars)

# Change the bar mode to stack
fig.update_layout(
    barmode='stack', 
    yaxis={'categoryorder':'total ascending'},
    autosize=False,
    width=1000,
    height=800,
    margin=dict(
        l=50,  # left margin
        r=50,  # right margin
        b=100,  # bottom margin
        t=100,  # top margin
        pad=10
    ),
)

fig.show()


# In[ ]:


balanced_df[balanced_df['new_csm_name'].isin(mt_count_lt_5_csms)]['new_csm_name'].value_counts()


# In[ ]:


balanced_df[balanced_df['new_csm_name'].isin(mt_count_gte_5_csms)]['new_csm_name'].value_counts()


# In[ ]:





# In[ ]:


from datetime import datetime

# Get the current timestamp
current_timestamp = datetime.now()
timestamp_str = current_timestamp.strftime("%Y-%m-%d %H:%M:%S")


balanced_df.to_csv('balanced_df_resi_output_' + timestamp_str + '.csv')


# In[ ]:




full_file_name = run_for_Segment +  '_balanced_df_resi_output_' + timestamp_str + '.csv'

merge_input_df = pd.read_csv(csv_file)



merge_input_df.merge(balanced_df[['account_id', 'new_csm_name', 'csm_changed_calendar_date', 'previous_csm', 'csm_changed_last_6mnths']], how='left', 
                     left_on = 'Account ID', 
                     right_on = 'account_id').to_csv(full_file_name)


# In[ ]:


merge_input_df_final = merge_input_df.merge(balanced_df[['account_id', 'new_csm_name', 'csm_changed_calendar_date', 'previous_csm', 'csm_changed_last_6mnths', 'contacted_last_90_days']], how='left', 
                     left_on = 'Account ID', 
                     right_on = 'account_id')


# In[ ]:


merge_input_df_final['new_csm_name_y'].fillna(merge_input_df_final['new_csm_name_x'], inplace=True)


# In[ ]:


merge_input_df_final.to_csv(full_file_name)


# In[ ]:


# Cain Mitchell
# Kei Washington
# Jennifer Lam
# Kieran Cockburn


# In[ ]:


merge_input_df.shape


# In[ ]:


stoppp


# In[ ]:


# Balance Jeffs books of business





# In[ ]:


import pandas as pd


df = pd.read_csv('_[confidential] Resi_Corp_Nov_5th_Routing_with_BoB - Output.csv')

df['industry_size'] = np.where(df['MTs+MIs'] >= 5, '5+ MT', '0-4 MT')


# In[ ]:


df.shape


# In[ ]:


df = df[df['Segment'] == 'Residential']


# In[ ]:


df = df[~df['new_csm_name'].isna()]


# In[ ]:


df.head()


# In[ ]:


df.shape


# In[ ]:


mt_count_gte_5_csms = ['Krister Karlsson',
'Vahagn Yaralian',
'Hrag Jinbashian',
'Armen Mardirossian',
'Elen Ghumashyan',
'Anna Hayrapetyan',
'Gor Shahbazyan',
'Kevin Martin',
'Adrienne Olzewski',
'Bernie Kosloski',
'Syd Croft',
'Alla Poghosyan',
'Davit Mkrtchyan',
'Alex Forston',
'Alexa Castro',
'Serro Park',
'Kyle Shinmoto',
'Alejandra Zuluaga',
'Alex Janssens',
'Esteban De La Riva',
'Nicole Moore',
'Cain Mitchell',
'Kei Washington',
'Jennifer Lam',
'Kieran Cockburn']

len(mt_count_gte_5_csms)


# In[ ]:


import numpy as np
# Departing CSM
departing_csm = 'Jeff Braxton'

# Filter accounts of the departing CSM
departing_accounts = df[df['new_csm_name'] == departing_csm]

# Remaining CSMs
remaining_csms = mt_count_gte_5_csms #df[ (df['new_csm_name'] != departing_csm) ]['new_csm_name'].unique()

# Add columns for health score counts
df['red_count'] = (df['Health Segment'] == 'Red').astype(int)
df['yellow_count'] = (df['Health Segment'] == 'Yellow').astype(int)
df['green_count'] = (df['Health Segment'] == 'Green').astype(int)

# Define cost function
def calculate_cost(df):
    grouped = df.groupby('new_csm').agg({
        'Neediness Score': 'mean',
        'red_count': 'sum',
        'yellow_count': 'sum',
        'green_count': 'sum'
    })
    neediness_variance = grouped['Neediness Score'].var()
    red_variance = grouped['red_count'].var()
    yellow_variance = grouped['yellow_count'].var()
    green_variance = grouped['green_count'].var()
    return neediness_variance, red_variance, yellow_variance, green_variance

# Initialize best cost and best assignment
best_cost = (float('inf'), float('inf'), float('inf'), float('inf'))
best_assignment = None

# Perform iterations for neediness variance
for _ in range(100):
    df['new_csm'] = df['new_csm_name']
    for account_id in departing_accounts['account_id']:
        new_csm = np.random.choice(remaining_csms)
        df.loc[df['account_id'] == account_id, 'new_csm'] = new_csm
    cost = calculate_cost(df)
    if cost[0] < best_cost[0]:
        best_cost = cost
        best_assignment = df['new_csm'].copy()

# Apply the best assignment for neediness variance
df['new_csm'] = best_assignment

# Perform iterations for red variance
for _ in range(100):
    df['new_csm'] = df['new_csm_name']
    for account_id in departing_accounts['account_id']:
        new_csm = np.random.choice(remaining_csms)
        df.loc[df['account_id'] == account_id, 'new_csm'] = new_csm
    cost = calculate_cost(df)
    if cost[1] < best_cost[1]:
        best_cost = cost
        best_assignment = df['new_csm'].copy()

# Apply the best assignment for red variance
df['new_csm'] = best_assignment

# Perform iterations for yellow variance
for _ in range(100):
    df['new_csm'] = df['new_csm_name']
    for account_id in departing_accounts['account_id']:
        new_csm = np.random.choice(remaining_csms)
        df.loc[df['account_id'] == account_id, 'new_csm'] = new_csm
    cost = calculate_cost(df)
    if cost[2] < best_cost[2]:
        best_cost = cost
        best_assignment = df['new_csm'].copy()

# Apply the best assignment for yellow variance
df['new_csm'] = best_assignment

# Perform iterations for green variance
for _ in range(100):
    df['new_csm'] = df['new_csm_name']
    for account_id in departing_accounts['account_id']:
        new_csm = np.random.choice(remaining_csms)
        df.loc[df['account_id'] == account_id, 'new_csm'] = new_csm
    cost = calculate_cost(df)
    if cost[3] < best_cost[3]:
        best_cost = cost
        best_assignment = df['new_csm'].copy()

# Apply the best assignment for green variance
df['new_csm'] = best_assignment



# In[ ]:


updated_csm_df = df[[
    'account_id',
    'Health Segment',
    'new_csm_name',
    'new_csm'
]]


# In[ ]:


updated_csm_df[updated_csm_df['new_csm_name'] != updated_csm_df['new_csm']]#.to_csv('new_output_resi_corp_after_jeff_braxton_balancing.csv')


# In[ ]:


updated_csm_df[updated_csm_df['new_csm_name'] != updated_csm_df['new_csm']]['new_csm'].value_counts()


# In[ ]:


updated_csm_df[updated_csm_df['new_csm_name'] != updated_csm_df['new_csm']].groupby('Health Segment')['new_csm'].value_counts()


# In[ ]:


call_activity_last_90_days = pd.read_csv('VW_GAINSIGHT_CSM_ACTIVITY_last_90_days_Nov_14.csv')
call_activity_last_90_days.columns = call_activity_last_90_days.columns.str.lower()


# In[ ]:


new_df = pd.merge(df, call_activity_last_90_days, on = 'account_id', how = 'left')


# In[ ]:


new_df['contacted_last_90_days'].value_counts()


# In[ ]:





# In[ ]:


new_df[new_df['industry_size'] == '0-4 MT']['contacted_last_90_days'].value_counts()


# In[ ]:


new_df[new_df['industry_size'] == '5+ MT']['contacted_last_90_days'].value_counts()


# In[ ]:


new_df[new_df['Preferred CSM Name'] != new_df['new_csm_name']]['contacted_last_90_days'].value_counts()


# In[ ]:


new_df[new_df['Health Segment'] == 'Red']['contacted_last_90_days'].value_counts()


# In[ ]:


new_df[new_df['Health Segment'] == 'Yellow']['contacted_last_90_days'].value_counts()


# In[ ]:


# industry_size  Health Segment
# 0-4 MT         Green              527
#                Red                175
#                Yellow             470
# 5+ MT          Green             2007
#                Red                 98
#                Yellow             447
# dtype: int64


# In[ ]:



for csm in new_df['Preferred CSM Name'].unique():
    print()
    print(csm)
    print(new_df[new_df['Preferred CSM Name'] == csm]['contacted_last_90_days'].value_counts())


# In[ ]:


new_df['new_csm_name'].value_counts()


# In[ ]:





# In[ ]:





# In[ ]:


#new_df.loc[new_df['contacted_last_90_days'] == True, 'new_csm_name'] = new_df['Preferred CSM Name']


# In[ ]:


new_df['new_csm_name'].value_counts()


# In[ ]:


optimized_df['contacted_last_90_days'].value_counts()


# In[ ]:


optimized_df[optimized_df['csm_name'] != optimized_df['new_csm_name']]['contacted_last_90_days'].value_counts()


# In[ ]:


optimized_df.shape


# In[ ]:





# In[ ]:


merge_input_df_final


# In[ ]:


merge_input_df_final[merge_input_df_final['Responsible CSM'] != merge_input_df_final['new_csm_name_y']]['contacted_last_90_days'].value_counts()



# In[ ]:





# In[ ]:


optimized_df.to_csv('Resi_corp_new_csms_output_to_Brandon_Chu.csv')


# In[ ]:





# In[ ]:


# stitch input and output file


inp_df = pd.read_csv(csv_file)
out_df = pd.read_csv('Resi_corp_new_csms_output_to_Brandon_Chu.csv')
out_df.rename({'account_id':'ACCOUNT_ID'}, axis=1, inplace=True)


# In[ ]:


inp_df = inp_df.merge(out_df[['ACCOUNT_ID', 'new_csm_name']], how='left', on = 'ACCOUNT_ID')

inp_df['new_csm_name'] = inp_df['new_csm_name'].fillna(inp_df['UPDATED CSM'])

inp_df.to_csv('Resi_corp_new_csms_output_to_Brandon_Chu_v2.csv')


# In[ ]:


inp_df.shape


# In[ ]:


out_df['account_to_keep'].value_counts()


# In[ ]:


inp_df[inp_df['ACCOUNT_ID'] == '0011a00000hEepbAAC']


# In[ ]:


validate_df = pd.read_csv('Resi_corp_new_csms_output_to_Brandon_Chu_v2.csv')


# In[ ]:





# In[ ]:





# In[ ]:


inp_df['UPDATED CSM'].value_counts()


# In[ ]:


inp_df['new_csm_name'].value_counts()


# In[ ]:





# In[ ]:





# In[ ]:





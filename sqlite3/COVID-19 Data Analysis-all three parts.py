#!/usr/bin/env python
# coding: utf-8

# <h2>COVID-19 Data Analysis</h2>

# In[1]:


import sqlite3
import numpy as np
import pandas as pd


# <p>Data file is in CSV format.</p>
# <p>File size is 9.21 GB.</p>
# <p>Since this file is too large, below, I count the number of lines and split the file into multiple files.</p>

# In[2]:


f = open("COVID-19_Case_Surveillance_Public_Use_Data_with_Geography.csv", "r")


# In[3]:


for count, line in enumerate(f):
    pass


# In[4]:


print(count)


# In[5]:


chunk_size = 20000000


# In[6]:


def write_chunk(part, lines):
    with open("data_part_" + str(part) + ".csv", "w") as f_out:
        f_out.write(header)
        f_out.writelines(lines)


# In[7]:


with open("COVID-19_Case_Surveillance_Public_Use_Data_with_Geography.csv", "r") as f:
    count = 0
    header = f.readline()
    lines = []
    for line in f:
        count += 1
        lines.append(line)
        if count % chunk_size == 0:
            write_chunk(count // chunk_size, lines)
            lines = []
    # write remainder
    if len(lines) > 0:
        write_chunk((count // chunk_size) + 1, lines)


# In[8]:


f.close()


# <h3>Using SQL to store large dataset</h3>
# <ul>
#     <li>Read in each CSV data-part into a DataFrame</li>
#     <li>Then, export each part to an SQL database</li>
# </ul>

# In[9]:


# Create SQL Engine, Connection, and Cursor
# If .db file does not exist, it will be created during connection
connection = sqlite3.connect('covid_large_dataset.db')
cursor = connection.cursor()


# In[10]:


# Create table in database, if it does not exist
command1 = """CREATE TABLE IF NOT EXISTS covid_data(id INTEGER PRIMARY KEY, case_month TEXT, res_state TEXT, 
            state_fips_code TEXT, res_county TEXT, county_fips_code TEXT, age_group TEXT, sex TEXT, race TEXT, 
            ethnicity TEXT, case_positive_specimen_interval INTEGER, case_onset_interval INTEGER, process TEXT, 
            exposure_yn TEXT, current_status TEXT, symptom_status TEXT, hosp_yn TEXT, icu_yn TEXT, death_yn TEXT, 
            underlying_conditions_yn TEXT)"""
cursor.execute(command1)
connection.commit()


# In[11]:


# Read in first CSV part and export to SQL table in database
df = pd.read_csv("data_part_1.csv", low_memory=False)
df.to_sql('covid_data', connection, if_exists='append', index_label='id')
connection.commit()


# In[12]:


# Read in second CSV part and export to SQL table in database
df = pd.read_csv("data_part_2.csv", low_memory=False)

# The first CSV part had an index (id) of range 0 to 19,999,999
# Therefore, we must reindex this part to the next range, 20,000,000 to 39,999,999
# Otherwise, we would get a unique index error when we try to export to SQL table
df.index = range(20000000, 40000000)

# Export data to SQL table in database
df.to_sql('covid_data', connection, if_exists='append', index_label='id')
connection.commit()


# In[13]:


# Read in third CSV part, reindex to range starting with 40,000,000, 
# and export data to SQL table in database
df = pd.read_csv("data_part_3.csv", low_memory=False)
df.index = range(40000000, 60000000)
df.to_sql('covid_data', connection, if_exists='append', index_label='id')
connection.commit()


# In[14]:


# Read in fourth CSV part. Get info to check number of lines. 
df = pd.read_csv("data_part_4.csv", low_memory=False)
df.info()


# In[15]:


# Then, reindex to range starting with 60,000,000, 
# and export data to SQL table in database
df.index = range(60000000, (60000000+11387132))
df.to_sql('covid_data', connection, if_exists='append', index_label='id')
connection.commit()


# In[16]:


# Close connection to database
connection.close()


# <h3>Open SQL database and read in data to DataFrame for data analysis</h3>

# In[3]:


# Create SQL Engine, Connection, and Cursor
connection = sqlite3.connect('covid_large_dataset.db')
cursor = connection.cursor()


# In[18]:


# SQL command to read data from table in database
command1 = """SELECT id,
                     case_month,
                     res_state,
                     age_group,
                     sex,
                     race,
                     ethnicity,
                     case_positive_specimen_interval,
                     case_onset_interval,
                     death_yn
                FROM covid_data
               WHERE death_yn = 'Yes';"""

# Execute command and read into DataFrame
df = pd.read_sql(sql=command1, con=connection, index_col="id")


# In[19]:


# Get DataFrame info
df.info()


# In[20]:


# Find how many covid deaths per state
deaths_by_state = df.groupby(["res_state"]).size()
print(deaths_by_state)


# In[21]:


# Export DataFrame to csv file for use later.
df.to_csv("covid-data-with-deaths.csv")


# <h2>Data Analysis using SQLite3</h2>
# <p>Count number of cases, per state, by month+year</p>

# In[7]:


command1 =  """
            SELECT DISTINCT
                case_month,
                res_state,
                count(res_state) AS state_total
            FROM
                covid_data
            WHERE
                case_month IS NOT NULL AND res_state IS NOT NULL
            GROUP BY
                case_month, res_state
            ORDER BY
                case_month, res_state;
            """


# In[3]:


# Create SQL Engine, Connection, and Cursor
connection = sqlite3.connect('covid_large_dataset.db')
cursor = connection.cursor()


# In[8]:


df = pd.read_sql(sql=command1, con=connection)


# In[9]:


df.head()


# In[10]:


df.info()


# In[11]:


df.to_csv('covid-cases-by-date-and-state.csv')


# In[13]:


dates = df['case_month'].value_counts()


# In[14]:


states = df['res_state'].value_counts()


# In[18]:


type(dates)


# In[23]:


dates.sort_index(inplace=True)


# In[24]:


states.sort_index(inplace=True)


# In[29]:


dates_list = list(dates.index)


# In[30]:


states_list = list(states.index)          
    


# In[ ]:





# In[76]:


df2 = pd.DataFrame(index=states_list, columns=dates_list)

for index, row in df.iterrows():
    state = row["res_state"]
    date = row["case_month"]
    total = row["state_total"]
    df2[date][state] = total


# In[ ]:





# In[80]:


df2.head(10)


# In[79]:


df.to_csv('final.csv')


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pyodbc
import csv
from datetime import datetime, timedelta


# In[2]:


sharkfile = r'c:\data\GSAF5 (2).csv'


# In[3]:


conn = pyodbc.connect('DSN=kubricksql;UID=DE14;PWD=password')


# In[4]:


cur = conn.cursor()


# In[9]:


data = zip(attack_dates, case_number, country, activity, age, gender, isfatal)


# In[6]:


attack_dates = []
case_number = []
country = []
activity = []
age = []
gender = []
isfatal = []

with open(sharkfile, encoding='utf-8') as f:
    reader = csv.DictReader(f)
    for row in reader:
        attack_dates.append(row['Date'])
        case_number.append(row['Case Number'])
        country.append(row['Country'])
        activity.append(row['Activity'])
        age.append(row['Age'])
        gender.append(row['Sex '])
        isfatal.append(row['Fatal (Y/N)'])


# In[12]:


cur.execute('truncate table kater.shark')


# In[10]:


#q = 'select * from kater.shark'
q = 'insert into kater.shark(attack_date, case_number, country, activity, age, gender, isfatal) values (?, ?, ?, ?, ?, ?, ?)'
#p = ['2019-10-30', 'dummy123', 'England', 'snorkling', 41, 'M', 1]


# In[13]:


#data = cur.execute(q)
#for row in data:
    #print(row)
for d in data:
    try:
        cur.execute(q, d)
        conn.commit()
    except:
        conn.rollback()


# In[ ]:





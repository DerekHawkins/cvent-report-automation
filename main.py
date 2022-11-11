import pandas as pd
from tqdm.notebook import tqdm
from time import sleep
import datetime
import requests
import base64
import time
import os
import json
import googleanalytics as ga
import gspread_dataframe as gd
import gspread

df_customer_list = pd.read_excel('customer_list.xlsx')
df_customer_list = df_customer_list[['Account Owner', 'Account Name']]
customers = df_customer_list['Account Name'].to_list()

employee_list = pd.read_excel('Marketing_(Domino_Data_Lab) (2).xlsx')

employee_list['Full Name'] = [f"{employee_list['First Name'][i]} {employee_list['Last Name'][i]}" for i in range(employee_list.shape[0])]
employee_list = employee_list[['Full Name', 'Department']].rename(columns={'Full Name':'Employee (if channel=employee sharing)'})


credentials = {'client_id':'',
              'client_secret':''}

encodedData = base64.b64encode(bytes(f"{credentials['client_id']}:{credentials['client_secret']}", "ISO-8859-1")).decode("ascii")
authorization_header_string = f"Authorization: Basic {encodedData}"

# Authentication of Cvent API
url = 'https://api-platform.cvent.com/ea/oauth2/token'
headers = {'content-type': 'application/x-www-form-urlencoded', 
           'Authorization': f'Basic {encodedData}'}

params = {
"grant_type":"client_credentials",
"client_id":f'{credentials["client_id"]}'
}
token = requests.post(url, params=params, headers=headers).json()['access_token']

event_id = ''


call_headers = {
    'Accept':'application/json', 
    'x-api-key':f'{credentials["client_id"]}',
    'Authorization': f"Bearer {token}"
}


r = requests.get(f'https://api-platform.cvent.com/ea/attendees?sort=registeredAt:DESC', headers=call_headers)
frame = []
for crawl in r.json()['data']:
    _id = crawl['id']
    full_name = f"{crawl['contact']['firstName']} {crawl['contact']['lastName']}"
    email_address = crawl['contact']['email']
    try:
        company_name = crawl['contact']['company']
    except KeyError as e: 
        company_name = 'N/A'
    
    title = crawl['contact']['title']
    registration_type = crawl['registrationType']['name']
    registered_at = crawl['registeredAt']
    
    cvent_data = {'id':_id,
                  'event_id':crawl['event']['id'],
                  'full_name':full_name,
                  'email_address':email_address,
                  'company_name':company_name,
                  'title':title, 
                  'registration_type':registration_type,
                  'registeredAt':registered_at}
    frame.append(cvent_data)
    
    
cvent_df = pd.DataFrame(frame)

cvent_main = cvent_df[(cvent_df['event_id']==event_id) & \
         ~(cvent_df['email_address'].str.contains('|'.join(['dominodatalab', '']))) & \
        ~(cvent_df['company_name'].str.contains('|'.join(['Test', 'Domino'])))].reset_index(drop=True).drop_duplicates()

try:
    if os.path.exists('credentials.json'):
        credentials = json.load(open('credentials.json'))
    else:
        # authorize your code to access the Google Analytics API
        # (this will be interactive, as you'll need to confirm
        # in a browser window)
        credentials = ga.authorize()
        # turn the credentials object into a plain dictionary
        credentials = credentials.serialize()
        json.dump(credentials, open('credentials.json', 'w'))

    account = ga.authenticate(**credentials)
except:
    credentials = ga.authorize()
    # turn the credentials object into a plain dictionary
    credentials = credentials.serialize()
    json.dump(credentials, open('credentials.json', 'w'))
    account = ga.authenticate(**credentials)

profile = account[0].webproperties[0].profiles[6]
data = profile.core.query.metrics('ga:uniqueEvents')\
.dimensions('ga:eventAction', 'ga:eventLabel', 'ga:source', 'ga:medium').range('2022-01-24',
                                                                               (datetime.datetime.today().date()).strftime('%Y-%m-%d'))
df_ga = pd.DataFrame(data.rows)
df_ga_reg = df_ga[df_ga['event_action'].str.contains('@')].rename(columns={'event_action':'email_address'})


cvent_ga_merge = df_ga_reg.merge(cvent_main, on='email_address', how='outer')

# dictionary for correcting misspelled company names
company_rewrite = {'BMS':'Bristol-Myers Squibb'}

cvent_ga_merge = cvent_ga_merge.replace({"company_name": company_rewrite})

cvent_ga_merge['registeredAt'] = pd.to_datetime(cvent_ga_merge['registeredAt'])
cvent_ga_merge['date_check'] = cvent_ga_merge['registeredAt'].apply(lambda x: x.strftime('%Y-%m-%d'))
cvent_ga_merge = cvent_ga_merge.sort_values(by='registeredAt', ascending=False)

reg_today = cvent_ga_merge[cvent_ga_merge['date_check']==(datetime.datetime.today().date()).strftime('%Y-%m-%d')].reset_index(drop=True)
reg_today = reg_today[['full_name', 'email_address', 'company_name', 'source', 'medium','registeredAt', 'title', 'registration_type']]

reg_today = reg_today.rename(columns={'full_name':'Full Name', 
                                      'email_address':'Email Address', 
                                      'company_name':'Company Name', 
                                     'source':'Channel', 'medium':'Employee (if channel=employee sharing)', 
                                     'registeredAt':'Last Registration Date (GMT)', 'title':'Title', 
                                     'registration_type':'Registration Type'})

reg_today['Channel'] = reg_today['Channel'].astype(str).apply(lambda x: x.replace('_', ' ')).str.title()
reg_today['Employee (if channel=employee sharing)'] = reg_today['Employee (if channel=employee sharing)'].astype(str).apply(lambda x: x.replace('_', ' ')).str.title()

reg_today['Channel'] = reg_today['Channel'].apply(lambda tag: 'Employee Sharing' if tag == 'Employee Sharing' else 'Marketing')
reg_today['Full Name'] = reg_today['Full Name'].str.title()

results = []
for check in reg_today['Company Name']:
    if check in customers:
        response = 'Customer'
    elif check in ['...']:
        response = 'Partner'
    else:
        response = 'Prospect'
    results.append(response)
    
reg_today['Customer/Prospect/Partner'] = results

def substring(str1):
    char_to_replace = {',':'', 'Inc.':'', ', Inc.':'', '& Co':''}
    for key, value in char_to_replace.items():
        # Replace key character with value character in string
        str1 = str1.replace(key, value)
    return str1

open_deals = pd.read_excel('stage_1_up_deals.xlsx')
open_deals = open_deals.sort_values(by='Associated Company', ascending=True)

open_deals['Associated Company'] = open_deals['Associated Company'].apply(lambda s: substring(s).strip())

open_deals = open_deals[['Associated Company','Deal Stage']].reset_index(drop=True).drop_duplicates()
open_deals = open_deals.rename(columns={'Associated Company':'Company Name'})

od_replace = {'Alcon Vision LLC':'Alcon', 'American Express Company':'American Express',
'Credit Suisse Group AG':'Credit Suisee', 'Intel Corporation':'Intel'}

open_deals = open_deals.replace({'Company Name':od_replace})

gc = gspread.service_account(filename='/Users/derekhawkins/Documents/Temp Creds/service_account.json')
ws = gc.open("Rev Reg Customer/Prospect Meeting List").worksheet("Customer/Prospect")
existing = gd.get_as_dataframe(ws)
updated = existing.append(reg_today).drop_duplicates(subset='Email Address')
updated['Last Registration Date (GMT)'] = pd.to_datetime(updated['Last Registration Date (GMT)'], utc=True)
updated = updated.sort_values(by='Last Registration Date (GMT)', ascending=False)
updated = updated.merge(employee_list, on='Employee (if channel=employee sharing)', how='left')

updated = updated.merge(open_deals, on='Company Name', how='left')

updated = updated.rename(columns={'Department_x':'Department', 'Deal Stage_y':'Deal Stage'})[['Full Name',
                                                                                              'Email Address', 'Company Name', 'Employee (if channel=employee sharing)',
                                                                                              'Last Registration Date (GMT)', 
                                                                                              'Title', 
                                                                                              'Registration Type', 'Customer/Prospect/Partner', 'Deal Stage', 'Tier', 'Booked Meeting', 
                                                                                              'Invite to VIP Dinner?', 'Q3', 'Q4']].drop_duplicates(subset=['Full Name', 'Email Address']).sort_values(by='Company Name', ascending=True)

gd.set_with_dataframe(ws, updated)

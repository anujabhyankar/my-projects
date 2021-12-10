#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import argparse
import json
from ibm_watson import AssistantV1 as WatsonAssistant
from ibm_cloud_sdk_core.authenticators import IAMAuthenticator
from urllib.parse import urlparse, parse_qs
import os
import sys
from bs4 import BeautifulSoup

import argparse
msg = """##### Date format is YYYY-MM-DD
##### TO_DATE should not be same or earlier than FROM_DATE.

##### Since, we are not using timestamps, TO_DATE is exclusive of the duration for which logs will be pulled. 
##### e.g. FROM_DATE = '2021-04-01' and TO_DATE = '2021-04-02' will pull conversation logs for 1 April 2021.
##### e.g. FROM_DATE = '2021-05-01' and TO_DATE = '2021-05-04' will pull conversation logs for 1, 2, 3 May 2021."""

parser = argparse.ArgumentParser(description = msg)
parser.add_argument('from_date', type=str, help='The date from which data is to be pulled. [inclusive]')
parser.add_argument('to_date', type=str, help='The date to which data is to be pulled. [non-inclusive]')
args = parser.parse_args()


FROM_DATE = args.from_date
TO_DATE = args.to_date


C_DEPLOYMENT = 'DEPLOYMENT'
C_ASSISTANT = 'ASSISTANT'
C_WORKSPACE = 'WORKSPACE'
C_CSV = 'CSV'
C_TSV = 'TSV'
C_XLSX = 'XLSX'
C_JSON = 'JSON'

c_RESPONSE = 'response'
c_CONTEXT = 'context'
c_SYSTEM = 'system'
c_INPUT = 'input'
c_OUTPUT = 'output'
c_INTENTS = 'intents'
c_INTENT = 'intent'
c_TEXT = 'text'
c_BRANCH_EXITED_REASON = 'branch_exited_reason'
c_LOG_MESSAGING = 'log_messaging'
c_CONFIDENCE = 'confidence'
c_LOGS = 'logs'
c_PAGINATION = 'pagination'
c_NEXT_URL = 'next_url'
c_CURSOR = 'cursor'
c_GENERIC = 'generic'
c_TITLE = 'title'
c_OPTIONS = 'options'
c_LABEL = 'label'
c_RESPONSE_TYPE = 'response_type'
c_ASSOCIATE_INFO = 'associateInfo'
c_COMPANY_NAME = 'companyName'
c_ERROR = 'error'
c_REQUEST_TIMESTAMP = 'request_timestamp'


# If you want to hard code your main defaults.
default_version = '2020-04-01'
default_url = 'https://gateway.watsonplatform.net/assistant/api'
default_logtype = C_WORKSPACE
default_language = 'en'


## PROD
apikey = 'DVKhYQXNCkFfv-kNg8vtLNyOsccnHtfXW-PFvREzghze'
id = 'd1af8d01-0b6e-4152-9c08-baec4d3b9861' # workspace/skill id
url = 'https://api.us-east.assistant.watson.cloud.ibm.com'

logtype = C_WORKSPACE
filetype = C_CSV
# filetype = C_JSON
language = 'en'
version = '2020-04-01'

totalpages = 999
pagelimit = 200

logs_from = FROM_DATE
logs_to = TO_DATE
filter = f'language::{language},workspace_id::{id},response_timestamp>={logs_from},response_timestamp<{logs_to}'

strip = False
save_path = os.path.join('exported_sheets', 'raw')
file_name = f'acubed_conversations.{logs_from}_to_{logs_to}.csv'
filename = os.path.join(save_path, file_name)


f_request_timestamp = 'Request_timestamp'
f_user_input = 'User_Input'
f_output = 'Response'
f_intent = 'Actual_Intent'
f_confidence = 'Confidence'
f_company_name = 'Company_Name'
f_conversation_id = 'Conversation Id'

columns = [
    f_user_input, f_intent, f_confidence, f_output, f_company_name, f_conversation_id, f_request_timestamp
]



# Saving methods.
def save_json(data=None,file_name=None):
    with open(file_name, 'w') as out:
        json.dump(data,out)


def save_xsv(data=None, sep=',', file_name=None):
    df = convert_json_to_dataframe(data)
    if df is not None:
        df.to_csv(filename,encoding='utf8',sep=sep,index=False)


def save_xlsx(data=None, file_name=None):
    df = convert_json_to_dataframe(data)
    if df is not None:
        df.to_excel(filename,index=False)


def convert_json_to_dataframe(data=None):
    rows = []

    if data == [[]]:
        print('No Logs found. :(')
        return None

    for data_records in data:
        for o in data_records:
           # print("data_records" data_records)
              print(o)
            row = {}
            
            # Let's shorthand the response and system object.
            r = o[c_RESPONSE]
            s = r[c_CONTEXT][c_SYSTEM]
                
            row[f_request_timestamp] = o[c_REQUEST_TIMESTAMP]

            if 'associateInfo' in r['context']:
                row[f_company_name] = r['context']['associateInfo']['companyName']
            else:
                row[f_company_name] = None
            
            if c_TEXT in r[c_INPUT]:
                row[f_user_input] = r[c_INPUT][c_TEXT]

            if c_TEXT in r[c_OUTPUT]:
                row[f_output] = ' '.join(r[c_OUTPUT][c_TEXT])
                if strip:
                    row[f_output] = row[f_output].replace('\l','').replace('\n','').replace('\r','')

            if not r[c_OUTPUT][c_TEXT]:
                if r[c_OUTPUT][c_GENERIC]:
                    if r[c_OUTPUT][c_GENERIC][0][c_RESPONSE_TYPE] == 'option':
                        options = ''
                        for x in range (0, len(r[c_OUTPUT][c_GENERIC][0][c_OPTIONS])):
                            options += ' <br/>'
                            options += r[c_OUTPUT][c_GENERIC][0][c_OPTIONS][x][c_LABEL]
                        row[f_output] = r[c_OUTPUT][c_GENERIC][0][c_TITLE] + ' <br/> ' + 'Options:' + options
                    elif r[c_OUTPUT][c_GENERIC][0][c_RESPONSE_TYPE] == 'text':
                        row[f_output] = r[c_OUTPUT][c_GENERIC][0][c_TEXT]
                    else:
                        row[f_output] = 'ERROR: No Output Available'
                else:
                    if 'error' in r[c_OUTPUT]:
                        if r[c_OUTPUT][c_ERROR]:
                            row[f_output] = r[c_OUTPUT][c_ERROR]
                        else:
                            row[f_output] = 'ERROR: No Output Available'
                    else:
                        row[f_output] = 'ERROR: No Output Available'
            
            if len(r[c_INTENTS]) > 2:
                if r[c_INTENTS][0][c_CONFIDENCE] < 0.3:
                    row[f_confidence] = r[c_INTENTS][0][c_CONFIDENCE], r[c_INTENTS][1][c_CONFIDENCE], r[c_INTENTS][2][c_CONFIDENCE]
                    row[f_intent] = r[c_INTENTS][0][c_INTENT], r[c_INTENTS][1][c_INTENT], r[c_INTENTS][2][c_INTENT]
                else:
                    row[f_confidence] = r[c_INTENTS][0][c_CONFIDENCE]
                    row[f_intent] = r[c_INTENTS][0][c_INTENT]
                    
            if len(r[c_INTENTS]) > 0 and len(r[c_INTENTS]) < 3:
                row[f_confidence] = r[c_INTENTS][0][c_CONFIDENCE]
                row[f_intent] = r[c_INTENTS][0][c_INTENT]

            
            rows.append(row)
            
# Build the dataframe. 
    df = pd.DataFrame(rows,columns=columns)
    
# cleaning up dataframe. Removing NaN and converting date fields. 
    df = df.fillna('')
    
# Prevent timezone limitation in to_excel call.
    if filetype != C_XLSX:
        df[f_request_timestamp] = pd.to_datetime(df[f_request_timestamp])
        
# Lastly sort by conversation ID, and then request, so that the logs become readable. 
    df = df.sort_values([f_request_timestamp], ascending=[True])

    return df


# Make connection to Watson Assistant.
authenticator = IAMAuthenticator(apikey)
c = WatsonAssistant(version=version, authenticator=authenticator)
c.set_service_url(url)



# Determine how logs will be pulled.
#logtype = None
pull_filter = None

if filter is None:
    logtype = logtype.upper()
    if logtype == C_WORKSPACE:
        logtype = 'workspace_id'
    elif logtype == C_ASSISTANT:
        logtype = 'request.context.system.assistant_id'
    elif logtype == C_DEPLOYMENT:
        logtype = 'request.context.metadata.deployment'
    else:
        print("Error: I don't understand logtype {}. Exiting.".format(logtype))
        exit(1)

    print(f'Reading {logtype} using ID {id}.')
    pull_filter = 'language::{},{}::{}'.format(language, logtype, id)
else:
    print(f'Reading using filter: {filter}')
    pull_filter = filter



# Download the logs.
j = []
page_count = 1
cursor = None
count = 0

x = { c_PAGINATION: 'DUMMY' }
while x[c_PAGINATION]:
    if page_count > totalpages: 
        break

    print('Reading page {}.'.format(page_count))
    x = c.list_all_logs(filter=pull_filter,cursor=cursor,page_limit=pagelimit).result
    
    j.append(x[c_LOGS])
    count = count + len(x[c_LOGS])

    page_count = page_count + 1

    if c_PAGINATION in x and c_NEXT_URL in x[c_PAGINATION]:
        p = x[c_PAGINATION][c_NEXT_URL]
        u = urlparse(p)
        query = parse_qs(u.query)
        cursor = query[c_CURSOR][0]
    
# Determine how the file should be saved.
filetype = filetype.upper()
if filetype == C_CSV:
    save_xsv(data=j, sep=',',file_name=filename)
elif filetype == C_TSV:
    save_xsv(data=j, sep='\t',file_name=filename)
elif filetype == C_XLSX:
    save_xlsx(data=j, file_name=filename)
else:
    save_json(data=j, file_name=filename),
 
print(f'Writing {count} records to: {filename} as file type: {filetype}')
print(f'Complete.')
print(f'Starting cleanup.')



df = pd.read_csv(filename)
df = df.dropna(axis=0, subset=['Confidence'])
# Remove special chars and html tags
df['User_Input'] = df['User_Input'].replace(to_replace=[r"\\t|\\n|\\r", "\t|\n|\r"], value=["",""], regex=True)
df['Response'] = df['Response'].apply(lambda x: BeautifulSoup(x, 'html.parser').get_text())
df['Response'] = df['Response'].replace(to_replace=[r"\\t|\\n|\\r", "\t|\n|\r"], value=["",""], regex=True)
# sorting alphabetically
df.sort_values("User_Input", inplace=True)
# For cleaned up values
df2 = df.copy()
# dropping duplicate values
df.drop_duplicates(subset ="User_Input", keep = 'first', inplace = True)
# Replace null values in response with text
df['Response'].fillna("NO response. Watson provided Options to choose.", inplace = True)
# removing common unigrams
common_words = ['hi', 'hello', 'yes', 'no', '👍', '👎', '?', '??', 'thank you', 'thanks', 'bye', 'goodbye']
df = (df[~df.User_Input.str.lower().isin(common_words)])

# Replace null values in response with text
df['Response'].fillna("NO response. Watson provided Options to choose.", inplace = True)

print(f'Removed null values, duplicates, common words. Cleanup Complete.')
print(f'Total number of rows exported: {df.shape[0]}')

df2 = pd.concat([df, df2])
df2.drop_duplicates(keep=False, inplace = True)

export_path_processed = os.path.join('exported_sheets', 'output', f'processed_{file_name}')
export_path_cleaned = os.path.join('exported_sheets', 'output', f'cleaned_{file_name}')
df.to_csv(export_path_processed, index=False, encoding='utf-8')
df2.to_csv(export_path_cleaned, index=False, encoding='utf-8')

print(f'Processed file saved at: {export_path_processed}')
print(f'Cleaned file saved at: {export_path_cleaned}')


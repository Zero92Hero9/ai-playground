
#ServerOperationError: [UNRESOLVED_ROUTINE] Cannot resolve function `ai_query` on search path [`system`.`builtin`, `system`.`session`, `hive_metastore`.`default`].; line 7 pos 15

from databricks import sql
import sys
from itertools import islice
from databricks.sql.parameters import StringParameter
import numpy as np

#command line arguments
arguments = sys.argv[1:]

if len(arguments) == 0:
    print('Usage: python2 case_classifier.py --inputFilePath filePath --tableName tableName')
file_path = sys.argv[1]
table_name = sys.argv[2]

driver = 'Driver=/Library/simba/spark/lib/libsparkodbc_sb64-universal.dylib'
host_name = 'ticketmaster-data-science.cloud.databricks.com'
http_path = '/sql/1.0/warehouses/bb5c579cf587b4d4'
access_token = ''
case_ids_file_path = 'dbfs:/FileStore/case_samples.txt'
endpoint_model_name = 'databricks-meta-llama-3-70b-instruct'
prepend_prompt = '''
    Classify the content for the support case using into the best choice of the following categories:

                - VIP Customer list Report
                - Audit Rep Report
                - REPGEN Report
                - Method of Payment Report
                - Customer List Report
                - Audit Report
                - Reporting Other
                - Account Manager password reset
                - TM1 Permissions password reset
                - Host password reset
                - Archtics password reset
                - Microflex password reset
                - Add or delete client account user
                - User Management Other

            Pick only one of these categories and return nothing else but the category.

            Support case content:
'''
append_prompt = ' '
case_query = '''
            SELECT CaseNumber,
               CreatedDate,
               subject,
               concat_ws('\n', subject, description, Sub_Category_1__c, Sub_Category_2__c, Reason) AS CASE_TEXT_COMBINED,
               ai_query(?,
                        CONCAT(?, CASE_TEXT_COMBINED,?)
                       ) AS AI_CLASSIFY_RESULT
        FROM (
              SELECT *
              FROM finance_nonprod.treasury_nonprod.salesforce_cases
              WHERE CaseNumber IN 
'''
chunk_size = 25


def get_sql_connection():
    connection = sql.connect(server_hostname=host_name, http_path=http_path, access_token=access_token)
    return connection


def read_case_ids_in_chunks(connection):
    with open(file_path, 'r') as file:
        while True:
            next_chunk=list(map(lambda s: s.strip(), islice(file, chunk_size)))
            if not next_chunk:
                break
            rows = case_details(connection, next_chunk)
            print(len(rows))


def case_details(connection, case_ids):

    cursor = connection.cursor()
    parameters = [endpoint_model_name, prepend_prompt, append_prompt]
    query = case_query + '(' + ','.join(f"'{w}'" for w in case_ids) + '))'
    cursor.execute(query, parameters=parameters)
    rows = cursor.fetchall()
    cursor.close()

    return rows


connection = get_sql_connection()
read_case_ids_in_chunks(connection)
connection.close()





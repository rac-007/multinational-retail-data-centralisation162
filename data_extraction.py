#import sqlalchemy
import pandas as pd
import requests
import json
import boto3

import tabula as tb

from sqlalchemy import inspect
from database_utils import DatabaseConnector

class  DataExtractor(DatabaseConnector):
    def __init__(self):
        pass
    def list_db_tables(self):
        
        '''
        Lists all tables in the RDS database.
        
        returns a list of table names.
        '''
        engine = self.init_db_engine()
        
        
        '''
        SQLAlchemy's inspect function to list all table names in the database.
        '''
        
        inspector = inspect(engine)
        
        # .get_tables_names returns a list of table names.
        tables = inspector.get_table_names()
        return tables
    
    def read_rds_table(self, table_name):
        engine = self.init_db_engine() 
        
        '''
        sqlalchemy read_sql_table function is used to read a SQL database table directly into a pandas
        DataFrame. It loads the entire content of a SQL Table in to a DataFrame.
        '''
        df_table = pd.read_sql_table(table_name, engine) 
        #print(df_table)
        return df_table
    
    def retrieve_pdf_data(self, pdf_link):
        df_pdf = tb.read_pdf(pdf_link, multiple_tables=True , pages = "all")
            
        #Getting the dataframe of df_pdf which is a list type.
        df_pdf = pd.concat(df_pdf)
        return df_pdf
    
    
    
    # Extracting data from an Endpoint with APIKey
    def list_number_of_stores(self,endpoint,apikey):
        response = requests.get(endpoint, headers=apikey)
        
        api_data = response.text  #api_data is 'str' type
        
        # Parsing api_data with json.loads() that returns a type 'list' 
        parse_json = json.loads(api_data)
        number_stores = parse_json['number_stores']
        return number_stores
    
    def retrieve_stores_data(self,stores_num,endpoint,apikey):   
        # declaring an empty list variable 'stores_data'
        stores_data = []
        for store_number in range(1, stores_num):
            print("stores_num is ",store_number)
            response = requests.get(f'{endpoint}{store_number}', headers=apikey)
            
            store_content = response.text  # store_content 'str' type
            store_content_list = json.loads(store_content)
            stores_data.append(store_content_list)
        df_stores_data = pd.DataFrame(stores_data)
        #print("Retrieved stores df:---->\n",df_stores_data)
        return df_stores_data
    
    # Extracting data from S3
    def extract_from_s3(self, s3_address):
        # Initialize boto3 S3 resource
        s3 = boto3.resource('s3')

        # Remove any protocol prefix from the s3_address
        if 's3://' in s3_address:
            s3_address = s3_address.replace('s3://', '')
        elif 'https://' in s3_address:
            s3_address = s3_address.replace('https://', '') 
        
        # Split the remaining address into bucket_name and file_key
        bucket_name, file_key = s3_address.split('/',1)
        print('bucket name ' ,bucket_name )
        print('file_key ' ,file_key )
        
        if bucket_name!='data-handling-public':
            bucket_name = bucket_name.split('.')[0]
        print(bucket_name)
        
        # Create an S3 Object instance
        '''
        obj = s3.Object(bucket_name, file_key) initializes an S3 object instance but does not actually retrieve any data from S3. 
        Instead, it creates a high-level Object resource that represents the S3 object located at the 
        specified bucket_name and file_key.
        '''
        obj = s3.Object(bucket_name, file_key)
        
        # Get the content stream from the S3 object
        '''
        obj.get() performs a GET request to the S3 service to fetch the object's data and metadata.
        The get() method returns a dictionary containing several key-value pairs, including 'Body', which contains the actual content of the file in the form of a StreamingBody object.
        obj.get()['Body'] extracts the 'Body' from this dictionary, which is a stream of the file's contents.
        '''
        body = obj.get()['Body']
        
        # Determine file type based on file extension    
        if file_key.endswith('.csv'):
            df = pd.read_csv(body)
        elif file_key.endswith('.json'):
            df = pd.read_json(body)
        else:
            raise ValueError(f"Unsupported file format for {file_key}")
        # Reset index of DataFrame
        df = df.reset_index(drop=True)  
        return df                              
        
          
           
    
if __name__ == '__main__':
    extractor = DataExtractor()
    tables = extractor.list_db_tables()
    table = "legacy_users"
    user_data = extractor.read_rds_table(table)
    #print("Extracted User Data with Pandas DataFrame\n", user_data)
    
    pdf_link = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"
    df_card_details = extractor.retrieve_pdf_data( pdf_link )
    
    endpoint_store_details = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/" 
    endpoint_store_count = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores"
    headers = {
        'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'
    }
    total_stores = extractor.list_number_of_stores(endpoint_store_count,headers)
    # print("Total stores:--->>",total_stores)
    # print(type(total_stores))
    stores_info = extractor.retrieve_stores_data(1,endpoint_store_details,headers)
    #---------------------------------------
    # object to access s3
    s3_url = "s3://data-handling-public/products.csv"
    product_data = extractor.extract_from_s3(s3_url)
    #print("Product_table:\n",product_data)
    
    # Object access for date_time
    date_time_data = extractor.extract_from_s3("https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json")
    # print("From cleaning date time----->>>>>>",date_time_data)
    
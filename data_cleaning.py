from IPython.display import display
import pandas as pd
import numpy as np
import re
import math
import boto3

from sqlalchemy import create_engine, inspect
import psycopg2

from data_extraction import DataExtractor
from database_utils import DatabaseConnector

class DataCleaning():
    def __init__(self):
        pass
    def clean_user_data(self):
        
        print('cleaning data ')
        extractor = DataExtractor()
        tables = extractor.list_db_tables()
        legacy_user_table = 'legacy_users'
        users_table = extractor.read_rds_table(legacy_user_table)
       
        
        # Replace 'NULL' strings with NaN
        users_table.replace('NULL', np.NaN, inplace=True)
        
        # Drop rows with NaN values in the specified columns
        users_table = users_table.dropna(subset=['date_of_birth', 'email_address', 'user_uuid'], how='any', axis=0, inplace=False)
        
        
        # Convert date_of_birth  and join_date of object type into datetime
        try:
            users_table['date_of_birth'] = pd.to_datetime(users_table['date_of_birth'])

        except ValueError:
            # List of possible date formats
            date_formats = ['%Y %B %d', '%Y-%m-%d', '%Y/%m/%d']
            for fmt in date_formats:
                try: 
                    pd.to_datetime(users_table['date_of_birth'], format=fmt)
                except ValueError :
                # Return NaT (Not a Time) for invalid formats
                #return pd.NaT
                    #print(f"Error parsing 'date_of_birth': {e}")
                    continue
        #users_table['date_of_birth'] = pd.to_datetime(users_table['date_of_birth'], errors ='ignore')
        users_table['join_date'] = pd.to_datetime(users_table['join_date'], errors ='coerce')
        
        
        '''
        .dropna(subset=['join_date']): The rows where join_date is NaN are removed,
        leaving only the rows with valid join_date values.
        '''
        users_table = users_table.dropna(subset=['join_date'])
        
        
        
        # replace all non-word characters in the phone_number column 
        '''
        (.str): This accesses the string methods for the phone_number column.
        replace(r'\W', '', regex=True): This method replaces all non-word characters (anything that is not a letter, digit, or underscore) with an empty string, effectively removing them.    
        '''
        users_table['phone_number'] = users_table['phone_number'].str.replace(r'\W', '', regex=True)
        users_table = users_table.drop_duplicates(subset=['email_address'])
        
        # Droping column index
        users_table.drop(users_table.columns[0], axis=1, inplace=True)
        
        # Write the contents of Pandas DataFrame (users_table) to a CSV file named users.csv
        '''
        to_csv() This method writes the DataFrame to a CSV File.
        '''
        users_table.to_csv("users.csv")
        
        return users_table  
    
    # This method gets card_details from retrieve_pdf_data() method and return value df_card_details 
    def clean_card_data(self, card_details):
        card_details.replace('NULL', np.NaN, inplace=True)
        card_details.dropna(subset=['card_number'], how='any', axis=0, inplace=True)
        
        card_details.drop_duplicates(subset=['card_number', 'date_payment_confirmed'], keep='last', inplace=True)
        
        card_details.dropna(subset=['expiry_date', 'date_payment_confirmed'], inplace=True)
        
        card_details.to_csv("card_details.csv")
        return card_details  
      
    def clean_store_data(self, store_data):
        store_data = store_data.reset_index(drop=True)
        store_data.replace('NULL', np.NaN, inplace=True)
        store_data.replace(['N/A', 'None'], np.NaN, inplace=True)
        store_data = store_data.replace({None: np.NaN})
        
        # Drop rows with no address entry
        store_data.dropna(subset=['address'], how='any', axis=0, inplace=True)
        
        store_data.dropna(axis=1, how='any', inplace=True)
       
        # .to_datetime type and assigns NaT for non-date entry
        store_data['opening_date'] = pd.to_datetime(store_data['opening_date'], errors ='coerce')
        
        # Removes rows with NaT value 
        store_data.dropna(subset=['opening_date'], inplace=True)  
        
        # Convert 'longitude' and 'latitude' to numeric values
        store_data['longitude'] = pd.to_numeric(store_data['longitude'], errors='coerce')
        store_data['latitude'] = pd.to_numeric(store_data['latitude'], errors='coerce')
        
        # Drop rows where the conversion to numeric resulted in NaN
        store_data.dropna(subset=['longitude', 'latitude'], inplace=True)
        
        # Remove duplicates, keeping the last occurrence
        store_data.drop_duplicates(keep='last', inplace=True)
        
        # Correcting continent value
        store_data_copy = store_data.copy()
        store_data_copy['continent'] = store_data_copy['continent'].str.replace('eeEurope', 'Europe').str.replace('eeAmerica', 'America')
        store_data_copy.drop(store_data_copy.columns[0], axis=1, inplace=True) 
        
        # Return the cleaned DataFrame
        return store_data_copy

    
    # Product Data cleaning methods
    def convert_product_weights(self,x):
        '''
        isnull(x) and isinstnce(),check if x is null or x is string type. 
        '''
        if pd.isnull(x) or not isinstance(x, str):
         return np.nan
        # Normalize the input (convert to lowercase and strip whitespace)
        x = x.lower().strip().replace(' .', '').strip()
        if 'kg' in x:
            x = x.replace('kg', '')
            x = float(x)         
        elif 'ml' in x:
            x = x.replace('ml', '')
            x = float(x)/1000
        elif 'g' in x:
            x = x.replace('g', '')
            x = float(x)/1000  
        elif 'lb' in x:
            x = x.replace('lb', '')
            x = float(x) * 0.453592  # Convert lb to kg

        elif 'oz' in x:
            x = x.replace('oz', '').strip()
            x = float(x) * 0.0283495  # Convert oz to kg        
        else:
            x= np.nan
        return x
    
    
    def  clean_products_data(self, df_product):
        # Reset index of DataFrame and Intitial cleaning of DataSet
        df_product = df_product.reset_index(drop=True)
        df_product.drop_duplicates()
        df_product.replace('NULL', np.NaN, inplace=True)
        df_product['date_added'] = pd.to_datetime(df_product['date_added'],errors='coerce')
        df_product.dropna(subset=['date_added'],how = 'any', axis=0,inplace=True)

        # Working with weight column, it has values like [.,12X100,9chd005hg]
        # Removing 'X' multiplication sign
        wt_value_x = df_product.loc[df_product.weight.str.contains('x'),'weight'].str.split('x',expand=True)
        numeric_cols = wt_value_x.apply(lambda x: pd.to_numeric(x.str.extract(r'(\d+\.?\d*)', expand=False)), axis=1)
        final_weight = numeric_cols.prod(axis=1)
        df_product.loc[df_product.weight.str.contains('x'),'weight'] = final_weight
        to_lower_unit = lambda value:str(value).lower().strip()

        df_product['weight'] = df_product['weight'].apply(to_lower_unit) 
        df_product['weight'] = df_product['weight'].apply(lambda x: self.convert_product_weights(x))
        
        # Droping the unnamed column, containing duplicate index column values
        df_product.drop(df_product.columns[0], axis=1, inplace=True)  
        return df_product
    
    
    def clean_orders_data(self, order_data_df):
        order_data_df.drop_duplicates()
        order_data_df.drop("level_0", axis=1, inplace=True) 
        order_data_df.drop(order_data_df.columns[0], axis=1, inplace=True)
        order_data_df.drop("1", axis=1, inplace=True)
        order_data_df.drop('first_name',axis=1, inplace=True)
        order_data_df.drop('last_name',axis=1, inplace=True)
        return order_data_df
    
    def clean_date_time(self,date_time_df):
        date_time_data['year']=pd.to_numeric(date_time_data['year'], errors='coerce')
        date_time_data.dropna(subset=['year'], how='any', axis=0,inplace=True)
        return date_time_data
        
        
if __name__ == '__main__':
    cleaner = DataCleaning()
    extractor = DataExtractor()
    db_connector = DatabaseConnector()
    print("all objects created")

    # ---------------------User Data---------------------
    #  clean_user_table get the clean users details from clean_user_data() method, that returns users_table
    clean_user_table = cleaner.clean_user_data()
    db_connector.upload_to_db(clean_user_table, 'dim_users')
    
    # ---------------------Card Data----------------------
    pdf_link = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"
    df_card_details = extractor.retrieve_pdf_data( pdf_link )
    # # clean_card_table  gets the filtered card details from clean_card_data(), that returns card_details
    clean_card_table = cleaner.clean_card_data(df_card_details)
    clean_card_table.to_csv('store_outputs.csv')
    db_connector.upload_to_db(clean_card_table, 'dim_card_details')
    
    
    
    # ---------------------Store Data----------------------
    # API and API key
    endpoint_store_details = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/" 
    endpoint_store_count = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores"
    headers = {
        'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'
    }
    # #print("  check this ",type(endpoint_store_count))
    total_stores = extractor.list_number_of_stores(endpoint_store_count,headers)
    # #print("end points called total_stores  ")
    stores_info = extractor.retrieve_stores_data(total_stores,endpoint_store_details,headers)
    # #clean_store_table get the cleaned store details from called_clean_store_data()
    clean_store_table = cleaner.clean_store_data(stores_info)
    clean_store_table.to_csv('store_outputs.csv')
    db_connector.upload_to_db(clean_store_table, 'dim_store_details')
    

    # ---------------------Product_data--------------------------
    s3_url = "s3://data-handling-public/products.csv"
    product_data = extractor.extract_from_s3(s3_url)
    # #print(product_data)
    clean_product_table = cleaner.clean_products_data(product_data)
    clean_product_table.to_csv('orders.csv')
    db_connector.upload_to_db(clean_product_table,'dim_products')
    
    # ---------------------Orders Table--------------------------
    order_table = extractor.read_rds_table('orders_table')
    clean_order_table = cleaner.clean_orders_data(order_table)
    clean_order_table.to_csv('orders.csv')
    db_connector.upload_to_db(clean_order_table,'orders_table')
   
    
    # ----------------------date_time Table-----------------------
    date_time_data = extractor.extract_from_s3("https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json")
    #print("From cleaning\n",date_time_data)
    clean_date_time_table = cleaner.clean_date_time(date_time_data)
    clean_date_time_table.to_csv('date.csv')
    db_connector.upload_to_db(clean_date_time_table,'dim_date_times')
    # -------------------------------------------------------------
import yaml
import sqlalchemy
from sqlalchemy import create_engine, inspect
import psycopg2
#from sqlalchemy import inspect
#from sqlalchemy.engine.url import URL
# import json
class DatabaseConnector:
    def __init__(self):    
        '''
        Reads database credentials from a YAML file and returns them as a dictionary.
        
        Returns:
        dict: A dictionary containing the database credentials.
        '''
    def read_db_creds(self):
        with open ('db_creds.yaml','r') as f:
           db_creds = yaml.load(f,Loader=yaml.SafeLoader)
           #print(db_creds)
        return db_creds

    def init_db_engine(self):
        db_creds= self.read_db_creds()
        connection_str = (f"{'postgresql'}+{'psycopg2'}://{db_creds['RDS_USER']}:{db_creds['RDS_PASSWORD']}@{db_creds['RDS_HOST']}:{db_creds['RDS_PORT']}/{db_creds['RDS_DATABASE']}")
        # Create the SQLAlchemy engine.
        engine = create_engine(connection_str)
        #engine.connect()
        return engine

    def init_local_db_engine(self):
        # Creating connection string for the local database.
        db_creds= self.read_db_creds()
        connection_str = (f"{'postgresql'}+{'psycopg2'}://{db_creds['LOCAL_USER']}:{db_creds['LOCAL_PASSWORD']}@{db_creds['LOCAL_HOST']}:{db_creds['LOCAL_PORT']}/{db_creds['LOCAL_DATABASE']}")
        engine = create_engine(connection_str)
        return engine
    
    def upload_to_db(self, data_frame, table_name):
        engine = self.init_local_db_engine()
        conn = engine.connect()
        data_frame.to_sql(table_name, conn, if_exists='replace')

        
    
if __name__ == '__main__':
    db_connector = DatabaseConnector()
    engine = db_connector.init_db_engine()
    
    
    # Test the connection
    with engine.connect() as connection:
        print("Connection successful!")
    
    
 
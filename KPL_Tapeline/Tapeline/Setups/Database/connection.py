from dotenv import dotenv_values
import sys

from ..Loggers.Logger import Logs

from dotenv import dotenv_values
import pymssql
from sqlalchemy import create_engine
from sqlalchemy import true


class Conn:
    def __init__(self):
        """
        Initializes the Conn class with server, database, username, and password attributes.

        Parameters:
        server (str): The name or IP address of the server to connect to.
        database (str): The name of the database to connect to.
        username (str): The username to use for authentication.
        password (str): The password to use for authentication.
        """
        
        # Load the environment variables from the .env file
        #config = dotenv_values('ScheduleV1\.env')
        config = dotenv_values('C:/Users/ATISHAY/Desktop/KPL_Tapeline/.env')
        #print(config)
        self.server = config['server']
        self.database = config['database']
        self.username = config['username']
        self.password = config['password']
        self.engine = None
        self.conx = None
        self.logger = Logs()  # Create an instance of the Logs class

    def connect(self):
        try:
            self.conx = pymssql.connect(server=self.server, database=self.database,
                                        user=self.username, password=self.password)
            print('Connection is Established')
            
            self.engine = create_engine('mssql+pymssql://', creator=lambda: self.conx)

            self.logger.Logging('Connection successfully established!')

            return True
        
        except Exception as e:
            self.logger.Logging(f'Failed to establish a connection, Reason:{e}')

            raise "Connection is not established"


    def get_engine(self):
        """
        Returns the connection engine.

        Returns:
        engine: The connection engine.
        """
        return self.engine

    def get_conx(self):
        """
        Returns the connection object.

        Returns:
        conx: The connection object.
        """
        return self.conx

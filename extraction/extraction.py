import os
import yfinance as yf
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Constants
DATABASE_TYPE = os.getenv("DATABASE_TYPE")
DB_API = os.getenv("DB_API")
HOST = os.getenv("HOST") 
PG_USER = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")
PG_DATABASE = os.getenv("PG_DATABASE")
PORT = os.getenv("PORT")
TABLE_NAME = 'spy_data'  

def fetch_and_store_data(ticker_symbol, start_date, end_date):
    """
    Fetches historical data for a given ticker symbol and stores it in a database.
    
    Args:
    ticker_symbol (str): The ticker symbol of the stock.
    start_date (str): The start date for the data fetch in YYYY-MM-DD format.
    end_date (str): The end date for the data fetch in YYYY-MM-DD format.
    """
    # Construct the database connection URI
    database_uri = f'{DATABASE_TYPE}+{DB_API}://{PG_USER}:{PG_PASSWORD}@{HOST}:{PORT}/{PG_DATABASE}'
    
    # Create a database engine
    engine = create_engine(database_uri, 
                           echo=False)
    
    # Use yfinance to fetch the data
    data = yf.download(ticker_symbol, 
                       start=start_date, 
                       end=end_date)
    
    # Load the data into PostgreSQL
    data.to_sql(TABLE_NAME, 
                engine, 
                if_exists='replace', 
                index=True, 
                index_label='Date')
    print("Data stored successfully.")

if __name__ == "__main__":
    # Define the ticker symbol and the period for the data you want to fetch
    ticker_symbol = "SPY"
    start_date = "2000-01-01"
    end_date = pd.Timestamp.today().strftime('%Y-%m-%d')  # Use current date as the end date
    
    # Schedule to run this function every day after the market close
    fetch_and_store_data(ticker_symbol, start_date, end_date)

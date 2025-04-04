import mysql.connector as sql
import datetime
from mysql.connector import Error
import requests
import json
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

from dotenv import load_dotenv
import os

load_dotenv()

conn = sql.connect(
    host=os.getenv("MYSQL_HOST"),
    user=os.getenv("MYSQL_USER"),
    password=os.getenv("MYSQL_PASSWORD"),
    database=os.getenv("MYSQL_DATABASE")
)
cur = conn.cursor()

def modify_table():
    alter_query = """
    ALTER TABLE ohlc_data
    ADD COLUMN SMA_14 float DEFAULT NULL,
    ADD COLUMN RSI_14 float DEFAULT NULL,
    ADD COLUMN MACD float DEFAULT NULL,
    ADD COLUMN MACD_Signal float DEFAULT NULL;
    """
    cur.execute(alter_query)
    conn.commit()

def store_market_data(price):
    try:
        if conn.is_connected():
            cur = conn.cursor()

            create_table_query = """
            CREATE TABLE IF NOT EXISTS ohlc_data (
                id INT AUTO_INCREMENT PRIMARY KEY,
                Timestamp DATETIME,
                Open_Price DECIMAL(18, 8),
                High_Price DECIMAL(18,8),
                Low_Price DECIMAL(18,8),
                Close_Price DECIMAL(18,8)
            );
            """
            cur.execute(create_table_query)
            price['Timestamp'] = price['Timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')

      
            insert_query = "INSERT INTO ohlc_data (Timestamp, Open_Price, High_Price, Low_Price, Close_price) VALUES (%s,%s,%s,%s,%s)"
            for _, row in price.iterrows():
                cur.execute(insert_query, tuple(row))
            conn.commit()
            print("Data inserted successfully!")

    except Error as e:
        print("Error while connecting to MySQL", e)
    
   

def fetch_market_data(coin_id='bitcoin', vs_currency='usd', days=365):
    all_data = []

    for chunk_start in [365, 1]:
        url = f"https://api.coingecko.com/api/v3/coins/{coin_id}/ohlc"
        params = {
            'vs_currency': vs_currency,
            'days': chunk_start
        }

        response = requests.get(url, params=params)
        if response.status_code == 200:
            data = response.json()
            
           
            print("Sample API Response:", data[:5])

                   
            for entry in data:
                if len(entry) == 5:
                    all_data.append([pd.to_datetime(entry[0], unit='ms')] + entry[1:])
                else:
                    print("Skipping malformed entry:", entry)

        else:
            print(f"Failed to fetch {chunk_start} days of data:", response.status_code)
            return None

    # Convert to DataFrame
    df = pd.DataFrame(all_data, columns=['Timestamp', 'Open', 'High', 'Low', 'Close'])
    return df

def fetch_close_prices():
    query = "Select id,Timestamp, Close_Price from ohlc_data order by id Asc"
    cur.execute(query)
    res = cur.fetchall()
    df = pd.DataFrame(res, columns=['id', 'Timestamp', 'Close_Price'])
    df['Timestamp'] = pd.to_datetime(df['Timestamp'])
    return df

def sma(data, window):
    
    return data['Close_Price'].rolling(window=window).mean()


def rsi(data, window=14):
    delta = data['Close_Price'].diff(1)
    gain = (delta.where(delta > 0, 0)).rolling(window=window).mean()
    loss = (delta.where(delta < 0, 0)).rolling(window=window).mean()
    
    rs = gain / loss
    rsi = 100 - (100 / (1 + rs))
    
    return rsi

def macd(df, a=12, b=26, c=9):
    df = df.copy()  
    df['ma_fast'] = df['Close_Price'].ewm(span=a, min_periods=a).mean()
    df['ma_slow'] = df['Close_Price'].ewm(span=b, min_periods=b).mean()
    df['macd'] = df['ma_fast'] - df['ma_slow']
    df['macd_signal'] = df['macd'].ewm(span=c, min_periods=c).mean()
    return df[['id', 'macd', 'macd_signal']]  


def update_sma_rsi():
    df = fetch_close_prices()
    if df is not None and not df.empty:
        df['SMA_14'] = sma(df, 14)
        df['RSI_14'] = rsi(df, 14)

        update_query = """
        UPDATE ohlc_data
        SET SMA_14 = %s, RSI_14 = %s
        WHERE id = %s;
        """
        for i, row in df.iterrows():
            cur.execute(update_query, (row['SMA_14'], row['RSI_14'], row['id']))

        conn.commit()
        print("SMA & RSI updated successfully!")

def update_macd():
    df = fetch_close_prices()  
    if df is not None and not df.empty:
        macd_values = macd(df)
        
        update_query = """
        UPDATE ohlc_data
        SET MACD = %s, MACD_Signal = %s
        WHERE id = %s;
        """
        for i, row in macd_values.iterrows():
            cur.execute(update_query, (float(row['macd']), float(row['macd_signal']), int(row['id'])))
        
        conn.commit()
        print("MACD & Signal updated successfully!")

def plot_trading_indicators():
    df = fetch_close_prices()
    if df is None or df.empty:
        print("No data to plot.")
        return

    df['SMA_14'] = sma(df, 14)
    df['RSI_14'] = rsi(df, 14)
    macd_values = macd(df)
    
    df['MACD'] = macd_values['macd']
    df['MACD_Signal'] = macd_values['macd_signal']

    
    fig, axes = plt.subplots(3, 1, figsize=(14, 10), sharex=True)

   
    axes[0].plot(df['Timestamp'], df['Close_Price'], label='Close Price', color='blue')
    axes[0].plot(df['Timestamp'], df['SMA_14'], label='SMA 14', linestyle='dashed', color='red')
    axes[0].set_title("Bitcoin Close Price & SMA")
    axes[0].legend()
    axes[0].grid()

    
    axes[1].plot(df['Timestamp'], df['RSI_14'], label='RSI 14', color='purple')
    axes[1].axhline(70, linestyle='dashed', color='red')  # Overbought
    axes[1].axhline(30, linestyle='dashed', color='green')  # Oversold
    axes[1].set_title("RSI Indicator")
    axes[1].legend()
    axes[1].grid()

    
    axes[2].plot(df['Timestamp'], df['MACD'], label='MACD', color='black')
    axes[2].plot(df['Timestamp'], df['MACD_Signal'], label='Signal Line', linestyle='dashed', color='orange')

    axes[2].bar(df['Timestamp'], df['MACD'] - df['MACD_Signal'], color=['green' if val >= 0 else 'red' for val in df['MACD'] - df['MACD_Signal']], alpha=0.5)
    
    axes[2].set_title("MACD Indicator")
    axes[2].legend()
    axes[2].grid()

    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()



if __name__ == "__main__":
    data = fetch_market_data("bitcoin","usd",365)
    print(data.head())
    store_market_data(data)
    update_sma_rsi()
    update_macd()
    print("Table updated")
    plot_trading_indicators()
    cur.close()
    conn.close()
    
    


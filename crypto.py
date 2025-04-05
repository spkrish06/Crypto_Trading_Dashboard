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
            cur.execute("DELETE FROM ohlc_data;")
            conn.commit()
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


def rsi(df, window=14):
    df = df.copy()
    df['Change'] = df['Close_Price'] - df['Close_Price'].shift(1)
    df['Gain'] = np.where(df['Change']>=0, df['Change'],0)
    df['Loss'] = np.where(df['Change']<0, -1*df['Change'],0)
    df['Avg_Gain'] = df['Gain'].ewm(alpha = 1/window, min_periods=window).mean()
    df['Avg_Loss'] = df['Loss'].ewm(alpha = 1/window, min_periods=window).mean()
    df['RS'] = df['Avg_Gain']/df['Avg_Loss']
    df['RSI'] = 100 - (100/(1+df['RS']))
    return df['RSI']


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
    
    plt.clf() 
    plt.close('all')
    df = fetch_close_prices()
    if df is None or df.empty:
        print("No data to plot.")
        return

    df['SMA_14'] = sma(df, 14)
    df['RSI_14'] = rsi(df, 14)
    macd_values = macd(df)

    df['MACD'] = macd_values['macd']
    df['MACD_Signal'] = macd_values['macd_signal']


    # SMA logic
    df['Signal'] = np.where(df['Close_Price'] > df['SMA_14'], 'Up',
                     np.where(df['Close_Price'] < df['SMA_14'], 'Down', ''))

    fig, axes = plt.subplots(3, 1, figsize=(14, 12), sharex=True)

    # --- SMA + Close Price ---
    axes[0].plot(df['Timestamp'], df['Close_Price'], label='Close Price', color='blue')
    axes[0].plot(df['Timestamp'], df['SMA_14'], label='SMA 14', linestyle='dashed', color='red')

    up_signals = df[df['Signal'] == 'Up']
    down_signals = df[df['Signal'] == 'Down']
    axes[0].scatter(up_signals['Timestamp'], up_signals['Close_Price'], marker='^', color='green', label='Uptrend', alpha=0.8)
    axes[0].scatter(down_signals['Timestamp'], down_signals['Close_Price'], marker='v', color='red', label='Downtrend', alpha=0.8)

    axes[0].set_title("Close Price & SMA with Trends")
    axes[0].legend(loc = "upper left")
    axes[0].grid()

    # --- RSI ---
    axes[1].plot(df['Timestamp'], df['RSI_14'], label='RSI 14', color='purple')
    axes[1].axhline(70, linestyle='dashed', color='red')   # Overbought
    axes[1].axhline(30, linestyle='dashed', color='green') # Oversold
    axes[1].set_title("RSI Indicator")
    axes[1].legend(loc = "upper left")
    axes[1].grid()

    # --- MACD ---
    axes[2].plot(df['Timestamp'], df['MACD'], label='MACD', color='black')
    axes[2].plot(df['Timestamp'], df['MACD_Signal'], label='Signal Line', linestyle='dashed', color='orange')
    axes[2].bar(df['Timestamp'], df['MACD'] - df['MACD_Signal'],
                color=['green' if val >= 0 else 'red' for val in df['MACD'] - df['MACD_Signal']], alpha=0.4)

    axes[2].set_title("MACD Indicator")
    axes[2].legend(loc = "upper left")
    axes[2].grid()

    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()




if __name__ == "__main__":
    crypto = input("Enter the crypto whose data to be fetched: ")
    cryp = crypto.lower()
    match crypto:
        case 'bitcoin' | 'btc':
            data = fetch_market_data("bitcoin","usd",365)

        case 'ethereum' | 'eth':
            data = fetch_market_data("ethereum","usd",365)

        case 'binancecoin' | 'bnb':
             data = fetch_market_data("binancecoin","usd",365)

        case 'tether' | 'usdt':
             data = fetch_market_data("tether","usd",365)
    
    print(data.head())
    store_market_data(data)
    update_sma_rsi()
    update_macd()
    print("Table updated")
    plot_trading_indicators()
    cur.close()
    conn.close()
    
    


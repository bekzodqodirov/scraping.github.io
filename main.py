import csv
import datetime as dt
import time
import requests
import schedule
from bs4 import BeautifulSoup
from telethon.sync import TelegramClient
from config import *
from datetime import timedelta
import pandas as pd


# this function return boolean if it is trading day
def is_trading_day():
    weekday = dt.datetime.utcnow().weekday()
    if weekday == 5 or weekday == 6:
        return False
    else:
        return True


# this function gets pre market gainers from benzinga.com
def pre_market_gainers_benzinga():
    gainers_list = []
    page = requests.get("https://www.benzinga.com/premarket/")
    soup = BeautifulSoup(page.content, "html.parser")
    tables = soup.findChildren("table", {"class": "premarket-stock-table premarket-stock-table--scrollable"})
    my_table = tables[0]
    print(page)

    # You can find children with multiple tags by passing a list of strings
    rows = my_table.findChildren(['th', 'tr'])

    for row in rows:

        cells = row.findChildren('a')
        for cell in cells:
            value = cell.string.strip()
            gainers_list.append(value)

    return gainers_list


# this function sent 7 top gainers via telegram to saved massage
def telegram_alarm():
    time_now = dt.datetime.utcnow().strftime("%Y-%m-%d  %H:%M")
    api_id = telegram_api_id
    api_hash = telegram_api_hash

    str1 = f"{time_now} \nToday's Gainers are: \n"
    count = 0
    for i in pre_market_gainers_benzinga():
        count += 1
        if count < 8:
            str1 += i + "\n"

    with TelegramClient('name', api_id, api_hash) as client:
        client.send_message("me", str1)


# writes top gainers to data_gainers.csv file
def list_gainers_csv():
    time_now = dt.datetime.utcnow().strftime("%Y-%m-%d  %H:%M")
    str1 = f"Today is {time_now} \nToday's gainers are: \n"
    count = 0
    for i in pre_market_gainers_benzinga():
        count += 1
        if count < 8:
            str1 += i + "\n"
    list_gainers = [time_now]
    for i in pre_market_gainers_benzinga():
        list_gainers.append(i)

    with open('data_gainers.csv', 'a', newline='') as f:
        write = csv.writer(f)
        write.writerow(list_gainers)


# this function gives
def price_action_intraday(ticker):

    market_open = dt.datetime.strptime((dt.datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d 09:31:00"),
                                       "%Y-%m-%d %H:%M:%S")
    market_close = dt.datetime.strptime((dt.datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d 16:01:00"),
                                        "%Y-%m-%d %H:%M:%S")
    pre_market_open = dt.datetime.strptime((dt.datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d 05:00:00"),
                                           "%Y-%m-%d %H:%M:%S")
    first_15_min = dt.datetime.strptime((dt.datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d 09:45:00"),
                                        "%Y-%m-%d %H:%M:%S")
    first_30_min = dt.datetime.strptime((dt.datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d 10:00:00"),
                                        "%Y-%m-%d %H:%M:%S")
    first_60_min = dt.datetime.strptime((dt.datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d 10:30:00"),
                                        "%Y-%m-%d %H:%M:%S")

    ticker = ticker
    try:
        data = requests.get(
            f"https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol={ticker}&interval=1min"
            f"&outputsize=full&apikey={alpha_vantage_api_key}").json()
        data_dic = data["Time Series (1min)"]
        # Pre Market Data
        pre_market_volume = 0
        pre_market_high = 0
        pre_market_low = 99999

        # Market open
        open_volume = 0
        open_high = 0
        open_low = 99999

        # First 15 minute Trading
        first_15_high = 0
        first_15_volume = 0
        first_15_low = 99990

        # First 30 minute of trading
        first_30_high = 0
        first_30_volume = 0
        first_30_low = 99990

        # first hour of Trading
        first_60_high = 0
        first_60_volume = 0
        first_60_low = 99990

        # total day trading
        day_high = 0
        day_volume = 0
        day_low = 99990

        # After Hours
        after_hours_high = 0
        after_hours_volume = 0
        after_hours_low = 99999

        for i_in_dic in data_dic:
            current_time = dt.datetime.strptime(i_in_dic, "%Y-%m-%d %H:%M:%S")
            # candle_open = float((data_dic[i_in_dic]["1. open"]))
            # candle_close = float((data_dic[i_in_dic]["4. close"]))
            candle_high = float((data_dic[i_in_dic]["2. high"]))
            candle_low = float((data_dic[i_in_dic]["3. low"]))
            candle_volume = int((data_dic[i_in_dic]["5. volume"]))

            # Pre Market Trading session
            if pre_market_open < current_time < market_open:

                # pre market Volume
                pre_market_volume += candle_volume

                # pre market High
                if candle_high > pre_market_high:
                    pre_market_high = candle_high

                # pre market Low
                if candle_low < pre_market_low:
                    pre_market_low = candle_low

            # market open
            if market_open == current_time:
                open_high = candle_high
                open_volume = candle_volume
                open_low = candle_low

            # first 15 minute trading
            if market_open <= current_time < first_15_min:
                # first 15 min volume
                first_15_volume += candle_volume

                # first 15 min high
                if first_15_high < candle_high:
                    first_15_high = candle_high

                # first 15 min low
                if first_15_low > candle_low:
                    first_15_low = candle_low

            # First 30 minute trading
            if market_open <= current_time < first_30_min:
                # first 30 min volume
                first_30_volume += candle_volume

                # first 30 min high
                if first_30_high < candle_high:
                    first_30_high = candle_high

                # first 30 min low
                if first_30_low > candle_low:
                    first_30_low = candle_low

            # first 60 minute trading
            if market_open <= current_time < first_60_min:
                # first 60 min volume
                first_60_volume += candle_volume

                # first 60 min high
                if first_60_high < candle_high:
                    first_60_high = candle_high

                # first 60 min low
                if first_60_low > candle_low:
                    first_60_low = candle_low

            # trading Hours
            if market_open <= current_time < market_close:
                # day volume
                day_volume += candle_volume

                # day High
                if day_high < candle_high:
                    day_high = candle_high

                # day low
                if day_low > candle_low:
                    day_low = candle_low

            # after Hours Trading session
            if market_close < current_time:
                after_hours_volume += candle_volume

                # After hours high
                if after_hours_high < candle_high:
                    after_hours_high = candle_high

                if after_hours_low > candle_low:
                    after_hours_low = candle_low

        total_data = {"ticker": ticker, "pre_market_high": pre_market_high, "pre_market_low": pre_market_low,
                      "pre_market_volume": pre_market_volume, "open_high": open_high, "open_volume": open_volume,
                      "open_low": open_low, "first_15_high": first_15_high, "first_15_low": first_15_low,
                      "first_15_volume": first_15_volume, "first_30_high": first_30_high, "first_30_low": first_30_low,
                      "first_30_volume": first_30_volume, "first_60_high": first_60_high, "first_60_low": first_60_low,
                      "first_60_volume": first_60_volume, "day_high": day_high, "day_low": day_low,
                      "day_volume": day_volume,
                      "after_hours_high": after_hours_high, "after_hours_low": after_hours_low,
                      "after_hours_volume": after_hours_volume}
        return True, total_data

    except KeyError:
        print("there is no such ticker(Error occurred)")
        b = {}
        return False, b


def price_action_csv():
    data = pd.read_csv("data_gainers.csv")
    list_gainers = [data["ticker"].iloc[-1]]

    for i in range(6):
        i += 1
        list_gainers.append(data[f"ticker.{i}"].iloc[-1])
    print(list_gainers)

    for i in list_gainers:
        boolean, dic_price_action = price_action_intraday(i)
        if boolean:
            field_name = []
            for keys in dic_price_action:
                field_name.append(keys)

            with open("market_gainers_data.csv", newline='', mode="a") as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=field_name)
                writer.writerows([dic_price_action])
            print(f"{dic_price_action['ticker']} have been written")
        else:
            pass


# this function runs continuously to run another functions every day at market open
def run_every_day():
    if is_trading_day():
        print(f"It is trading day \ntime is: {dt.datetime.now()}")

        schedule.every().day.at("22:10").do(price_action_csv)
        schedule.every().day.at("22:30").do(list_gainers_csv)
        schedule.every().day.at("22:30").do(telegram_alarm)

    while True:
        schedule.run_pending()
        time.sleep(1)


run_every_day()

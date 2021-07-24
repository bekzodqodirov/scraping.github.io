import csv
import requests
import datetime as dt
from config import *
from datetime import timedelta
import pandas as pd


def price_action_intraday(ticker):
    global total_data
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
        return False, total_data


def write_csv():
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

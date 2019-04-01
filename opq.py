
'''
The main file to do daily trading.
It first fetch the pairs in the portfolio,
then fetch the stocks' prices,
then calculate the spread and target positions,
and finally print the target orders to user.

To Improve: Execute orders using IB API.

'''

import os

import pandas as pd

from util import *
from strategy import *
import data



def main(*argv):
    
    # Intialize the strategy and fetch pair info
    
    config = load_config("asset_config.txt")
    config_general = load_config()
    log_file = config_general['LOG_FILE']

    write_log("Top Secret Program Start", log_file)

    ustime = get_us_time()

    asset_file = config["ASSET_FILE"]
    asset_history_folder = config["ASSET_HISTORY_FOLDER"]
    transaction_history_folder = config["TRANSACTION_HISTORY_FOLDER"]
    asset_file_today = os.path.join(asset_history_folder, f"asset_{ustime}.csv")
    tx_file_today = os.path.join(transaction_history_folder, f"transactions_{ustime}.csv")

    max_leverage = float(config['MAX_LEVERAGE'])
    thresholds = [
        float(config["EXIT_THRESHOLD"]),
        float(config["ENTER_THRESHOLD"]),
        float(config["ENTER2_THRESHOLD"]),
        float(config["ENTER3_THRESHOLD"]),
        float(config["STOP_THRESHOLD"])
    ]
    allocations = [
        float(config["ENTER_ALLOCATION"]),
        float(config["ENTER2_ALLOCATION"]),
        float(config["ENTER3_ALLOCATION"])
    ]

    strat = PairTradeStrategy([], thresholds, allocations)

    write_log(f"Loading Pair Asset from {asset_file}", log_file)
    try:    
        strat.load_pair_info(asset_file)
    except Exception as e:
        write_log(f"Failed to load Pair Asset from {asset_file}: " + str(e), log_file)
        write_log(f"Program Terminated", log_file)
        return

    write_log("Fetching real-time stock price...", log_file)
    
    # Fetch stock prices
    stocks = set()
    for pair in strat.pairs:
        stocks.add(pair.X)
        stocks.add(pair.Y)
    stock_prices = data.get_current_price(list(stocks))

    for stock in stocks:
        if stock not in stock_prices:
            print(f"Failed to get price for {stock}")

    write_log("Analyzing spreads...", log_file)

    # Ask strategy to decide
    orders = strat.decide(stock_prices)

    # Output orders
    print("Today's Orders:")
    print("Action  Stock       Quantity    Price")
    print("------  -----       --------    -----")
    tx_df = pd.DataFrame(columns=["Direction", "Stock", "Quantity", "Price"])
    for stock, quantity in orders.items():
        if quantity == 0:
            continue
        direction = "Buy" if quantity > 0 else "Sell"
        quantity = abs(quantity)
        price = stock_prices[stock]
        print(f"{direction:8}{stock:12}{str(quantity):12}{str(price):12}")
        tx_df = tx_df.append({
            "Direction": direction,
            "Stock": stock,
            "Quantity": quantity,
            "Price": price
        }, ignore_index=True)
    print("\nEnd of Today's Orders")


    # Future: Use IBAPI to exec orders
    #

    # File I/O
    if not os.path.isdir(asset_history_folder):
        os.makedirs(asset_history_folder)
    if not os.path.isdir(transaction_history_folder):
        os.makedirs(transaction_history_folder)
    strat.dump_pair_info(asset_file)
    strat.dump_pair_info(asset_file_today)
    tx_df.to_csv(tx_file_today, index=False)

    write_log(f"Suggested orders have been saved to {asset_file_today}", log_file)
    write_log(f"Updated positions have been saved to {asset_file} and {asset_file_today}", log_file)

    write_log(f"Program Exit. Good night!", log_file)
    input()


if __name__ == "__main__":
    main()















'''
Backtest the strategies.
'''

import os

import pandas as pd
import numpy as np

import util
from strategy import *


def print_order(orders, stock_data, indent=1):
    '''
    Print the order in human-readable format. For debug purpose
    '''

    for code, quantity in orders.items():
        price = stock_data[code][-1:]['CLOSE'][0]
        if quantity > 0:
            print('\t' * indent + f"Bought {quantity} shares of {code} @ {price}")
        elif quantity < 0:
            quantity = -quantity
            print('\t' * indent + f"Sold/shorted {quantity} shares of {code} @ {price}")

    

def cash_change(orders, stock_data, tx_cost_per_share=0, tx_cost_per_dollar=0):
    '''
    Calculate the change of cash as a result of a series of orders.
    Use the latest daily close price of the stocks as the settlement price.
    
    For now, accept any order with no margin requirement (you can buy/short however much you want).
    
    '''
    
    change = 0
    for code, quantity in orders.items():
        if not code in stock_data:
            raise Exception("Stock data not found for " + code)
        settlement_price = stock_data[code][-1:]['CLOSE'][0]
        # If quantity > 0, it means buying the instrument, so cash goes down. Vice versa.
        tx_amount = -settlement_price * int(quantity)
        change += tx_amount
        
        # Apply SEC's fee (per dollar amount)
        change -= abs(tx_amount) * tx_cost_per_dollar
        # Apply broker's fee (per share)
        change -= quantity * tx_cost_per_share
        

    return round(change, 2)


def net_worth(positions, stock_data):
    '''
    Calculate the network of the positions.
    '''

    worth = 0
    for code, quantity in positions.items():
        if not code in stock_data:
            raise Exception("Stock data not found for " + code)
        settlement_price = stock_data[code][-1:]['CLOSE'][0]
        worth += settlement_price * int(quantity)
        
    return round(worth, 2)


def evaluate(strategy, config):
    '''
    Evaluate the strategy.
    Returns a dict of performace metrics
    '''

    training_start = config['TRAINING_START']
    training_end = config['TRAINING_END']
    backtesting_start = config['BACKTESTING_START']
    backtesting_end = config['BACKTESTING_END']
    tx_cost_per_share = float(config['TX_COST_PER_SHARE'])
    tx_cost_per_dollar = float(config['TX_COST_PER_DOLLAR'])

    # Prepare the data for the strategy
    stock_codes = []
    for pair in strategy.watch_list:
        stock_codes.append(pair['Stock_1'])
        stock_codes.append(pair['Stock_2'])
    num_days_test = 0
    stock_data = util.load_stock_data(config['STOCK_DATA_FOLDER'], stock_codes)
    stock_data_training = {}
    stock_data_backtesting = {}
    for code, df in stock_data.items():
        stock_data_training[code] = df.loc[training_start : training_end]
        stock_data_backtesting[code] = df.loc[backtesting_start : backtesting_end]
        num_days_test = len(stock_data_backtesting[code])

    testing_dates = [d for d in list(stock_data_backtesting.values())[0].index if backtesting_start <= d and d <= backtesting_end]
    testing_dates.sort()

    # Define initial cash and reset positions to empty
    initial_cash = 1000000
    cash = initial_cash
    strategy.initial_cash = initial_cash
    strategy.positions({})
    
    # First feed the "past" data, analyze the spread pattern, and then execute orders
    strategy.feed(stock_data_training)
    strategy.analyze_spread()
    orders = strategy.decide()
    cash += cash_change(orders, stock_data_training, tx_cost_per_share, tx_cost_per_dollar)
    strategy.positions(orders, incremental=True)

    # Then advance the timeline day by day
    daily_tnw = []
    for date in testing_dates:
        # Each day starts with some new data
        stock_data_single_day = {}
        for code, df in stock_data_backtesting.items():
            stock_data_single_day[code] = df.loc[date : date]
            day = stock_data_single_day[code].index[0]
        strategy.feed(stock_data_single_day)
        # Get and execute orders for the day
        orders = strategy.decide()
        cash += cash_change(orders, stock_data_single_day)
        strategy.positions(orders, incremental=True)
        # Record the net worth and return
        total_net_worth = cash + net_worth(strategy.positions(), stock_data_single_day)
        daily_tnw.append(total_net_worth)

    # Calculate the values for the performance metrics
    daily_return = []
    prev_tnw = initial_cash
    for tnw in daily_tnw:
        daily_return.append(tnw / prev_tnw - 1)
        prev_tnw = tnw

    df_metrics = pd.DataFrame({
        "tnw": daily_tnw,
        "ret": daily_return
    })
    final_return = daily_tnw[-1] / initial_cash - 1
    daily_tnw_aug = [initial_cash] + daily_tnw
    performance_metrics = {
        "Final Return": final_return,
        "Volatility": df_metrics['ret'].std(),
        "Sharpe Ratio": (final_return - df_metrics['ret'].mean()) / df_metrics['ret'].std(),
        "Up Percentage": len([r for r in daily_return if r > 0]) / len(daily_return),
        "Max Drawdown": (min(daily_tnw_aug) - max(daily_tnw_aug)) / max(daily_tnw_aug),
        "Skewness": df_metrics['ret'].skew(),
        "Kurtosis": df_metrics['ret'].kurt()
    }

    return performance_metrics


def select_pairs_bulk():

    num_pairs = 40

    df = pd.read_csv("Training_Output/top_secret.csv")
    metrics = [m for m in df][3:]
    for metric in metrics:
        asc = (metric[0:3] == "SSD")
        pairs = PairTradeStrategy.select_pairs(df, num_pairs, metric, asc)
        PairTradeStrategy.dump_pairs(f"Pairs/top_{num_pairs}_pairs_by_{metric}.csv", pairs)
        print("Exported", metric)

    df = pd.read_csv("Training_Output/beta_secret.csv")
    pairs = PairTradeStrategy.select_pairs(df, num_pairs, "CoInt_rsq", False, "CoInt_beta")
    PairTradeStrategy.dump_pairs(f"Pairs/top_{num_pairs}_pairs_by_CoInt.csv", pairs)
    print("Exported", "CoInt")


def evaluate_bulk():

    config = util.load_config()

    num_pairs_limit = int(config.get("NUM_PAIRS_LIMIT", 0))

    exit_threshold = float(config["EXIT_THRESHOLD"])
    enter_threshold = float(config["ENTER_THRESHOLD"])
    stop_threshold = float(config["STOP_THRESHOLD"])
    
    backtesting_start = config['BACKTESTING_START']
    backtesting_end = config['BACKTESTING_END']
    
    thresholds = {
        'enter': enter_threshold,
        'stop': stop_threshold,
        'exit': exit_threshold
    }

    print(f"Back testing period = from {backtesting_start} to {backtesting_end}")
    print(f"num_pairs_limit={num_pairs_limit}, enter={enter_threshold}, stop={stop_threshold}, exit={exit_threshold}")

    for fname in os.listdir('Pairs'):
        print("\nEvaluating", fname, "...")
        pairs = PairTradeStrategy.load_pairs(os.path.join('Pairs', fname))
        if num_pairs_limit:
            pairs = pairs[:num_pairs_limit]
        strategy = PairTradeStrategy(thresholds, pairs)
        results = evaluate(strategy, config)
        for metric, value in results.items():
            print(metric, round(value, 5), sep='\t')



def evaluate_by_num_of_pairs(filename, lower, upper):
    '''
    Evaluate a portfolio by various number of pairs.
    '''

    config = util.load_config()
    exit_threshold = float(config["EXIT_THRESHOLD"])
    enter_threshold = float(config["ENTER_THRESHOLD"])
    stop_threshold = float(config["STOP_THRESHOLD"])
    backtesting_start = config['BACKTESTING_START']
    backtesting_end = config['BACKTESTING_END']
    
    thresholds = {
        'enter': enter_threshold,
        'stop': stop_threshold,
        'exit': exit_threshold
    }

    print(f"Back testing period = from {backtesting_start} to {backtesting_end}")
    print(f"enter={enter_threshold}, stop={stop_threshold}, exit={exit_threshold}")
    print("Evaluating", filename, f"with number of pairs ranging from {lower} to {upper}")
    print()
    
    pairs_original = PairTradeStrategy.load_pairs(filename)
    metric_columns = None
    for num_pairs_limit in range(lower, upper + 1):
        pairs = pairs_original[:num_pairs_limit]
        strategy = PairTradeStrategy(thresholds, pairs)
        results = evaluate(strategy, config)
        if metric_columns is None:
            metric_columns = list(results.keys())
            print('\t'.join(['Num_pairs'] + metric_columns))
        print(num_pairs_limit, end='\t')
        print('\t'.join([str(round(results[metric], 5)) for metric in metric_columns]))

  

if __name__ == "__main__":
    # evaluate_by_num_of_pairs("Pairs/top_40_pairs_by_CoInt.csv", 10, 30)
    evaluate_bulk()
    input("\nDone")



















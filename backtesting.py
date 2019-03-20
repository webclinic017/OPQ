
'''
Backtest the strategies.
'''

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

    

def cash_change(orders, stock_data):
    '''
    Calculate the change of cash as a result of a series of orders.
    Use the latest daily close price of the stocks as the settlement price.
    
    For now, accept any order with no margin requirement (you can buy/short however much you want).
    Also, ignore transaction cost.
    
    '''
    
    change = 0
    for code, quantity in orders.items():
        if not code in stock_data:
            raise Exception("Stock data not found for " + code)
        settlement_price = stock_data[code][-1:]['CLOSE'][0]
        # If quantity > 0, it means buying the instrument, so cash goes down. Vice versa.
        change += (-settlement_price) * int(quantity)

    return round(change, 2)



def evaluate(strategy):
    '''
    Evaluate the strategy.
    Returns (return, sharpe ratio, maximum dropdown)
    '''

    config = util.load_config()
    training_start = config['TRAINING_START']
    training_end = config['TRAINING_END']
    backtesting_start = config['BACKTESTING_START']
    backtesting_end = config['BACKTESTING_END']

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

    # Define initial cash and reset positions to empty
    initial_cash = 1000000
    cash = initial_cash
    strategy.initial_cash = initial_cash
    strategy.positions({})
    
    # First feed the "past" data, analyze the spread pattern, and then execute orders
    strategy.feed(stock_data_training)
    strategy.analyze_spread()
    orders = strategy.decide()
    cash += cash_change(orders, stock_data_training)
    strategy.positions(orders, incremental=True)

    # Then advance the timeline day by day
    for i in range(num_days_test):
        # Each day starts with some new data
        stock_data_single_day = {}
        for code, df in stock_data_backtesting.items():
            stock_data_single_day[code] = df[i : i + 1]
            day = stock_data_single_day[code].index[0]
        strategy.feed(stock_data_single_day)
        # Get and execute orders for the day
        orders = strategy.decide()
        cash += cash_change(orders, stock_data_single_day)
        strategy.positions(orders, incremental=True)

    # Clear positions, calculate the final cash and yield the overall return
    final_positions = strategy.positions()
    for code, df in stock_data_single_day.items():
        quantity_held = final_positions.get(code, 0)
        market_price = df[-1:]['CLOSE'][0]
    cash -= cash_change(strategy.positions(), stock_data_single_day)

    return cash / initial_cash - 1




import os

def evaluate_bulk():

    thresholds = {
        'enter': 2,
        'stop': 2.25,
        'exit': 1.75
    }

    for fname in os.listdir('Pairs'):
        print("\nEvaluating", fname, "...")
        strategy = PairTradeStrategy(thresholds, os.path.join('Pairs', fname))
        print("\nOverall return: ", round(evaluate(strategy) * 100, 2), "%", sep='')


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



if __name__ == "__main__":
    evaluate_bulk()
    input()



















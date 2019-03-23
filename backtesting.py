
'''
Backtest the strategies.
'''

import os
import argparse

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

def print_config(config):
    '''
    Print the key configurations for backtesting.
    '''

    backtesting_start = config.get('BACKTESTING_START', '?')
    backtesting_end = config.get('BACKTESTING_END', '?')
    exit_threshold = config.get('EXIT_THRESHOLD', '?')
    enter_threshold = config.get('ENTER_THRESHOLD', '?')
    stop_threshold = config.get('STOP_THRESHOLD', '?')

    print(f"Back-testing period=from {backtesting_start} to {backtesting_end}")
    print(f"enter={enter_threshold}, stop={stop_threshold}, exit={exit_threshold}")
    
    

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
    ret_std = df_metrics['ret'].std()
    performance_metrics = {
        "Final Return": final_return,
        "Volatility": ret_std,
        "Sharpe Ratio": (final_return - df_metrics['ret'].mean()) / ret_std if ret_std > 0 else np.nan,
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



def evaluate_cumulative_pairs(pairs, config, lower=None, upper=None):
    '''
    Evaluate pairs cumulatively of a portfolio.
    '''

   

    thresholds = {
        'enter': float(config["ENTER_THRESHOLD"]),
        'stop': float(config["STOP_THRESHOLD"]),
        'exit': float(config["EXIT_THRESHOLD"])
    }
    if lower is None:
        lower = [1, 1]
    lower_start = int(lower[0])
    lower_end = int(lower[1]) if len(lower) > 1 else lower_start
    if upper is None:
        upper = [len(pairs), len(pairs)]
    upper_start = int(upper[0])
    upper_end = int(upper[1]) if len(upper) > 1 else upper_start
    
    pairs_original = pairs
    columns = None
    
    for s in range(lower_start - 1, lower_end):
        for e in range(upper_start, upper_end + 1):
            pairs = pairs_original[s : e]
            strategy = PairTradeStrategy(thresholds, pairs)
            results = evaluate(strategy, config)
            if columns is None:
                columns = ['Start Pair', 'End Pair'] + list(results.keys())
                print(*columns, sep='\t')
            results['Start Pair'] = s
            results['End Pair'] = e
            print(*[results[c] if type(results[c]) is str else round(results[c], 5) for c in columns], sep='\t')



def evaluate_individual_pairs(pairs, config, lower=None, upper=None):
    '''
    Evaluate pairs individually of a portfolio.
    '''

    thresholds = {
        'enter': float(config["ENTER_THRESHOLD"]),
        'stop': float(config["STOP_THRESHOLD"]),
        'exit': float(config["EXIT_THRESHOLD"])
    }
    if lower is None:
        lower = [1]
    lower = int(lower[0])
    if upper is None:
        upper = [len(pairs)]
    upper = int(upper[0])
    columns = None
    for pair in pairs[lower - 1 : upper]:
        strategy = PairTradeStrategy(thresholds, [pair])
        results = evaluate(strategy, config)
        if columns is None:
            columns = ['Stock_1', 'Stock_2'] + list(results.keys())
            print(*columns, sep='\t')
        results['Stock_1'] = pair['Stock_1']
        results['Stock_2'] = pair['Stock_2']
        print(*[results[c] if type(results[c]) is str else round(results[c], 5) for c in columns], sep='\t')
        


if __name__ == "__main__":

    config = util.load_config()

    parser = argparse.ArgumentParser()
    parser.add_argument('-f', '--file', dest="in_file",
                        default=None, help="A csv file that specifies a list of stock pairs. Could also be a folder of csv files.")
    parser.add_argument('-d', '--directory', '--folder', dest="in_directory",
                        default="Pairs/", help="A folder of csv files, each specifying a list of stock pairs.")
    parser.add_argument('-i', '--individual', dest="indiv",
                        default=False, action='store_true', help="Evaluate pairs individually.")
    parser.add_argument('-p', '--pair', dest="pairs",
                        default=None, action='append', nargs='+', help="A specific pair (Stock_1, Stock_2[, beta]) to evaluate.")
    parser.add_argument('-l', '--lower', dest="lower",
                        default=None, nargs='+', help="The lower bound of number of pairs or the starting pair indices.")
    parser.add_argument('-u', '--upper', dest="upper",
                        default=None, nargs='+', help="The upper bound of number of pairs or the ending pair indices.")
    config_params = [
        "BACKTESTING_START",
        "BACKTESTING_END",
        "EXIT_THRESHOLD",
        "ENTER_THRESHOLD",
        "STOP_THRESHOLD",
        "TX_COST_PER_SHARE",
        "TX_COST_PER_DOLLAR"
    ]
    for param in config_params:
        parser.add_argument(f'--{param}', dest=param,
                            default=None, help="Overwrite this parameter in config.txt")
    args = parser.parse_args()
    
    for param in config_params:
        if param in config and getattr(args, param) is not None:
            config[param] = getattr(args, param)
    print_config(config)
    print()

    if args.in_file:
        pairs = PairTradeStrategy.load_pairs(args.in_file)
        if args.indiv:
            evaluate_individual_pairs(pairs, config, args.lower, args.upper)
        else:
            evaluate_cumulative_pairs(pairs, config, args.lower, args.upper)
    elif args.pairs:
        pairs = []
        for pair in args.pairs:
            pairs.append({
                'Stock_1': pair[0],
                'Stock_2': pair[1],
                'beta': float(pair[2] if len(pair) >= 3 else 1)
            })
        if args.indiv:
            evaluate_individual_pairs(pairs, config, args.lower, args.upper)
        else:
            evaluate_cumulative_pairs(pairs, config, args.lower, args.upper)
    else:
        for fname in os.listdir(args.in_directory):
            print("File:", fname)
            fname = os.path.join(args.in_directory, fname)
            pairs = PairTradeStrategy.load_pairs(fname)
            if args.indiv:
                evaluate_individual_pairs(pairs, config, args.lower, args.upper)
            else:
                evaluate_cumulative_pairs(pairs, config, args.lower, args.upper)
            print()

    input("\nDone")















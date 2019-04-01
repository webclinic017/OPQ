
'''
Backtest the strategies.
'''

import os
import sys
import argparse

import pandas as pd
import numpy as np

import util
from strategy import *



def print_orders(orders, stock_data, indent=1):
    '''
    Print the order in human-readable format. For debug purpose
    '''

    for code, quantity in orders.items():
        price = stock_data[code]['CLOSE']
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
    enter2_threshold = config.get('ENTER2_THRESHOLD', '?')
    enter3_threshold = config.get('ENTER3_THRESHOLD', '?')
    stop_threshold = config.get('STOP_THRESHOLD', '?')
    enter_allocation = config.get('ENTER_ALLOCATION', '?')
    enter2_allocation = config.get('ENTER2_ALLOCATION', '?')
    enter3_allocation = config.get('ENTER3_ALLOCATION', '?')
    max_leverage = config.get('MAX_LEVERAGE', '?')

    print(f"Back-testing period=from {backtesting_start} to {backtesting_end}")
    print(f"enter={enter_threshold}, {enter2_threshold}, {enter3_threshold}, stop={stop_threshold}, exit={exit_threshold}")
    print(f"max_leverage={max_leverage}, allocations={enter_allocation}, {enter2_allocation}, {enter3_allocation}")
    

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
        settlement_price = stock_data[code]['CLOSE']
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
        settlement_price = stock_data[code]['CLOSE']
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
    initial_cash = float(config['INITIAL_CASH'])
    max_leverage = float(config['MAX_LEVERAGE'])
    tx_cost_per_share = float(config['TX_COST_PER_SHARE'])
    tx_cost_per_dollar = float(config['TX_COST_PER_DOLLAR'])
    risk_free_rate = float(config['RISK_FREE_RATE']) * (pd.to_datetime(backtesting_end) - pd.to_datetime(backtesting_start)).days / 360

    # Prepare the data for the strategy
    stock_codes = set()
    for pair in strategy.pairs:
        stock_codes.add(pair.X)
        stock_codes.add(pair.Y)
    stock_data = util.load_stock_data(config['STOCK_DATA_FOLDER'], list(stock_codes))
    
    strategy.feed(stock_data)
    strategy.analyze_spread(training_start, training_end)
    strategy.allocate_money(initial_cash * max_leverage)

    cash = initial_cash
    daily_tnw = []
    leverages = []
    
    # Advance the timeline day by day
    testing_dates = [d for d in list(stock_data.values())[0].index if backtesting_start <= d and d <= backtesting_end]
    testing_dates.sort()
    for date in testing_dates:
        strategy.now(date)
        # Get and execute orders for the day
        orders = strategy.decide()
        stock_data_single_day = {}
        for code, df in stock_data.items():
            stock_data_single_day[code] = df.loc[date]
        cash += cash_change(orders, stock_data_single_day)
            
        # Update the orders in strategy
        strategy.positions(orders, incremental=True)
        # Record the net worth and return
        total_net_worth = cash + net_worth(strategy.positions(), stock_data_single_day)

        # Calculate the leverage
        market_value = cash
        for stock, quantity in strategy.positions().items():
            if quantity > 0:
                market_value += quantity * stock_data_single_day[stock]['CLOSE']
        leverage = market_value / total_net_worth
        leverages.append(leverage)
        
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
        "Sharpe Ratio": (final_return - risk_free_rate) / ret_std if ret_std > 0 else np.nan,
        "Up Percentage": len([r for r in daily_return if r > 0]) / len(daily_return),
        "Max Drawdown": (min(daily_tnw_aug) - max(daily_tnw_aug)) / max(daily_tnw_aug),
        "Skewness": df_metrics['ret'].skew(),
        "Kurtosis": df_metrics['ret'].kurt(),
        "Avg Leverage": sum(leverages) / len(leverages),
        "Max Leverage": max(leverages)
    }

    return performance_metrics



def evaluate_cumulative_pairs(pairs, config, lower=None, upper=None, tx_log=False):
    '''
    Evaluate pairs cumulatively of a portfolio.
    Returns a pandas DataFrame Object contains the evaluation result.
    '''

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
    df_result = pd.DataFrame()
    for s in range(lower_start - 1, lower_end):
        for e in range(upper_start, upper_end + 1):
            pairs = pairs_original[s : e]
            strategy = PairTradeStrategy(pairs, thresholds, allocations)
            results = evaluate(strategy, config)
            if tx_log:
                results = strategy.transaction_history()
            if columns is None:
                columns = ['Start Pair', 'End Pair'] + list(results.keys())
                if not tx_log:
                    print(*columns, sep='\t')
                df_result = pd.DataFrame(columns=columns)
            results['Start Pair'] = s
            results['End Pair'] = e
            df_result = df_result.append(results, ignore_index=True, sort=False)
            if tx_log and len(results) > 0:
                print(results.set_index("Date"))
                print()
            else:
                print(*[results[c] if type(results[c]) is str else round(results[c], 5) for c in columns], sep='\t')

    return df_result


def evaluate_individual_pairs(pairs, config, lower=None, upper=None, tx_log=False):
    '''
    Evaluate pairs individually of a portfolio.
    '''

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
    if lower is None:
        lower = [1]
    lower = int(lower[0])
    if upper is None:
        upper = [len(pairs)]
    upper = int(upper[0])

    columns = None
    df_result = pd.DataFrame()
    for pair in pairs[lower - 1 : upper]:
        strategy = PairTradeStrategy([pair], thresholds, allocations)
        results = evaluate(strategy, config)
        if tx_log:
            results = strategy.transaction_history()
        if columns is None:
            columns = ['Stock_1', 'Stock_2'] + list(results.keys())
            if not tx_log:
                print(*columns, sep='\t')
            df_result = pd.DataFrame(columns=columns)
        results['Stock_1'] = pair['Stock_1']
        results['Stock_2'] = pair['Stock_2']
        df_result = df_result.append(results, ignore_index=True, sort=False)
        if tx_log and len(results) > 0:
            print(results.set_index("Date"))
            print()
        else:
            print(*[results[c] if type(results[c]) is str else round(results[c], 5) for c in columns], sep='\t')
        
    return df_result


def main(*argv, **kwargs):

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
    parser.add_argument('-o', '--out', dest="out_file",
                        default=None, help="The file to store backtesting results.")
    parser.add_argument('-t', '--transaction', dest="transaction_log",
                        default=False, action='store_true', help="Show transaction log.")
    
    for param in config.keys():
        parser.add_argument(f'--{param}', dest=param,
                            default=None, help="Overwrite this parameter in config.txt")
    args = parser.parse_args(argv)
    
    for param in config:
        if getattr(args, param) is not None:
            config[param] = getattr(args, param)
        if param in kwargs:
            config[param] = kwargs[param]
    print_config(config)

    result = pd.DataFrame()
    if args.in_file:
        pairs = PairTradeStrategy.load_pairs(args.in_file)
        if args.indiv:
            result = evaluate_individual_pairs(pairs, config, args.lower, args.upper, args.transaction_log)
        else:
            result = evaluate_cumulative_pairs(pairs, config, args.lower, args.upper, args.transaction_log)
    elif args.pairs:
        pairs = []
        for pair in args.pairs:
            pairs.append({
                'Stock_1': pair[0],
                'Stock_2': pair[1],
                'beta': float(pair[2] if len(pair) >= 3 else 1)
            })
        if args.indiv:
            result = evaluate_individual_pairs(pairs, config, args.lower, args.upper, args.transaction_log)
        else:
            result = evaluate_cumulative_pairs(pairs, config, args.lower, args.upper, args.transaction_log)
    else:
        for fname in os.listdir(args.in_directory):
            print("\nFile:", fname)
            fname = os.path.join(args.in_directory, fname)
            if not fname[-4:] == '.csv':
                continue
            pairs = PairTradeStrategy.load_pairs(fname)
            if args.indiv:
                result_cur = evaluate_individual_pairs(pairs, config, args.lower, args.upper, args.transaction_log)
            else:
                result_cur = evaluate_cumulative_pairs(pairs, config, args.lower, args.upper, args.transaction_log)
            result_cur.insert(0, "File", fname)
            result = pd.concat([result, result_cur])

    if args.out_file is not None:
        result.to_csv(args.out_file, index=False)
                
    print("\nDone")



if __name__ == "__main__":
    main(*sys.argv[1:])








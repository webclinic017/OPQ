
'''
The main file to do routine trading.

It first fetches the pairs info in the portfolio,
then fetch the involved stocks' prices,
then calculate the spread and target positions,
and finally place orders through IB API.

'''

import os
import argparse
import datetime
import collections
import inspect
import logging
import time


import pandas as pd

from ibapi import wrapper
from ibapi.client import EClient
from ibapi.utils import iswrapper
# types
from ibapi.common import * # @UnusedWildImport
from ibapi.order_condition import * # @UnusedWildImport
from ibapi.contract import * # @UnusedWildImport
from ibapi.order import * # @UnusedWildImport
from ibapi.order_state import * # @UnusedWildImport
from ibapi.execution import Execution
from ibapi.execution import ExecutionFilter
from ibapi.commission_report import CommissionReport
from ibapi.ticktype import * # @UnusedWildImport
from ibapi.tag_value import TagValue
from ibapi.account_summary_tags import *
from ibapi.scanner import ScanData


from util import *
from strategy import *
import data



EXCHANGES_MAPPING = {
    "OQ": "ISLAND",
    "N": "NYSE",
    "Z": "BATS",
    "PK": "PINK",
    "EI": "IEX",
    "A": "AMEX"
}
EXCHANGES_MAPPING_INVERSED = {
    'NASDAQ': 'OQ',
    'NYSE': 'N',
    'BATS': 'Z',
    'PINK': 'PK',
    'IEX': 'EI',
    'AMEX': 'A'
}



def place_orders(orders):
    '''
    Submit orders.
    '''
    
    app = TestApp(orders_to_place=orders)

    app.connect("127.0.0.1", 7497, clientId=0)

    app.run()


def place_orders_by_target_positions(target_positions, account):
    '''
    A better way to place orders: given the target positions,
    let the IB API fetches the actual current positions and derive the orders to place.
    '''

    app = TestApp(account=account, target_positions=target_positions)

    app.connect("127.0.0.1", 7497, clientId=0)

    app.run()




# ! [socket_declare]
class TestClient(EClient):
    
    def __init__(self, wrapper):
        EClient.__init__(self, wrapper)
        # ! [socket_declare]

        # how many times a method is called to see test coverage
        self.clntMeth2callCount = collections.defaultdict(int)
        self.clntMeth2reqIdIdx = collections.defaultdict(lambda: -1)
        self.reqId2nReq = collections.defaultdict(int)
        self.setupDetectReqId()
        

    def countReqId(self, methName, fn):
        def countReqId_(*args, **kwargs):
            self.clntMeth2callCount[methName] += 1
            idx = self.clntMeth2reqIdIdx[methName]
            if idx >= 0:
                sign = -1 if 'cancel' in methName else 1
                self.reqId2nReq[sign * args[idx]] += 1
            return fn(*args, **kwargs)

        return countReqId_
    

    def setupDetectReqId(self):

        methods = inspect.getmembers(EClient, inspect.isfunction)
        for (methName, meth) in methods:
            if methName != "send_msg":
                # don't screw up the nice automated logging in the send_msg()
                self.clntMeth2callCount[methName] = 0
                # logging.debug("meth %s", name)
                sig = inspect.signature(meth)
                for (idx, pnameNparam) in enumerate(sig.parameters.items()):
                    (paramName, param) = pnameNparam # @UnusedVariable
                    if paramName == "reqId":
                        self.clntMeth2reqIdIdx[methName] = idx

                setattr(TestClient, methName, self.countReqId(methName, meth))



# ! [ewrapperimpl]
class TestWrapper(wrapper.EWrapper):
    # ! [ewrapperimpl]
    def __init__(self):
        wrapper.EWrapper.__init__(self)

        self.wrapMeth2callCount = collections.defaultdict(int)
        self.wrapMeth2reqIdIdx = collections.defaultdict(lambda: -1)
        self.reqId2nAns = collections.defaultdict(int)
        self.setupDetectWrapperReqId()

    # TODO: see how to factor this out !!

    def countWrapReqId(self, methName, fn):
        def countWrapReqId_(*args, **kwargs):
            self.wrapMeth2callCount[methName] += 1
            idx = self.wrapMeth2reqIdIdx[methName]
            if idx >= 0:
                self.reqId2nAns[args[idx]] += 1
            return fn(*args, **kwargs)

        return countWrapReqId_

    def setupDetectWrapperReqId(self):

        methods = inspect.getmembers(wrapper.EWrapper, inspect.isfunction)
        for (methName, meth) in methods:
            self.wrapMeth2callCount[methName] = 0
            # logging.debug("meth %s", name)
            sig = inspect.signature(meth)
            for (idx, pnameNparam) in enumerate(sig.parameters.items()):
                (paramName, param) = pnameNparam # @UnusedVariable
                # we want to count the errors as 'error' not 'answer'
                if 'error' not in methName and paramName == "reqId":
                    self.wrapMeth2reqIdIdx[methName] = idx

            setattr(TestWrapper, methName, self.countWrapReqId(methName, meth))

            # print("TestClient.wrapMeth2reqIdIdx", self.wrapMeth2reqIdIdx)



# ! [socket_init]
class TestApp(TestWrapper, TestClient):
    
    def __init__(self, config={}):
        TestWrapper.__init__(self)
        TestClient.__init__(self, wrapper=self)
        # ! [socket_init]
        self.nKeybInt = 0
        self.started = False
        self.nextValidOrderId = None
        self.permId2ord = {}
        self.reqId2nErr = collections.defaultdict(int)
        self.globalCancelOnly = False
        self.simplePlaceOid = None

        self.config = config
        self.account = self.config.get('ACCOUNT_ID', None)
        self.current_positions = {}
        self.orders_to_place = {}
        self.stock_prices = {}


    def write_log(self, msg):
        '''
        Print the log to screen and save the same log to log file.
        '''

        log_file = self.config['LOG_FILE']
        write_log(msg, log_file)


    def init_strategy(self):
        '''
        Construct the PairTradeStrategy instance.
        '''

        thresholds = [
            float(self.config["EXIT_THRESHOLD"]),
            float(self.config["ENTER_THRESHOLD"]),
            float(self.config["ENTER2_THRESHOLD"]),
            float(self.config["ENTER3_THRESHOLD"]),
            float(self.config["STOP_THRESHOLD"])
        ]
        allocations = [
            float(self.config["ENTER_ALLOCATION"]),
            float(self.config["ENTER2_ALLOCATION"]),
            float(self.config["ENTER3_ALLOCATION"])
        ]
        asset_file = self.config["ASSET_FILE"]

        self.strategy = PairTradeStrategy([], thresholds, allocations)
        self.strategy.load_pair_info(asset_file)
        
      
    def start(self):
        '''
        Starting the app.
        '''
        
        if self.started:
            return
        self.started = True

        self.write_log("Program Start")

        # Initial the PairTradeStrategy instance
        self.init_strategy()
        self.write_log("Pair assets loaded")

        # For each stock in self.strategy.pairs, get its realitime price
        self.request_stock_data()
        self.write_log("Stock prices fetched")

        # Get the current by-stock positions
        self.current_positions = {}
        self.reqPositions()
        # When it's done, self.positionEnd() will be called. See what happens next there
        

    def nextOrderId(self):
        oid = self.nextValidOrderId
        self.nextValidOrderId += 1
        return oid

        
    # ! [nextvalidid]
    def nextValidId(self, orderId: int):
        super().nextValidId(orderId)
        self.nextValidOrderId = orderId
        self.start()
        

    def position(self, account: str, contract: Contract, position: float, avgCost: float):
        '''
        Callback for each instrument position fetched.
        '''
        
        super().position(account, contract, position, avgCost)
        if self.account is None or account == self.account:
            stock_code = contract.symbol + '.' + EXCHANGES_MAPPING_INVERSED[contract.exchange]
            self.current_positions[stock_code] = int(position)

            
    def positionEnd(self):
        '''
        Callback when all instrument positions have been fetched.
        '''
        
        super().positionEnd()

        self.write_log("Current positions fetched")

        # Feed the PairTradeStrategy instance the current by-stock positions
        self.strategy.positions(self.current_positions)
        
        # Ask for the orders to place
        self.orders_to_place = self.strategy.decide(self.stock_prices)
        self.write_log("Orders derived")
        self.print_orders()
        
        # Place all orders
        self.place_all_orders()
        self.write_log("All orders placed")

        # File I/O to save positions and orders placed
        ustime = get_us_time('%Y_%m_%d_%H_%M_%S')
        asset_file = self.config["ASSET_FILE"]
        asset_history_folder = self.config["ASSET_HISTORY_FOLDER"]
        transaction_history_folder = self.config["TRANSACTION_HISTORY_FOLDER"]
        asset_file_today = os.path.join(asset_history_folder, f"asset_{ustime}.csv")
        tx_file_today = os.path.join(transaction_history_folder, f"transactions_{ustime}.csv")
        if not os.path.isdir(asset_history_folder):
            os.makedirs(asset_history_folder)
        if not os.path.isdir(transaction_history_folder):
            os.makedirs(transaction_history_folder)
        self.save_positions_to_file(asset_file)
        self.save_positions_to_file(asset_file_today)
        self.save_orders_to_file(tx_file_today)

        self.write_log(f"Today's orders have been saved to {tx_file_today}")
        self.write_log(f"Latest positions have been saved to {asset_file} and {asset_file_today}")
        
        self.write_log(f"All done. Program Exit")


    def request_stock_data(self):
        '''
        Since I haven't subsribed to IB's livestream data feed, here I use Yahoo Finance to get real-time stock data.
        '''

        stocks = set()
        for pair in self.strategy.pairs:
            stocks.add(pair.X)
            stocks.add(pair.Y)
        self.stock_prices = data.get_current_price(list(stocks))
        

    def place_all_orders(self):
        '''
        Place all orders of self.orders_to_place
        '''
        
        for stock_code, quantity in self.orders_to_place.items():
            if quantity == 0:
                continue
            symbol, exchange = stock_code.split('.')
            contract = Contract()
            contract.symbol = symbol
            contract.secType = "STK"
            contract.currency = "USD"
            contract.exchange = "SMART"
            if exchange in EXCHANGES_MAPPING:
                contract.primaryExchange = EXCHANGES_MAPPING[exchange]

            order = Order()
            order.action = "BUY" if quantity > 0 else "SELL"
            order.orderType = "MKT"
            order.totalQuantity = abs(quantity)

            self.placeOrder(self.nextOrderId(), contract, order)


    def print_orders(self):
        '''
        Print out the orders.
        ''' 

        print("Today's Orders:")
        print("Action  Stock       Quantity    Price")
        print("------  -----       --------    -----")
        tx_df = pd.DataFrame(columns=["Direction", "Stock", "Quantity", "Price"])
        for stock, quantity in self.orders_to_place.items():
            if quantity == 0:
                continue
            direction = "Buy" if quantity > 0 else "Sell"
            quantity = abs(quantity)
            price = self.stock_prices[stock]
            print(f"{direction:8}{stock:12}{str(quantity):12}{str(price):12}")
        print("\nEnd of Today's Orders")
        

    def save_orders_to_file(self, filepath):
        '''
        Save the orders placed to the file specified by {filepath}.
        '''

        tx_df = pd.DataFrame(columns=["Direction", "Stock", "Quantity", "Price"])
        for stock, quantity in self.orders_to_place.items():
            if quantity == 0:
                continue
            direction = "Buy" if quantity > 0 else "Sell"
            quantity = abs(quantity)
            price = self.stock_prices[stock]
            tx_df = tx_df.append({
                "Direction": direction, "Stock": stock, "Quantity": quantity, "Price": price
            }, ignore_index=True)
        tx_df.to_csv(filepath, index=False)


    def save_positions_to_file(self, filepath):
        '''
        Save the latest aseet positions to the file specified by {filepath}.
        '''

        self.strategy.dump_pair_info(filepath)




def main(*argv):
    
    config = load_config("asset_config.txt")
    
    app = TestApp(config)
    app.connect("127.0.0.1", 7497, clientId=0)
    app.run()


if __name__ == "__main__":
    main()





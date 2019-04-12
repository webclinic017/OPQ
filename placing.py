
'''
Placing orders to IB using IB API.
'''


import argparse
import datetime
import collections
import inspect

import logging
import time
import os.path

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
    
    def __init__(self, orders_to_place={}, target_positions=None, account=None):
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

        self.orders_to_place = orders_to_place
        self.target_positions = target_positions
        self.current_positions = {}
        self.account = account
        
      
    def start(self):
        if self.started:
            return
        self.started = True

        if self.target_positions is not None:
            self.reqPositions()
        else:
            self.placeAllOrders()


    def position(self, account: str, contract: Contract, position: float, avgCost: float):
        super().position(account, contract, position, avgCost)
        if self.account is None or account == self.account:
            stock_code = contract.symbol + '.' + EXCHANGES_MAPPING_INVERSED[contract.exchange]
            self.current_positions[stock_code] = int(position)

            
    def positionEnd(self):
        super().positionEnd()
        self.orders_to_place = {}
        for stock_code, target_position in self.target_positions.items():
            current_position = self.current_positions.get(stock_code, 0)
            if target_position != current_position:
                self.orders_to_place[stock_code] = target_position - current_position
        self.placeAllOrders()
        
            

    def placeAllOrders(self):
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


    def nextOrderId(self):
        oid = self.nextValidOrderId
        self.nextValidOrderId += 1
        return oid

        
    # ! [nextvalidid]
    def nextValidId(self, orderId: int):
        super().nextValidId(orderId)
        self.nextValidOrderId = orderId
        self.start()








'''
Get stock data.
'''

from urllib import request
from urllib import parse
import os
import time
import datetime
import sched
import re
import threading

import pandas as pd
try:
    import eikon as ek
except:
    pass

import util


def update_daily_data(stocks):
    '''
    Update the daily data for the stocks specified.
    '''
    
    s = sched.scheduler(time.time, time.sleep)
    ek.set_app_key('66063f6d35e4453ebba0696f40307bc61e7172f2')

    config = util.import_config()
    stock_data_folder = config['STOCK_DATA_FOLDER']

    # Get the current moment date in US
    end_date = util.get_us_time()

    for stock_code in stocks:
        fname = os.path.join(stock_data_folder, stock_code + '.csv')
        if os.path.isfile(fname):
            stock_df = pd.read_csv(fname)
            start_date = max(stock_df["Date"])
        else:
            stock_df = pd.DataFrame()
            start_date = end_date
        try: 
            new_df = ek.get_timeseries(stock_code, start_date=start_date, end_date=end_date).reset_index()
            new_df["Date"] = new_df["Date"][0:10]
            stock_df = stock_df.append(new_df)
            stock_df = stock_df.reindex(columns=["Date", "HIGH", "CLOSE", "LOW", "OPEN", "COUNT", "VOLUME"])
            stock_df.drop_duplicates(["Date"], inplace=True)
            stock_df.to_csv(fname, index=False)
            #time.sleep(5.0 - ((time.time() - start_time) % 5.0))
            print("Successfully updated daily date for", stock_code)
        except Exception as e:
            print("Error in acquiring ", stock_code, ":")
            print(str(e))



def get_current_price(stocks):
    '''
    Get the real-time price from yahoo finance. "Real-time" is relative as yahoo finance data has delay of 15min.
    Return a dict with key being stock code and value being the current price (for those succesful).
    '''

    WorkerThread.Stock_Data = {}
    workers = []
    for stock in stocks:
        worker = WorkerThread(stock_code=stock)
        workers.append(worker)
        worker.start()
    for worker in workers:
        worker.join()

    return WorkerThread.Stock_Data.copy()




class WorkerThread(threading.Thread):

    Stock_Data = {}

    @staticmethod
    def get_response(url, headers=dict()):
        '''
        Ger raw response in string using HTTP Request.
        '''
        
        headers['User-Agent'] = "Mozilla/5.0 (X11; Linux i686) AppleWebKit/537.17 (KHTML, like Gecko) Chrome/24.0.1312.27 Safari/537.17"
        try:
            req = request.Request(url, headers=headers)
            resp = request.urlopen(req)
            encoding = 'utf-8'
            for val in resp.getheader('Content-Type').split(';'):
                if 'charset' in val:
                    encoding = val.split('=')[-1].strip()
            if resp.readable():
                return resp.read().decode(encoding)
        except:
            return ''


    def __init__(self, stock_code):
        '''
        Each worker (thread) is responsible for getting one stock's price.
        '''
        
        super().__init__()
        self.stock_code = stock_code
        

    def run(self):
        '''
        Crawl the webpage and find the stock's current price.
        '''
        
        ticker = self.stock_code.split('.')[0]
        url = f"https://finance.yahoo.com/quote/{ticker}"
        rdata = WorkerThread.get_response(url)
        res = re.findall(r"<span.*?data-reactid=['\"]34['\"]>([0-9\.,]*?)</span>", rdata)
        if res:
            WorkerThread.Stock_Data[self.stock_code] = float(res[0].replace(',',''))





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


def update_all():
    '''
    Update the daily data for all stocks in the stock data folder.
    '''

    config = util.import_config()

    s = sched.scheduler(time.time, time.sleep)
    ek.set_app_key('66063f6d35e4453ebba0696f40307bc61e7172f2')
    
    stock_folder = config['STOCK_DATA_FOLDER']

    # Get the current date in US
    tz_us = datetime.timezone(datetime.timedelta(hours=-5))
    tz_hk = datetime.timezone(datetime.timedelta(hours=8))
    dt = datetime.datetime(*time.localtime()[:6], tzinfo=tz_hk)
    end_date = dt.astimezone(tz_us).strftime("%Y-%m-%d")

    for fname in os.listdir(stock_folder):
        stock_code = util.get_stock_code(fname)
        stock_df = pd.read_csv(os.path.join(stock_folder, fname))
        start_date = max(stock_df["Date"])
        try: 
            new_df = ek.get_timeseries(stock_code, start_date=start_date, end_date=end_date).reset_index()
            new_df["Date"] = new_df["Date"][0:10]
            stock_df = stock_df.append(new_df)
            stock_df = stock_df.reindex(columns=["Date", "HIGH", "CLOSE", "LOW", "OPEN", "COUNT", "VOLUME"])
            stock_file.to_csv(os.path.join(stock_folder, fname), index=False)
            #time.sleep(5.0 - ((time.time() - start_time) % 5.0))
        except Exception as e:
            print("Error in acquiring data for", stock_code)
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
        super().__init__()
        self.stock_code = stock_code        

    def run(self):
        ticker = self.stock_code.split('.')[0]
        url = f"https://finance.yahoo.com/quote/{ticker}"
        rdata = WorkerThread.get_response(url)
        res = re.findall(r"<span.*?data-reactid=['\"]34['\"]>([0-9\.,]*?)</span>", rdata)
        if res:
            WorkerThread.Stock_Data[self.stock_code] = float(res[0].replace(',',''))
        else:
            print("Failed to get data for", self.stock_code)

# Testing
stocks_test = [
    'AFG.N',
    'ROP.N',
    'BAX.N',
    'JOBS.OQ',
    'MU.OQ',
    'OC.N',
    'INXN.N',
    'TEL.N',
    'SNPS.OQ',
    'RCL.N',
    'ISRG.OQ',
    'WTFC.OQ',
    'CDW.OQ',
    'MKSI.OQ',
    'WLK.N',
    'HEI.N',
    'SITC.N',
    'BAC.N',
    'CCL.N',
    'VMW.N',
    'MSFT.OQ',
    'HWC.OQ',
    'RGA.N',
    'CBOE.Z',
    'ENTG.OQ',
    'ANSS.OQ',
    'MMM.N',
    'AMAT.OQ',
    'GDOT.N',
    'HPQ.N',
    'CFR.N',
    'HLT.N',
    'MCHP.OQ',
    'HRS.N',
    'NVR.N',
    'PH.N',
    'APH.N',
    'BBT.N',
    'JPM.N',
    'MAR.OQ',
    'TTWO.OQ',
    'LRCX.OQ',
    'AME.N',
    'TDY.N',
    'CMA.N',
    'HON.N',
    'TEX.N',
    'NTRS.OQ',
    'RF.N',
    'A.N',
    'MA.N',
    'DAN.N',
    'SNV.N',
    'MS.N',
    'MPWR.OQ',
    'MTB.N',
    'ROK.N',
    'CGNX.OQ',
    'LMT.N',
    'MSCI.N',
    'FITB.OQ',
    'V.N',
    'KIM.N',
    'HBAN.OQ',
    'AABA.OQ',
    'SLAB.OQ',
    'USB.N',
    'EWBC.OQ',
    'EV.N',
    'LNC.N',
    'RTN.N',
    'ADBE.OQ',
    'MCO.N',
    'RJF.N',
    'CAT.N',
    'MTG.N',
    'BWXT.N',
    'ITW.N',
    'HTHT.OQ',
    'MTN.N'
]
    





'''
Get stock data.
'''

from urllib import request
from urllib import parse
import re
import threading



def get_stock_prices(stocks):
    '''
    Get the real-time price for each stock. "Real-time" is relative as yahoo data has delay of 15min.
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



class WorkerThread(threading.Thread):

    Stock_Data = {}

    def __init__(self, stock_code):
        super().__init__()
        self.stock_code = stock_code        

    def run(self):
        ticker = self.stock_code.split('.')[0]
        url = f"https://finance.yahoo.com/quote/{ticker}"
        rdata = get_response(url, headers=dict())
        res = re.findall(r"<span.*?data-reactid=['\"]34['\"]>([0-9\.,]*?)</span>", rdata)
        if res:
            WorkerThread.Stock_Data[self.stock_code] = float(res[0].replace(',',''))
        else:
            print("Failed to get data for", self.stock_code)


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
    




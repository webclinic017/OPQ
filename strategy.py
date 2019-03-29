
'''
Implementations of (daily) Pair's Trading strategies.

'''

import time

import pandas as pd



class Strategy:
    '''
    The abstract base class strategy.
    '''

    def __init__(self):
        self._positions = {}
        self._stock_data = {}
        self.today = time.strftime("%Y-%m-%d")


    def now(self, date=None):
        '''
        Get/Set the point of time of the strategy.
        {date} should be a string
        '''
        if date is not None:
            self.today = date
        return self.today
    

    def feed(self, stock_data):
        '''
        Feed the strategy with latest stock data.
        '''

        for stock, stock_df in stock_data.items():
            if stock in self._stock_data:
                df = pd.concat([self._stock_data[stock], stock_df])
                df = df[~df.index.duplicated(keep='last')]
                self._stock_data[stock] = df
            else:
                self._stock_data[stock] = stock_df


    def positions(self, param_positions=None, incremental=False):
        '''
        Get or set the stock positions.
        The object representing the positions is of type dict. The key is the stock code
        and the value is the number of shares in held.
        A positive value indicates a LONG position while a negative value indicates a SHORT position.
        '''
        
        if not param_positions is None:
            if incremental:
                for stock, position in param_positions.items():
                    if not stock in self._positions:
                        self._positions[stock] = 0
                    self._positions[stock] += position
            else:
                self._positions = param_positions.copy()
        return self._positions.copy()
        

    def decide(self):
        '''
        Make buy / sell / short decisions.

        This method should be overrided in subclass.
        '''

        return {}



class HoldingPair:
    '''
    A pair of stock in held.
    '''

    def __init__(self, X, Y, beta=1, money_allocated=0):
        self.X = X
        self.Y = Y
        self.beta = beta
        self.money_allocated = money_allocated
        self.spread_mean = 0
        self.spread_std = 0
        self.level = 0
        self.X_quantity = 0
        self.Y_quantity = 0


class PairTradeStrategy(Strategy):
    '''
    Pair trading strategy.
    '''

    @staticmethod
    def select_pairs(file, num_pairs, metric=None, ascending=False, unique=False, beta=None, filter=None):
        '''
        Choose pairs of stocks.
        '''

        if type(file) is str:
            df = pd.read_csv(file)
        else:
            df = file
        stock_codes = {} # No repeat stock
        if metric is not None:
            df.sort_values(metric, ascending=ascending, inplace=True)
        pairs = []
        for index, row in df.iterrows():
            if len(pairs) >= num_pairs:
                break
            stock_x = row['Stock_1']
            stock_y = row['Stock_2']
            if unique and stock_x in stock_codes or stock_y in stock_codes:
                continue
            if filter and not filter(row):
                continue
            stock_codes[stock_x] = True
            stock_codes[stock_y] = True
            beta_ = row[beta] if beta else 1
            pairs.append({
                'Stock_1': stock_x,
                'Stock_2': stock_y,
                'beta': beta_
            })
        return pairs
    

    @staticmethod
    def dump_pairs(filename, pairs):
        '''
        Export the pairs into a csv file.
        '''
        df = pd.DataFrame(columns=["Stock_1", "Stock_2", "beta"])
        for pair in pairs:
            df = df.append(pair, ignore_index=True)
        df.to_csv(filename, index=False)

  
    @staticmethod
    def load_pairs(filename):
        '''
        Load pairs from a csv file.
        '''
        
        pairs = []
        df = pd.read_csv(filename)
        for index, row in df.iterrows():
            pairs.append({
                'Stock_1': row['Stock_1'],
                'Stock_2': row['Stock_2'],
                'beta': row['beta']
            })
        return pairs
    

    def __init__(self, pairs, thresholds, allocations):
        '''
        {thresholds} should be a list of size n.
        The first is the exit threshold and the last is the stop loss threshold.
        
        {allocations} should be a list of size n - 2.
        '''
        
        super().__init__()
        thresholds = sorted([abs(t) for t in thresholds])
        self.threshold_exit = thresholds[0]
        self.threshold_stop = thresholds[-1]
        self.thresholds_enter = thresholds[1:-1]
        self.allocations = [abs(t) for t in allocations]
        assert len(self.thresholds_enter) == len(self.allocations)
        assert sum(self.allocations) <= 1
        
        if type(pairs) is str:
            pairs = PairTradeStrategy.load_pairs(pairs)
        self.pairs = []
        for pair in pairs:
            self.pairs.append(HoldingPair(pair['Stock_1'], pair['Stock_2'], pair['beta']))
        self.tx_history = []
        

    def allocate_money(self, cash):
        '''
        Notify the strategy the total amount of (leveraged) cash allocated for it.
        '''
        
        money_each_pair = cash / len(self.pairs)
        for pair in self.pairs:
            pair.money_allocated = money_each_pair


    def transaction_history(self):
        '''
        Get the transaction history in pandas dataframe format.
        '''
        
        history = pd.DataFrame(columns=["Date", "Stock", "Direction", "Quantity", "Price"])
        for date, orders in self.tx_history:
            for stock, quantity in orders.items():
                if quantity != 0:
                    order = {"Date": date, "Stock": stock}
                    order["Quantity"] = abs(quantity)
                    order["Direction"] = "Buy" if quantity > 0 else "Sell"
                    order["Price"] = self._stock_data[stock].loc[date]['CLOSE']
                    history = history.append(order, ignore_index=True)
        history.sort_values("Date", inplace=True)
        return history
    

    def analyze_spread(self, start=None, end=None):
        '''
        For each pair,
        recalculate the mean and stdev of its spread using data from start to end, inclusive.
        '''

        if end is None:
            end = self.today

        for pair in self.pairs:
            df_stock_x = self._stock_data[pair.X].loc[:end]
            df_stock_y = self._stock_data[pair.Y].loc[:end]
            if start is not None:
                df_stock_x = df_stock_x.loc[start:]
                df_stock_y = df_stock_y.loc[start:]
            df = pd.DataFrame(columns=['spread'])
            df['spread'] = df_stock_x['CLOSE'] - pair.beta * df_stock_y['CLOSE']
            pair.spread_mean = df['spread'].mean()
            pair.spread_std = df['spread'].std()
            

    def detect_signal(self, pair):
        '''
        For a given pair of stocks, detect the signal, if any.
        If self.threshold is of size n, then
        The result is encoded as an integer ranging from -5 to 5:
        
            n      upward cross stop loss threshold
            n-1    upward cross last enter threshold
            ...
            2      upward cross first enter threshold
            1      downward cross exit threshold
            0      no signal
            -1     upward cross (negative) exit threshold
            -2     downward cross (negative) first enter threshold
            ...
            -(n-1) downward cross (negative) last enter threshold
            -n:    downward cross (negative) stop loss threshold
        '''
        
        df_stock_x = self._stock_data[pair.X].loc[:self.today]
        df_stock_y = self._stock_data[pair.Y].loc[:self.today]

        cur_spread = df_stock_x.iloc[-1]['CLOSE'] - pair.beta * df_stock_y.iloc[-1]['CLOSE']
        cur_spread_z = (cur_spread - pair.spread_mean) / pair.spread_std
        prev_spread = df_stock_x.iloc[-2]['CLOSE'] - pair.beta * df_stock_y.iloc[-2]['CLOSE']
        prev_spread_z = (prev_spread - pair.spread_mean) / pair.spread_std

        thresholds_all = [-self.threshold_exit] + self.thresholds_enter + [self.threshold_stop]
        signal = 0
        if cur_spread_z > prev_spread_z:
            # Upward crossing
            for i, t in enumerate(thresholds_all):
                if cur_spread_z >= t > prev_spread_z:
                    signal = i + 1 if i > 0 else -1
        else:
            # Downward crossing
            for i, t in enumerate(thresholds_all):
                if cur_spread_z <= -t < prev_spread_z:
                    signal = -(i + 1) if i > 0 else 1
        return signal


    def decide(self):
        '''
        For each pair in the watch list, make orders if a signal is detected.
        '''

        orders = {}
        for pair in self.pairs:
            orders[pair.X] = orders.get(pair.X, 0)
            orders[pair.Y] = orders.get(pair.Y, 0)
            x_price = self._stock_data[pair.X].loc[self.today]['CLOSE']
            y_price = self._stock_data[pair.Y].loc[self.today]['CLOSE']
            pair_price = x_price + abs(pair.beta) * y_price
            X_quantity_change = 0
            Y_quantity_change = 0
            
            # Future TODO: Infer pair positions from stock positions
            # 
            # For now, pair position is stored and assuming the orders are always accepted

            signal = self.detect_signal(pair)
            stop_signal = len(self.thresholds_enter) + 2
            if abs(signal) in [1, stop_signal]:
                # Stop loss or exit to make profit. Clear positions
                X_quantity_change -= pair.X_quantity
                Y_quantity_change -= pair.Y_quantity
                pair.level = 0
            elif 1 < signal < stop_signal:
                # Crossing SHORT enter thresholds
                signal_level = signal - 1
                if pair.level > 0:
                    # Current position is LONG. Clear first
                    X_quantity_change -= pair.X_quantity
                    Y_quantity_change -= pair.Y_quantity
                    pair.level = 0
                cum_allocation = sum(self.allocations[:abs(signal_level + pair.level)])
                money_alloc = pair.money_allocated * cum_allocation
                X_quantity_change += -abs(int(money_alloc / pair_price))
                Y_quantity_change += -int(X_quantity_change * pair.beta)
                pair.level = -signal_level
            elif -1 > signal > -stop_signal:
                # Crossing LONG enter thresholds
                signal_level = signal + 1
                if pair.level < 0:
                    # Current position is SHORT. Clear first
                    X_quantity_change -= pair.X_quantity
                    Y_quantity_change -= pair.Y_quantity
                    pair.level = 0
                cum_allocation = sum(self.allocations[:abs(signal_level + pair.level)])
                money_alloc = pair.money_allocated * cum_allocation
                X_quantity_change += abs(int(money_alloc / pair_price))
                Y_quantity_change += -int(X_quantity_change * pair.beta)
                pair.level = -signal_level

            orders[pair.X] += X_quantity_change
            orders[pair.Y] += Y_quantity_change
            pair.X_quantity += X_quantity_change
            pair.Y_quantity += Y_quantity_change

        self.tx_history.append([self.today, orders])
        return orders





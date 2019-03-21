
'''
Implementations of (daily) Pair's Trading strategies.

'''


import pandas as pd


POSITION_LONG = 1
POSITION_SHORT = -1
POSITION_EMPTY = 0



class Strategy:
    '''
    The abstract base class strategy.
    
    '''

    def __init__(self):
        self._positions = {}
        self._stock_data = {}


    def feed(self, stock_data):
        '''
        Feed the strategy with latest stock data.
        '''

        for stock, stock_df in stock_data.items():
            if stock in self._stock_data:
                df = pd.concat([self._stock_data[stock], stock_df])
                #df = df[~df.index.duplicated(keep='last')]
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



class PairTradeStrategy(Strategy):

    @staticmethod
    def select_pairs(file, num_pairs, metric_col, ascending=False, beta_col=None):
        '''
        Choose pairs of stocks for trading.
        '''

        if type(file) is str:
            df = pd.read_csv(file)
        else:
            df = file
        pairs = []
        stock_codes = {}
        df.sort_values(metric_col, ascending=ascending, inplace=True)
        for index, row in df.iterrows():
            if len(pairs) >= num_pairs:
                break
            stock_x = row['Stock_1']
            stock_y = row['Stock_2']
            if stock_x in stock_codes or stock_y in stock_codes:
                continue
            stock_codes[stock_x] = True
            stock_codes[stock_y] = True
            beta = row[beta_col] if beta_col else 1
            pairs.append({
                'Stock_1': stock_x,
                'Stock_2': stock_y,
                'beta': beta
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
        Load pairs from a file.
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
    

    def __init__(self, thresholds, pairs, initial_cash=0):
        '''
        {thresholds} is a dict and must include keys 'enter', 'exit' and 'stop'
        All values should be positive and [exit] < [enter] < [stop].

        {pairs} should be list of pairs of stocks, or the path to a csv file.
        Each pair is a dict and must include the following attributes
            Stock_1 : str The stock code of one stock
            Stock_2 : str The stock code of the other stock
            beta : float The multiplier of Stock_2, used for the calculation of spread
        '''
        
        super().__init__()
        self.threshold_enter = abs(thresholds['enter'])
        self.threshold_exit = abs(thresholds['exit'])
        self.threshold_stop = abs(thresholds['stop'])
        if type(pairs) is str:
            self.watch_list = PairTradeStrategy.load_pairs(pairs)
        else:
            self.watch_list = pairs
        self.initial_cash = initial_cash


    def analyze_spread(self):
        '''
        For each pair of stock in the watchlist,
        recalculate the mean and stdev of its spread using all data available.
        '''

        for pair in self.watch_list:
            df_stock_x = self._stock_data[pair['Stock_1']]
            df_stock_y = self._stock_data[pair['Stock_2']]
            df = pd.DataFrame(columns=['spread'])
            df['spread'] = df_stock_x['CLOSE'] - pair['beta'] * df_stock_y['CLOSE']
            pair['spread_mean'] = df['spread'].mean()
            pair['spread_std'] = df['spread'].std()


    def detect_signal(self, pair):
        '''
        For a given pair of stocks, detect the signal, if any.
        The result is encoded as an integer ranging from -3 to 3.

        There are 6 different signals:
            3: upward cross stop loss threshold
            2: upward cross enter threshold
            1: downward cross exit threshold
            -1: upward cross (negative) exit threshold
            -2: downward cross (negative) enter threshold
            -3: downward cross (negative) stop loss threshold

        0 means no signal.
        '''
        
        df_stock_x = self._stock_data[pair['Stock_1']]
        df_stock_y = self._stock_data[pair['Stock_2']]
        beta = pair['beta']
        spread_mean = pair['spread_mean']
        spread_std = pair['spread_std']

        cur_spread = df_stock_x[-1:]['CLOSE'][0] - beta * df_stock_y[-1:]['CLOSE'][0]
        cur_spread_z = (cur_spread - spread_mean) / spread_std
        prev_spread = df_stock_x[-2:-1]['CLOSE'][0] - beta * df_stock_y[-2:-1]['CLOSE'][0]
        prev_spread_z = (prev_spread - spread_mean) / spread_std

        signal = 0
        if cur_spread_z >= self.threshold_stop and prev_spread_z < self.threshold_stop:
            signal = 3
        elif cur_spread_z >= self.threshold_enter and prev_spread_z < self.threshold_enter:
            signal = 2
        elif cur_spread_z <= self.threshold_exit and prev_spread_z > self.threshold_exit:
            signal = 1
        elif cur_spread_z >= -self.threshold_exit and prev_spread_z < -self.threshold_exit:
            signal = -1
        elif cur_spread_z <= -self.threshold_enter and prev_spread_z > -self.threshold_enter:
            signal = -2
        elif cur_spread_z <= -self.threshold_exit and prev_spread_z > -self.threshold_exit:
            signal = -3

        return signal


    def decide(self):
        '''
        For each pair in the watch list, make orders if a signal is detected.
        '''

        positions_by_stock = self.positions()
        cash_for_each_pair = int(self.initial_cash / len(self.watch_list))
         
        orders = {}
        for pair in self.watch_list:
            stock_x = pair['Stock_1']
            stock_y = pair['Stock_2']
            beta = pair['beta']

            x_price = self._stock_data[stock_x][-1:]['CLOSE'][0]
            y_price = self._stock_data[stock_y][-1:]['CLOSE'][0]
            
            pair_price = x_price - beta * y_price
            # The maximum number of pair to enter without excessing the cash limit
            # Assuming 100% margin rate
            if pair_price == 0:
                continue
            try:
                num_pairs = abs(int(cash_for_each_pair / pair_price))
            except:
                continue
            
            # Positions of the stocks in quantities
            position_x = positions_by_stock.get(stock_x, 0)
            position_y = positions_by_stock.get(stock_y, 0)
            # Position of the pair. The same as the position of X
            if position_x > 0:
                position = POSITION_LONG
            elif position_x < 0:
                position = POSITION_SHORT
            else:
                position = POSITION_EMPTY

            signal = self.detect_signal(pair)
            
            if (signal == 3 or signal == 1) and position == POSITION_SHORT:
                # Stop loss or exiting the SHORT position and make profit. Clear positions
                orders[stock_x] = -position_x
                orders[stock_y] = -position_y
            elif signal == 2 and position != POSITION_SHORT:
                # Enter by shorting the pair if not already SHORT (prevents doubling up)
                # Short x + long y (if beta > 0) or short y (if beta < 0)
                orders[stock_x] = -num_pairs
                orders[stock_y] = int(num_pairs * beta)
            elif signal == -2 and position != POSITION_LONG:
                # Enter by longing the pair if not already LONG (prevents doubling up)
                # Long x + short y (if beta > 0) or long y (if beta < 0)
                orders[stock_x] = num_pairs
                orders[stock_y] = -int(num_pairs * beta)
            elif (signal == -3 or signal == -1) and position == POSITION_LONG:
                # Stop loss or exiting the LONG position and make profit. Clear positions
                orders[stock_x] = -position_x
                orders[stock_y] = -position_y

        return orders





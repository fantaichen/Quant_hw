import numpy as np
import pandas as pd
import datetime
from matplotlib import  pyplot as plt


def code_check(code):
    code = str(code)
    if len(code) == 9:
        return code
    elif len(code) == 6:
        if code[0] == '6':
            code = code + '.SH'
        else:
            code = code + '.SZ'
        return code
    else:
        print('invalid stock code for ' + code)
        return None


class StockData:
    def __init__(self, path):
        self.path = path
        self.symbols = {}

    def read(self, symbols):
        data = {}
        for i in symbols:
            code = code_check(i)
            if code is not None:
                df = pd.read_csv(self.path + '\\' + code + '.csv')
                index = str(code)[0:6]
                df = df.rename(columns={'S_INFO_WINDCODE': 'symbols', 'S_DQ_AMOUNT': 'turnover',
                                        'TRADE_DT': 'date', 'S_DQ_AVGPRICE': 'vwap'})
                column_list = df.columns.tolist()
                for i, name in enumerate(column_list):
                    column_list[i] = str(name).replace('S_DQ_', '').lower()
                df.columns = column_list
                df['symbols'] = df['symbols'].apply(lambda x: x[0:6])
                # df['date'] = df['date'].apply(lambda x: datetime.datetime.strptime(str(x), '%Y%m%d'))
                data.update({index: df.copy()})
        self.symbols = data

    def get_data_by_symbol(self, symbol, start_date, end_date):
        # start_date = datetime.datetime.strptime(str(start_date), '%Y%m%d')
        # end_date = datetime.datetime.strptime(str(end_date), '%Y%m%d')
        start_date, end_date = int(start_date), int(end_date)
        symbol = code_check(symbol)
        if symbol is not None:
            symbol = symbol[0:6]
            data = self.symbols.copy()
            this_stk = data[symbol][['date', 'open', 'high', 'low', 'close']]
            # this_stk['date'] = this_stk['date'].apply(lambda x: int(x))
            this_stk = this_stk[this_stk['date'] <= end_date]
            this_stk = this_stk[this_stk['date'] >= start_date]
            symbol_df = this_stk
            symbol_df.set_index(['date'], inplace=True)
            return symbol_df
        else:
            return None

    def get_data_by_date(self, adate, symbols):
        date_df = pd.DataFrame(columns=['symbols', 'open', 'high', 'low', 'close'])
        adate = int(adate)
        for i in symbols:
            code = code_check(i)
            if code is not None:
                code = code[0:6]
                df_tmp = self.symbols[code].copy()
                df_tmp = df_tmp[df_tmp['date'] == adate]
                this_stk = df_tmp[['symbols', 'open', 'high', 'low', 'close']]
                date_df = pd.concat([date_df, this_stk], axis=0)
        date_df.set_index(['symbols'], inplace=True)
        return date_df

    def get_data_by_field(self, field, symbols):
        field_df = pd.DataFrame(columns=['date'])
        field_df.set_index(['date'])
        df_list = []
        for i in symbols:
            code = code_check(i)
            if code is not None:
                code = code[0:6]
                df_tmp = self.symbols[code].copy()
                this_df = df_tmp[['date', field]]
                ser = pd.Series(this_df[field], name=code)
                ser.index = this_df['date']
                df_list.append(ser)
        field_df = pd.concat(df_list, axis=1)
        return field_df

    def format_date(self, symbol):
        # symbol['date'] = symbol['date'].apply(lambda x: datetime.datetime.strptime(str(x), '%Y%m%d'))
        # return symbol
        symbol_df = self.symbols[symbol].copy()
        symbol_df['date'] = symbol_df['date'].apply(lambda x: datetime.datetime.strptime(str(x), '%Y%m%d'))
        self.symbols[symbol] = symbol_df
        return symbol_df

    def plot(self, symbol, field):
        field_df = self.symbols[symbol][['date', field]].copy()
        field_df = field_df.dropna()
        field_df = field_df.sort_values(by=['date'])
        x = np.asarray(field_df['date'].tolist())
        y = np.asarray(field_df[field].tolist())
        if field in ['volume', 'turnover']:
            plt.bar(x, y)
        else:
            fig = plt.figure()
            axes = fig.add_axes([0.1, 0.1, 0.9, 0.9])
            axes.plot(x, y, 'r')
        plt.show()

    def adjust_data(self, symbol):
        symbol_df = self.symbols[symbol].copy()
        factor = [1]
        for i in range(1, len(symbol_df['close'])):
            factor.append(symbol_df['close'][i-1] / symbol_df['close'][i] * factor[i-1])
        # print(factor)
        factor_ser = pd.Series(factor, name='adjfactor')
        symbol_df['adjfactor'] = factor_ser
        for i in ['open', 'high', 'low', 'close']:
            name = 'adj' + i
            a = symbol_df[i] * symbol_df['adjfactor'] / factor[-1]
            symbol_df[name] = a
        self.symbols[symbol] = symbol_df
        return symbol_df

    def resample(self, symbol, freq):
        features = ['date', 'open', 'high', 'low', 'close', 'volume', 'turnover', 'vwap']
        df = self.format_date(symbol)[features].copy()
        df_new = df.set_index(['date'])
        features_new = pd.DataFrame()
        freq = str(freq) + 'D'
        features_new['open'] = df_new['open'].resample(freq).first()
        features_new['high'] = df_new['high'].resample(freq).max()
        features_new['low'] = df_new['low'].resample(freq).min()
        features_new['close'] = df_new['close'].resample(freq).last()
        features_new['volume'] = df_new['volume'].resample(freq).sum()
        features_new['turnover'] = df_new['turnover'].resample(freq).sum()
        features_new['vwap'] = df_new['vwap'].mean()
        return features_new

    def moving_average(self, symbol, field, window):
        this_df = self.symbols[symbol].copy()
        this_df = this_df.sort_values(by=['date'])
        this_se = this_df[field]
        se = this_se.rolling(int(window)).mean()
        se.index = this_df['date'].tolist()
        return se

    def ema(self, symbol, field, window):
        this_df = self.symbols[symbol].copy()
        this_df = this_df.sort_values(by=['date'])
        price = this_df[field]
        # price.index = this_df['date'].tolist()
        ema = []
        for i in range(len(price)):
            if i == 0:
                ema.append(price[i])
            if i > 0:
                ema.append((int(window)-1)*price[i-1] + 2*price[i]/(int(window)+1))
        name = str(field) + '_ema'
        ema_se = pd.Series(ema, name=name)
        ema_se.index = this_df['date'].tolist()
        return ema_se

    def atr(self, symbol, window):
        this_df = self.symbols[symbol].copy()
        this_df = this_df.sort_values(by=['date'])
        last = this_df['close'].tolist()[-1]
        dif = (this_df['high'] - this_df['low'])
        abs_high = (this_df['high'] - last).apply(lambda x: abs(x))
        abs_low = (this_df['low'] - last).apply(lambda x: abs(x))
        tr = []
        for i in range(len(dif)):
            tr.append(max(dif[i], abs_high[i], abs_low[i]))
        tr = pd.Series(tr)
        atr = tr.rolling(window).mean()
        atr.index = this_df['date'].tolist()
        return atr

    def rsi(self, symbol, window):
        this_df = self.symbols[symbol].copy()
        this_df = this_df.sort_values(by=['date'])
        data = pd.DataFrame()
        data['value'] = this_df['close'] - this_df['close'].shift()
        data.fillna(0, inplace=True)
        data['value1'] = data['value'] .copy()
        data.loc[data['value1'] < 0, 'value1'] = 0
        data['value2'] = data['value'] .copy()
        data.loc[data['value2'] > 0, 'value2'] = 0
        data['plus'] = data['value1'].rolling(window).sum()
        data['minus'] = data['value2'].rolling(window).sum()
        data.fillna(0, inplace=True)
        rsi = data['plus']/(data['plus'] - data['minus'])*100
        data.fillna(0, inplace=True)
        rsi.index = this_df['date'].tolist()
        return rsi

    def macd(self, symbol, short, long, mid):
        short = int(short)
        long = int(long)
        mid = int(mid)
        this_df = self.symbols[symbol].copy()
        this_df = this_df.sort_values(by=['date'])
        data = pd.DataFrame()
        data['date'] = this_df['date']
        data['sema'] = this_df['close'].ewm(adjust=False, alpha=2/(short + 1), ignore_na=True).mean()
        data['lema'] = this_df['close'].ewm(adjust=False, alpha=2/(long + 1), ignore_na=True).mean()
        data.fillna(0, inplace=True)
        data['dif'] = data['sema'] - data['lema']
        data['dea'] = data['dif'].ewm(adjust=False, alpha=2/(mid + 1), ignore_na=True).mean()
        data['macd'] = 2*(data['dif'] - data['dea'])
        data.fillna(0, inplace=True)
        macd = data['macd'].copy()
        macd.index = this_df['date'].tolist()
        return macd








# codes for test
# path = 'C:\\intern\\training\\data'
# symbols = ['000004', '000005', '000027']
# data = StockData(path)
# data.read(symbols)

# print(data.get_data_by_symbol(symbols[0], '19991115', '20000531'))
# print(data.get_data_by_date('19991124', symbols))
# print(data.get_data_by_field('volume', symbols))
# df = data.get_data_by_field('volume', symbols)
# print(df.index.tolist())
# print(data.plot('000004', 'vwap'))
# data.plot('000004', 'open')
# print(data.format_date('000005'))
# print(data.adjust_data('000027')['adjfactor'])
# data.format_date()
# print(data.resample('000027', 5))
# print(data.moving_average('000027', 'close', 10))
# print(data.ema('000027', 'close', 12))
# print(data.rsi('000027', 10))
# print(data.macd('000027', 12, 26, 9))
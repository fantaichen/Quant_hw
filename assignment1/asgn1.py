# _*_ coding: utf-8 _*_
"""
Time:     8/2/2021 15:57
Author:   FAN Taichen
Version:  V 0.1
File:     asgn1.py
Describe: CUHK course homework, Github link: https://github.com/fantaichen/Quant_hw
"""
import pandas as pd


def min5(a):
    minute = int(a[-2:])
    minutenew = int(minute / 5) * 5
    if len(str(minutenew)) == 1:
        minutenew = '0' + str(minutenew)
    b = a[:-2] + str(minutenew)
    return b


quote = pd.read_csv('trade_quote_data/quote.csv')
trade = pd.read_csv('trade_quote_data/trade.csv')

trade['notional'] = trade['price'] * trade['size']
quote['minute'] = quote['time'].apply(lambda x: x[:5])
trade['minute'] = trade['time'].apply(lambda x: x[:5])

newtrade = trade.groupby(['date', 'sym']).agg({'price': 'last', 'size': 'sum', 'notional': 'sum'})
tradedict = {}
for i, tradedf in newtrade.groupby('sym'):
    tradedf['return'] = tradedf['price'].pct_change()
    tradedict[str(i)] = tradedf.copy()
sz = tradedict['000001.SZSE'].groupby('sym').agg({'size': 'mean', 'notional': 'mean', 'return': 'std'})
sh = tradedict['600030.SHSE'].groupby('sym').agg({'size': 'mean', 'notional': 'mean', 'return': 'std'})
leftdf = pd.concat([sz, sh])

mintrade = trade.groupby(['date', 'sym', 'minute']).agg({'price': 'last'})
mintrade_sym = mintrade.groupby(['date', 'sym'])
mindict = []
for syms, symtradedf in mintrade_sym:
    symtradedf['minret'] = symtradedf['price'].pct_change()
    mindict.append(symtradedf.copy())
middf = pd.concat(mindict).groupby('sym').agg({'minret': 'std'})
t = 240 ** 0.5
middf['Volatility5'] = middf['minret'] * t

quote["spread"] = 10000 * (quote["ask"] - quote["bid"]) / 0.5 / (quote["ask"] + quote["bid"])
quote["qsize"] = 0.5 * (quote["asize"] + quote["bsize"])
rightdf = quote.groupby('sym').agg({'spread': 'mean', 'qsize': 'mean'})

q1 = pd.concat([leftdf, middf['Volatility5'], rightdf], axis=1)
q1 = q1.reset_index()
q1.columns = ['Stock', 'ADV', 'ADTV', 'Volatility', 'Volatility5', 'Spread(bps)', 'Quote Size']

quotenew = quote[quote['sym'] == '600030.SHSE']
tradenew = trade[trade['sym'] == '600030.SHSE']
tradenew['mins5'] = tradenew['minute'].apply(min5)
quotenew['mins5'] = quotenew['minute'].apply(min5)

tgdf = tradenew.groupby(['mins5']).agg({'size': 'sum'})
total_size = tradenew['size'].sum()
volpct = tgdf / total_size

newret = pd.concat(mindict)
newret = newret.reset_index()
newret = newret[newret['sym'] == '600030.SHSE']
newret['mins5'] = newret['minute'].apply(min5)
minretstd = newret.groupby('mins5').agg({'minret': 'std'})

qsmean = quotenew.groupby('mins5').agg({'spread': 'mean', 'qsize': 'mean'})

q2 = pd.concat([minretstd, qsmean, volpct], axis=1)
q2 = q2.reset_index()
q2.columns = ['time', 'vol5', 'spread', 'qsize', 'volpct']
q1.to_excel('asgn1_1.xlsx', index=False)
q2.to_excel('asgn1_2.xlsx', index=False)

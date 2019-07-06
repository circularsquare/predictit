import sqlite3 as sql
import os
import pickle
import matplotlib.pyplot as plt
import numpy as np
import copy

'''
todo:
1. fuse markets that are the same market
2. estimate markets by going per contract and saying
    whats the price of this contract? averaging low and high.
    then assuming contract is all in middle of range and weighting it by price to get the estimated tweets PER HOUR. YAY!
'''

'''
the sql tables are Markets, Contracts, Prices, Volumes
markets: market_id, market_name, market_url, market_status, market_predictit_id
contracts: contract_id, market_id, contract_name, contract_status, contract_predictit_id
prices: price_id, contract_id, last_price, buy_yes, buy_no, sell_yes, sell_no, time_stamp
volumes: volume_id, contract_id, open_share_price, high_share_price, low_share_price, close_share_price, volume, time_stamp
my big table format is:
markets[]  (accounts in order are rdt, wh, vp, potus)
    account{}: 4 of them, indexed by marketid   (aka marketList)
        marketPair[]: name + contractList
            contractList{}: indexed by contractid
                contractPair: name + pricelist
                    priceList{}: indexed by timestamp (yymmddhhmm)
'''

def getRawMarkets(): #populates rawMarkets with my formatting... #run to load data into data.txt from data folder
    rawMarkets = [[], [], [], [], []]
    for filename in os.listdir('data'):
        db = sql.connect('data/' + filename)
        marketsSQL = db.execute('select * from markets where market_name like "How many tweets%"')
        for market in marketsSQL:
            contracts = [contract for contract in db.execute('select * from contracts where market_id = ' + str(market[0]) )]
            contractLabels = str(tuple([contract[0] for contract in contracts]))
            prices = [price for price in db.execute('select * from prices where contract_id in ' + contractLabels + ' and time_stamp like ' + '"% __:_0:%"')]
            contracts = [(contract, prices) for contract in contracts]
            marketPair = (market, contracts) #wait this whole shit is fucked up.... 
            if 'realDonald' in str(market[1]):
                (rawMarkets[0]).append(marketPair)
            elif 'whitehouse' in str(market[1]):
                (rawMarkets[1]).append(marketPair)
            elif '@vp' in str(market[1]):
                (rawMarkets[2]).append(marketPair)
            elif '@potus' in str(market[1]):
                (rawMarkets[3]).append(marketPair)
            else:
                (rawMarkets[4]).append(marketPair) #shouldnt happen
        print('file ' + filename + ' done')
        break
    return rawMarkets
def joinMarkets(rawMarkets):
    markets = [{}, {}, {}, {}, {}]
    for i in range(len(rawMarkets)): #loop through the 4 twitter accounts
        for marketPair in rawMarkets[i]:
            account = markets[i]
            marketid = copy.copy(marketPair[0][4])
            if marketid not in account.keys(): #if the market id is new, set the markets dict value to a tuple with the market name and the contract list
                account[marketid] = [marketPair[0][1], marketPair[1]] #marketpair[1] is contracts
            else: #if the market id already is there, add the new contractlist to the existing one
                account[marketid][1] += marketPair[1]
    return markets
def cleanContracts(markets):
    for account in markets:
        for marketid in account:
            rawContractPairs = copy.copy(account[marketid][1])
            account[marketid][1] = {}
            for rawContractPair in rawContractPairs:
                contractid = rawContractPair[0][4]
                print(rawContractPair[1][0])
                print(rawContractPair[0])
                account[marketid][1][contractid] = [rawContractPair[0][2], rawContractPair[1]]
    return markets
def cleanPrices(markets):
    for account in markets:
        for marketid in account:
            for contractid in (account[marketid][1]):
                priceList = copy.copy(account[marketid][1][contractid][1])
                print(priceList)
                newPriceList = {}
                for rawPrice in priceList:
                    condensedTimeStamp = int(rawPrice[7][2:4]+rawPrice[7][5:7]+rawPrice[7][8:10]+rawPrice[7][11:13]+rawPrice[7][14:16])
                    buyYes = rawPrice[3]
                    sellYes = rawPrice[5]
                    if buyYes == None:
                        buyYes = 0
                    if sellYes == None:
                        sellYes = 0
                    newPriceList[condensedTimeStamp] = ((buyYes+sellYes) / 2.0)
                    #print(str(buyYes) + ':' + str(sellYes) + ':' + str(newPriceList[condensedTimeStamp]))
                        #this makes the pricelist indexed by timestamp (up to the minute character), and reduces price to an average
                account[marketid][1][contractid][1] = newPriceList

    return markets
def cleanData():
    out = cleanPrices(cleanContracts(joinMarkets(getRawMarkets())))
    with open('data.txt', 'wb') as saveFile:
        pickle.dump(out, saveFile)


#run this to load data from the sql files into data.txt
cleanData()
with open('data.txt', 'rb') as saveFile:
    markets = pickle.load(saveFile)
#example get: from the 0th account, the 5372th market, the contractList, the 14809th contract, the pricelist, the 0th price

def plotMarket(marketid, markets):
    for account in markets:
        if marketid in account:
            contractList = account[marketid][1]
            priceLists = []
            for contract in contractList.values():
                priceLists.append(contract[1].values())
            for priceList in priceLists:
                print(list(priceList)[10])
                plt.plot(priceList)
            plt.show()

plotMarket(5388, markets)
print('?')

#list of tweet markets
[5372, 5388, 5404, 5420, 5436, 5453, 5468, 5494, 5513, 5537, 5552, 5571, 5591, 5614, 5374, 5391, 5407, 5422, 5438, 5456, 5470, 5493, 5519, 5541, 5555, 5576, 5594, 5617, 5377, 5394, 5410, 5428, 5442, 5457, 5478, 5505, 5523, 5546, 5559, 5582, 5606, 5621, 5378, 5393, 5411, 5427, 5443, 5458, 5479, 5504, 5524, 5547, 5560, 5583, 5607, 5622]









#hi

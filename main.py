import sqlite3 as sql
import os
import pickle
import matplotlib.pyplot as plt
import numpy as np
import copy
import re

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
            prices = [price for price in db.execute('select * from prices where contract_id in ' + contractLabels + ' and time_stamp like ' + '"% __:00:%"')]
            newContracts = []
            for contract in contracts:
                newPrices = [price for price in prices if price[1] == contract[0]]
                newContracts.append((contract, newPrices))
            marketPair = (market, newContracts) #wait this whole shit is fucked up....
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
    return rawMarkets
def joinMarkets(rawMarkets):
    markets = [{}, {}, {}, {}, {}]
    for i in range(len(rawMarkets)): #loop through the 4 twitter accounts
        account = markets[i]
        for marketPair in rawMarkets[i]:
            marketid = copy.copy(marketPair[0][4])
            if marketid not in account.keys(): #if the market id is new, set the markets dict value to a tuple with the market name and the contract list
                account[marketid] = [marketPair[0][1], marketPair[1]] #marketpair[1] is contracts
            else: #if the market id already is there, add the new contractlist to the existing one
                account[marketid][1] += marketPair[1]

        for marketid in account:
            rawMarketName = account[marketid][0]
            if re.search('@..', rawMarketName):
                accountName = re.search('@..', rawMarketName)[0]
                accountNameDict = {'@re':'@RDT', '@vp':'@VP', '@wh':'@WH', '@po':'@POTUS'}
                fullAccountName = accountNameDict[accountName]
            else:
                fullAccountName = 'unknownacct'
            if re.search('../..', rawMarketName):
                fullDateName = re.search('../..', rawMarketName)[0].strip()
            elif re.search('.... [0-9].', rawMarketName):
                date1 = re.search('.... [0-9].', rawMarketName)[0]
                monthDict = {'Jan.': 1, 'Feb.': 2, 'Mar.': 3, 'Apr.': 4, ' May':5, 'June':6, 'July':7, 'Aug.':8, 'Sep.':9, 'Oct.':10, 'Nov.':11, 'Dec.':12}
                month1 = monthDict[date1[0:4]]
                day1 = date1[5:7].strip()
                fullDateName = str(month1) + '/' + str(day1)
            else:
                fullDateName = 'unknowndate'
            account[marketid][0] = fullAccountName + ' from ' + fullDateName
    return markets
def cleanContracts(markets):
    for account in markets:
        for marketid in account:
            rawContractPairs = copy.copy(account[marketid][1])
            account[marketid][1] = {}
            for rawContractPair in rawContractPairs:
                contractid = rawContractPair[0][4]
                contractName = rawContractPair[0][2]
                account[marketid][1][contractid] = [contractName, rawContractPair[1]]
    return markets
def cleanPrices(markets):
    for account in markets:
        for marketid in account:
            for contractid in (account[marketid][1]):
                priceList = copy.copy(account[marketid][1][contractid][1])
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
                        #this makes the pricelist indexed by timestamp (up to the minute character), and reduces price to an average
                account[marketid][1][contractid][1] = newPriceList
    return markets
def cleanData():
    out = cleanPrices(cleanContracts(joinMarkets(getRawMarkets())))
    with open('data.txt', 'wb') as saveFile:
        pickle.dump(out, saveFile)


#run this to load data from the sql files into data.txt
#cleanData takes a longass time
#cleanData()
with open('data.txt', 'rb') as saveFile:
    markets = pickle.load(saveFile)
#example get: from the 0th account, the 5372th market, the contractList, the 14809th contract, the pricelist, the 0th price

def getMarket(marketid, markets = markets):
    for account in markets:
        if marketid in account:
            return account[marketid]
def plotMarket(market):
    contractList = market[1]
    plt.title(market[0])
    for contract in contractList.values():
        priceList = contract[1].values()
        contractName = contract[0]
        length = len(priceList)
        normalizer = np.linspace(0, 1, length)
        plt.plot(normalizer, priceList, label=contractName)
    plt.legend()
    #plt.show()
def avgTweetCount(market):
    contractList = market[1]
    tweetCountDict = {}
    avgTweets = {}
    for contract in contractList.values():
        priceList = contract[1]
        contractName = contract[0]
        if '-' in contractName:
            dash = contractName.index('-')
            num1 = int(contractName[0:dash-1])
            num2 = int(contractName[dash+1:])
            tweetCount = (num1+num2)//2 #rounds for the sake of indexing the dict
        elif 'more' in contractName:
            if contractName[2] == ' ':
                tweetCount = int(contractName[0:2]) + 5
            else:
                tweetCount = int(contractName[0:3]) + 5
        else:
            if contractName[2] == ' ':
                tweetCount = int(contractName[0:2]) - 5
            else:
                tweetCount = int(contractName[0:3]) - 5
        tweetCountDict[tweetCount] = priceList
    somePriceList = list(contractList.values())[0][1]
    for priceTime in somePriceList:
        avgTweets[priceTime] = 0
        sumPrices = sum(list([tweetCountDict[tweetCount][priceTime] for tweetCount in tweetCountDict])) #sum of prices at a given time, should be around 1.0, for weighting
        for tweetCount in tweetCountDict:
            priceList = tweetCountDict[tweetCount]
            price = priceList[priceTime]
            adjustedPrice = price/sumPrices
            avgTweets[priceTime] += adjustedPrice*tweetCount #adds the weighted tweet count to avgTweets for the given time!
    return avgTweets

def plotAvgTweetCount(market):
    plt.title('avg tweets for ' + market[0])
    avgTweets = avgTweetCount(market)
    length = len(avgTweets)
    normalizer = np.linspace(0, 1, length)
    plt.plot(normalizer, avgTweets.values(), label=market[0])
    plt.legend()
    #plt.show()

def plotMarketAndTweetCount(market):
    plt.subplot(2, 1, 1)
    plotMarket(market)
    plt.subplot(2, 1, 2)
    plotAvgTweetCount(market)

def plotAccount(i):
    for marketid in markets[i]:
        plotMarketAndTweetCount(getMarket(marketid))

plotAccount(0)

plt.show()
#list of tweet markets
[5372, 5388, 5404, 5420, 5436, 5453, 5468, 5494, 5513, 5537, 5552, 5571, 5591, 5614, 5374, 5391, 5407, 5422, 5438, 5456, 5470, 5493, 5519, 5541, 5555, 5576, 5594, 5617, 5377, 5394, 5410, 5428, 5442, 5457, 5478, 5505, 5523, 5546, 5559, 5582, 5606, 5621, 5378, 5393, 5411, 5427, 5443, 5458, 5479, 5504, 5524, 5547, 5560, 5583, 5607, 5622]





#hi

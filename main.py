import sqlite3 as sql
import os
import pickle
import matplotlib.pyplot as plt
import numpy as np
import copy
import re
import time
import random
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
                        [avg of buy and sell costs, buy, sell]
'''

def getRawMarkets(): #populates rawMarkets with my formatting... #run to load data into data.txt from data folder
    startTime = time.time()
    rawMarkets = [[], [], [], [], []]
    for filename in os.listdir('data'):
        db = sql.connect('data/' + filename)
        marketsSQL = db.execute('select * from markets where market_name like "How many tweets%"')
        for market in marketsSQL:
            contracts = [contract for contract in db.execute('select * from contracts where market_id = ' + str(market[0]) )]
            contractLabels = str(tuple([contract[0] for contract in contracts]))
            newContracts = []
            for contract in contracts:
                prices = [price for price in db.execute('select * from prices where contract_id = ' + str(contract[0]) + ' and time_stamp like ' + '"% __:__:%"')]
                newContracts.append((contract, prices))
            marketPair = (market, newContracts)
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
    with open('rawData.txt', 'wb') as saveFile:
        pickle.dump(rawMarkets, saveFile)
    print('getting markets took ' + str(time.time() - startTime))

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
                    if (abs(buyYes-sellYes)>.9):
                        sellYes = 1
                        buyYes = 1
                    newPriceList[condensedTimeStamp] = [((buyYes+sellYes) / 2.0), buyYes, sellYes]
                        #this makes the pricelist indexed by timestamp (up to the minute character), and reduces price to list containing [avg, buy, sell]
                account[marketid][1][contractid][1] = newPriceList
    return markets
def cleanData():
    startTime = time.time()
    with open('rawData.txt', 'rb') as saveFile:
        rawMarkets = pickle.load(saveFile)
    markets = cleanPrices(cleanContracts(joinMarkets(rawMarkets)))
    with open('data.txt', 'wb') as saveFile:
        pickle.dump(markets, saveFile)
    print('cleaning data took ' + str(time.time() - startTime))

#run this to load data from the sql files into data.txt
#getting raw markets takes a longass time
#getRawMarkets()
#cleanData()

startTime = time.time()
with open('data.txt', 'rb') as saveFile:
    markets = pickle.load(saveFile)
print('loaded data in ' + str(time.time()-startTime))

def getMarket(marketid, markets = markets):
    for account in markets:
        if marketid in account:
            return account[marketid]
def plotMarket(market):
    contractList = market[1]
    plt.title(market[0])
    for contract in contractList.values():
        plotContract(contract)
    plt.legend()
def plotContract(contract):
    priceList = contract[1].values()
    avgPriceList = [price[0] for price in priceList]
    highPriceList = [price[1] for price in priceList]
    lowPriceList = [price[2] for price in priceList]
    contractName = contract[0]
    plt.plot(avgPriceList, label=contractName, color = '#70badb', alpha=.5)
    plt.plot(lowPriceList, color = '#1e91ca', alpha=.5)
    plt.plot(highPriceList, color = '#1072ab', alpha=.5)

def contractNameToAvgTweetCount(name): #returns the average of the high and low of a contract
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
def avgTweetCount(market):
    contractList = market[1]
    tweetCountDict = {}
    avgTweets = {}
    for contract in contractList.values():
        priceList = contract[1]
        tweetCount = contractNameToAvgTweetCount(contract[0])
        tweetCountDict[tweetCount] = priceList
    somePriceList = list(contractList.values())[0][1]
    for priceTime in somePriceList:
        avgTweets[priceTime] = 0
        sumPrices = sum(list([tweetCountDict[tweetCount][priceTime] for tweetCount in tweetCountDict])) #sum of prices at a given time, should be around 1.0, for weighting
        for tweetCount in tweetCountDict:
            priceList = tweetCountDict[tweetCount]
            price = priceList[priceTime]
            if(sumPrices==0):
                adjustedPrice = 0
            else:
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

def plotMarketAndTweetCount(market):
    plt.subplot(2, 1, 1)
    plotMarket(market)
    plt.subplot(2, 1, 2)
    plotAvgTweetCount(market)

def plotAccount(i):
    for marketid in markets[i]:
        plotMarketAndTweetCount(getMarket(marketid))

def movingAverage(priceList, timePeriod = 60): #time period is in minutes
    avg = [0]*timePeriod
    for i in range(len(priceList)-timePeriod):
        avg.append(sum(priceList[i:i+timePeriod])/timePeriod )
    return avg
def plotMovingAverage(contract, timePeriod = 60):
    priceList = [price[0] for price in contract[1].values()]
    plt.plot(movingAverage(priceList, timePeriod))

def getMarketIds():
    marketids = []
    for i in range(4):
        for marketid in markets[i]:
            marketids.append(marketid)
    return marketids

def backtestMeanReversion(contract):
    yesHeld = [0] #the number of yes's you hold (or negative the number of nos)
    money = [0, 0] #simulates ur net money over time.. money[0] is precashout, money[1] after cashout
    avgPriceList = [priceTriplet[0] for priceTriplet in contract[1].values()]
    buyPriceList = [priceTriplet[1] for priceTriplet in contract[1].values()]
    sellPriceList = [priceTriplet[2] for priceTriplet in contract[1].values()]
    shortAverage = movingAverage(avgPriceList, 1)
    longAverage = movingAverage(avgPriceList, 100)
    q1 = len(avgPriceList)//4
    q2 = len(avgPriceList)//2
    q3 = q1+q2

    def buy(n, i): #n is number of stocks, i is time (by index), NOTE n must be positive
        money[0] -= n*buyPriceList[i]
        if yesHeld[0] < -n:
            money[0] += n*1.0
        elif yesHeld[0] < 0:
            money[0] += (0-yesHeld[0])*1.0
        yesHeld[0] += n
    def sell(n, i): #n must be positive
        money[0] += n*sellPriceList[i]
        if yesHeld[0] < 0:
            money[0] -= n*1.0
        elif yesHeld[0] < n:
            money[0] -= (n-yesHeld[0])*1.0
        yesHeld[0] -= n
    for i in range(len(longAverage)):
        spread = buyPriceList[i]-sellPriceList[i]
        if i < q3:
            if shortAverage[i] - longAverage[i] > spread*2: #upward trending
                buy(1, i)
            if shortAverage[i] - longAverage[i] < spread*2: #downward trending
                sell(1, i)
    money[1] = money[0]
    if avgPriceList[-1]>=.98 and yesHeld[0]>0: #cashout
        money[1] += yesHeld[0]*1
    if avgPriceList[-1]<=.02 and yesHeld[0]<0:
        money[1] -= yesHeld[0]*1
    if avgPriceList[-1]>.02 and avgPriceList[-1]<.98:
        #print('market didnt resolve yet...')
        return [0, 0]
    return money

def plotMovingAndCurrent(market):
    startTime = time.time()
    for contract in market[1].values():
        plotContract(contract)
        #plotMovingAverage(contract, 60)
        plotMovingAverage(contract, 100)
    print('plotting took ' + str(time.time() - startTime))
    plt.show()

cumulativeRatio = 0
cumulativeProfit = 0

for account in markets:
    cumulativeRatio = 0
    cumulativeProfit = 0
    for market in account.values():
        #plotMovingAndCurrent(market)
        investment = 0
        profit = 0
        #plotMovingAndCurrent(market)
        for contract in market[1].values():
            investment -= backtestMeanReversion(contract)[0]
            profit += backtestMeanReversion(contract)[1]
            if investment == 0:
                if profit == 0:
                    ratio = 0
                else:
                    ratio = 'wtf?'
            else:
                ratio = profit/investment
            cumulativeRatio += ratio
            cumulativeProfit += profit
            #if random.random()<.1:
            #    print('ratio:' + str(cumulativeRatio))
            #    print('proft:' + str(cumulativeProfit))
    print(cumulativeRatio)
    print(cumulativeProfit)











#xd

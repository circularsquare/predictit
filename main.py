import sqlite3 as sql
import os
import pickle



'''
todo:
1. fuse markets that are the same market
2. 
'''
tweetMarkets = [[], [], [], [], []] #rdt, wh, vp, potus
def getTweetMarkets(): #populates tweetMarkets with my formatting... #run to load data into data.txt from data folder
    for filename in os.listdir('data'):
        db = sql.connect('data/' + filename)
        markets = db.execute('select * from markets where market_name like "How many tweets%"')
        for market in markets:
            contracts = [contract for contract in db.execute('select * from contracts where market_id = ' + str(market[0]) )]
            contractLabels = str(tuple([contract[0] for contract in contracts]))
            prices = [price for price in db.execute('select * from prices where contract_id in ' + contractLabels + ' and time_stamp like ' + '"%:00:%"')]
            contracts = [(contract, prices) for contract in contracts]
            #makes each tweet makret a tuple, whose second entry is the market's contracts
            #then adds the market to the correct twitter account's thing
            marketPair = (market, contracts)

            if 'realDonald' in str(market[1]):
                (tweetMarkets[0]).append(marketPair)
            elif 'whitehouse' in str(market[1]):
                (tweetMarkets[1]).append(marketPair)
            elif '@vp' in str(market[1]):
                (tweetMarkets[2]).append(marketPair)
            elif '@potus' in str(market[1]):
                (tweetMarkets[3]).append(marketPair)
            else:
                (tweetMarkets[4]).append(marketPair) #shouldnt happen
        print('file ' + filename + ' done')
    with open('data.txt', 'wb') as saveFile:
        pickle.dump(tweetMarkets, saveFile)

#getTweetMarkets()
#run this to load data



with open("data.txt", 'rb') as saveFile:
    markets = pickle.load(saveFile)


def marketEstimate(market):





#... ok so goal is to get for each contract, a graph of prices over time.

'''
the sql tables are Markets, Contracts, Prices, Volumes
markets: market_id, market_name, market_url, market_status, market_predictit_id
contracts: contract_id, market_id, contract_name, contract_status, contract_predictit_id
prices: price_id, contract_id, last_price, buy_yes, buy_no, sell_yes, sell_no, time_stamp
volumes: volume_id, contract_id, open_share_price, high_share_price, low_share_price, close_share_price, volume, time_stamp

my big table is
tweetMarkets[]
    which is full of markets which each have contracts[]
        which are full of contracts which have prices[]
            which have low/high prices and timestamps

alright lets get like.. uh.. 10 minutely data?

'''

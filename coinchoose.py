""" Module for requesting data from coinchoose.com and parsing it. """
from datetime import date
from datetime import datetime
from datetime import time as tm
from decimal import Decimal
import json
import logging
import requests
import os
from random import random
import sys
import time
import unittest

baseUrl = "http://www.coinchoose.com"
countRequested = 0
interReqTime = 2
lastReqTime = None


def _request(payloadString):
    """Private method for requesting an arbitrary query string."""
    global countRequested
    global lastReqTime
    if lastReqTime is not None and time.time() - lastReqTime < interReqTime:
        timeToSleep = random()*(interReqTime-time.time()+lastReqTime)*2
        logging.info("Sleeping for {0} seconds before request.".format(
            timeToSleep))
        time.sleep(timeToSleep)
    logging.info("Issuing request for the following payload: {0}".format(
        payloadString))
    r = requests.get("{0}/{1}".format(baseUrl, payloadString))
    lastReqTime = time.time()
    countRequested += 1
    if r.status_code == requests.codes.ok:
        return r.text
    else:
        raise Exception("Could not process request. \
            Received status code {0}.".format(r.status_code))


def requestLatest():
    """Method for requesting a the lastest set of information."""
    return _request("api.php?base=BTC")


def parseLatestCurrencies(jsonDump):
    """Parse the latest currency list from an API call."""
    data = []
    rawData = json.loads(jsonDump)
    for rawDatum in rawData:
        datum = {}
        datum['symbol'] = rawDatum['symbol']
        datum['name'] = rawDatum['name']
        datum['algo'] = rawDatum['algo']
        data.append(datum)
    return data


def parseLatestNetworkStatus(jsonDump, scrapeTime=datetime.utcnow()):
    """Parse the latest network status from API call."""
    data = []
    rawData = json.loads(jsonDump)
    for rawDatum in rawData:
        datum = {}
        datum['symbol'] = rawDatum['symbol']
        datum['scrape_time'] = scrapeTime
        datum['current_blocks'] = long(rawDatum['currentBlocks']) if rawDatum['currentBlocks'] is not None else None
        datum['difficulty'] = Decimal(rawDatum['difficulty']) if rawDatum['difficulty'] is not None else None
        datum['reward'] = Decimal(rawDatum['reward']) if rawDatum['reward'] is not None else None
        datum['hash_rate'] = long(rawDatum['networkhashrate']) if rawDatum['networkhashrate'] is not None else None
        datum['avg_hash_rate'] = Decimal(rawDatum['avgHash']) if rawDatum['avgHash'] is not None else None
        data.append(datum)
    return data


class CoinchooseTest(unittest.TestCase):

    """"Testing suite for coinchoose module."""

    def testRequestLatest(self):
        """Test requestLatest."""
        jsonDump = requestLatest()
        f = open("{0}/data/test_api.json".format(
            os.path.dirname(os.path.abspath(__file__))), 'w')
        f.write(jsonDump)
        f.close()
        json.loads(jsonDump)

    def testParseLatestCurrencies(self):
        """Method for testing parseLatestCurrencies."""
        f = open("{0}/example/api.json".format(
            os.path.dirname(os.path.abspath(__file__))), 'r')
        jsonDump = f.read()
        f.close()
        data = parseLatestCurrencies(jsonDump)
        self.assertEqual(len(data), 59)
        expectedFirst = {
            'symbol': 'ALF',
            'name': 'Alphacoin',
            'algo': 'scrypt'
        }
        self.assertEqual(data[0], expectedFirst)
        expectedLast = {
            'symbol': 'GLC',
            'name': 'GlobalCoin',
            'algo': 'scrypt'
        }
        self.assertEqual(data[-1], expectedLast)

    def testParseLatestNetworkStatus(self):
        """Method for testing parseLatestNetworkStatus."""
        f = open("{0}/example/api.json".format(
            os.path.dirname(os.path.abspath(__file__))), 'r')
        jsonDump = f.read()
        f.close()
        now = datetime.utcnow()
        data = parseLatestNetworkStatus(jsonDump, scrapeTime=now)
        self.assertEqual(len(data), 59)
        expectedFirst = {
            'symbol': 'ALF',
            'scrape_time': now,
            'current_blocks': long(655258),
            'difficulty': Decimal("1.52109832"),
            'reward': Decimal(50),
            'hash_rate': long(10308452),
            'avg_hash_rate': Decimal("10308452.0000")
        }
        self.assertEqual(data[0], expectedFirst)
        expectedLast = {
            'symbol': 'GLC',
            'scrape_time': now,
            'current_blocks': long(300011),
            'difficulty': Decimal("0.768"),
            'reward': Decimal(100),
            'hash_rate': long(0),
            'avg_hash_rate': Decimal("0")
        }
        self.assertEqual(data[-1], expectedLast)

if __name__ == "__main__":
    unittest.main()

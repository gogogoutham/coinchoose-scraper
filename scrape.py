""" Core scraper for coinchoose.com. """
import coinchoose
from datetime import datetime
import logging
import os
import pg
import sys
import traceback

# Configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s:%(message)s',
    datefmt='%m/%d/%Y %I:%M:%S %p')


def saveToFile(content, prefix, extension):
    """Save given entity to a file."""
    f = open("{0}/data/{1}_{2}.{3}".format(
        os.path.dirname(os.path.abspath(__file__)),
        prefix,
        int((datetime.utcnow() - datetime(1970, 1, 1)).total_seconds()),
        extension),
        'w')
    f.write(content)
    f.close()


logging.info("""Starting scrape...""")
jsonDump = coinchoose.requestLatest()
scrapeTime = datetime.utcnow()
logging.info("""JSON request successful. Saving to file...""")
saveToFile(jsonDump, 'api', 'json')
logging.info("""Done. Parsing latest currencies...""")
currencies = coinchoose.parseLatestCurrencies(jsonDump)
logging.info("""Done. Inserting latest currencies into DB...""")
pg.insertLatestCurrencies(currencies)
logging.info("""Done. Parsing latest network status...""")
networkStatus = coinchoose.parseLatestNetworkStatus(
    jsonDump, scrapeTime=scrapeTime)
logging.info("""Done. Inserting latest network status into DB...""")
pg.insertLatestNetworkStatus(networkStatus)

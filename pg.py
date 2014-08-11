"""Module for storing coinchoose data in the database."""
import coinchoose
from datetime import datetime
from datetime import timedelta
from decimal import Decimal
import os
import psycopg2 as pg2
import psycopg2.extras as pg2ext
import random
import unittest

# Configuration variables
batchLimit = 1000
tables = {
    "currency": "currency",
    "currency_historical": "currency_historical",
    "network_status": "network_status",
    "network_status_latest": "network_status_latest"
}

# Pull in postgres configuration information
# Pull in postgres configuration information
dbcFile = open(
    "{0}/.pgpass".format(os.path.dirname(os.path.abspath(__file__))),
    'r')
dbcRaw = dbcFile.readline().strip().split(':')
dbcParams = {
    'database': dbcRaw[2],
    'user': dbcRaw[3],
    'password': dbcRaw[4],
    'host': dbcRaw[0],
    'port': dbcRaw[1]
}
dbcFile.close()

# Connection variable
conn = None


def connect():
    """Connect to the database."""
    global conn
    if conn is not None:
        return conn
    else:
        conn = pg2.connect(**dbcParams)
        return conn


def cursor():
    """"Pull a cursor from the connection."""
    return connect().cursor()


def dictCursor():
    """"Pull a dictionary cursor from the connection."""
    return connect().cursor(cursor_factory=pg2ext.RealDictCursor)


def _createStaging(tableName, cursor):
    """Create staging table."""
    stagingTable = "{0}_{1}".format(
        tableName, str(int(pow(10, random.random()*10))).zfill(10))
    cursor.execute("""CREATE TABLE {0} (LIKE {1}
        INCLUDING DEFAULTS)""".format(stagingTable, tableName))
    return stagingTable


def _dropStaging(tableName, cursor):
    """Drop staging table."""
    cursor.execute("""
        DROP TABLE {0}""".format(tableName))


def insertLatestCurrencies(data, withHistory=True):
    """Insert latest currency data."""
    cursor = dictCursor()
    targetTable = tables['currency']

    # Create staging table
    stagingTable = _createStaging(targetTable, cursor)

    # Move data into staging table
    cursor.executemany("""
        INSERT INTO {0} (
            symbol, name, algo)
        VALUES (
            %(symbol)s,
            %(name)s,
            %(algo)s
        )""".format(stagingTable), data)

    # Update any altered currencies
    cursor.execute("""
        UPDATE {0} tgt
        SET name = stg.name, algo = stg.algo,
            db_update_time = stg.db_update_time
        FROM {1} stg
        WHERE tgt.symbol = stg.symbol
        AND (tgt.name <> stg.name OR
            tgt.algo <> stg.algo)""".format(
        targetTable, stagingTable))

    # Merge any new currencies into target table
    cursor.execute("""
        INSERT INTO {0} (
            symbol, name, algo, db_update_time)
        (SELECT stg.*
        FROM {1} stg
        LEFT JOIN {0} tgt ON tgt.symbol = stg.symbol
        WHERE tgt.symbol IS NULL)""".format(
        targetTable, stagingTable))

    # If requested, merge data into the historical table
    if withHistory:
        historicalTable = tables['currency_historical']
        cursor.execute("""
            INSERT INTO {0} (
                symbol, name, algo, db_update_time)
            (SELECT stg.*
            FROM {1} stg
            LEFT JOIN {0} tgt ON
                tgt.symbol = stg.symbol AND
                tgt.name = stg.name AND
                tgt.algo = stg.algo
            WHERE tgt.symbol IS NULL)""".format(
            historicalTable, stagingTable))

    # Drop staging table
    _dropStaging(stagingTable, cursor)

    # Commit
    cursor.execute("""COMMIT""")


def insertLatestNetworkStatus(data):
    """Insert latest network status data."""
    cursor = dictCursor()
    targetTable = tables['network_status']
    latestTable = tables['network_status_latest']

    # Create staging table
    stagingTable = _createStaging(targetTable, cursor)

    # Move data into staging table
    cursor.executemany("""
        INSERT INTO {0}
            (scrape_time, symbol, current_blocks, difficulty,
            reward, hash_rate, avg_hash_rate)
        VALUES (
            %(scrape_time)s,
            %(symbol)s,
            %(current_blocks)s,
            %(difficulty)s,
            %(reward)s,
            %(hash_rate)s,
            %(avg_hash_rate)s
        )""".format(stagingTable), data)

    # Update target table where we have new data
    cursor.execute("""
        INSERT INTO {0}
            (scrape_time, symbol, current_blocks, difficulty,
            reward, hash_rate, avg_hash_rate, db_update_time)
        (SELECT stg.*
        FROM {1} stg
        LEFT JOIN {2} lt
            ON lt.symbol = stg.symbol
            AND lt.current_blocks = stg.current_blocks
            AND lt.difficulty = stg.difficulty
            AND lt.reward = stg.reward
            AND lt.hash_rate = stg.hash_rate
            AND lt.avg_hash_rate = stg.avg_hash_rate
        WHERE lt.scrape_time IS NULL)""".format(
        targetTable, stagingTable, latestTable))

    # Replace data in latest table with new data in staging
    cursor.execute("""DELETE FROM {0}""".format(latestTable))
    cursor.execute("""INSERT INTO {0}
        SELECT *
        FROM {1}""".format(latestTable, stagingTable))

    # Drop staging table
    _dropStaging(stagingTable, cursor)

    # Commit
    cursor.execute("""COMMIT""")


class PgTest(unittest.TestCase):

    """Testing suite for pg module."""

    def setUp(self):
        """Setup tables for test."""
        # Swap and sub configuration variables
        global tables
        self.tablesOriginal = tables
        tables = {}
        for key, table in self.tablesOriginal.iteritems():
            tables[key] = "{0}_test".format(table)
        global batchLimit
        self.batchLimitOriginal = batchLimit
        batchLimit = 20

        # Create test tables
        cur = cursor()
        for key, table in tables.iteritems():
            cur.execute("""CREATE TABLE IF NOT EXISTS
                {0} (LIKE {1} INCLUDING ALL)""".format(
                table, self.tablesOriginal[key]))
        cur.execute("""COMMIT""")

    def tearDown(self):
        """Teardown test tables."""
        # Drop test tables
        global tables
        cur = cursor()
        for table in tables.values():
            cur.execute("""DROP TABLE IF EXISTS
                {0}""".format(table))

        # Undo swap / sub
        tables = self.tablesOriginal
        global batchLimit
        batchLimit = self.batchLimitOriginal

    def testInsertLatestCurrencies(self):
        """Test insertLatestCurrencies function."""
        fileString = "{0}/example/api.json"
        f = open(fileString.format(
            os.path.dirname(os.path.abspath(__file__))), 'r')
        jsonDump = f.read()
        f.close()
        data = coinchoose.parseLatestCurrencies(jsonDump)
        insertLatestCurrencies(data)

        # Test out some basic count statistics
        cur = dictCursor()
        cur.execute("""SELECT COUNT(*) cnt FROM {0}""".format(
            tables['currency']))
        row = cur.fetchone()
        self.assertEqual(row['cnt'], 59)
        cur.execute("""SELECT COUNT(*) cnt FROM {0}""".format(
            tables['currency_historical']))
        row = cur.fetchone()
        self.assertEqual(row['cnt'], 59)

        # Test out contents of first and last row
        expectedFirst = {
            'symbol': 'ALF',
            'name': 'Alphacoin',
            'algo': 'scrypt'
        }
        cur.execute("""SELECT symbol, name, algo
            FROM {0}
            WHERE symbol = '{1}'""".format(
            tables['currency'], 'ALF'))
        datumFirst = cur.fetchone()
        self.assertEqual(datumFirst, expectedFirst)
        expectedLast = {
            'symbol': 'GLC',
            'name': 'GlobalCoin',
            'algo': 'scrypt'
        }
        cur.execute("""SELECT symbol, name, algo
            FROM {0}
            WHERE symbol = '{1}'""".format(
            tables['currency'], 'GLC'))
        datumLast = cur.fetchone()
        self.assertEqual(datumLast, expectedLast)

        # Update the data in a way that modifies what's in the DB
        updatedData = [
            {
                'symbol': 'ALF',
                'name': 'XXAlphacoinXX',
                'algo': 'scrypt'
            },
            {
                'symbol': 'GLC',
                'name': 'GlobalCoin',
                'algo': 'SHA-256'
            }
        ]
        insertLatestCurrencies(updatedData)
        cur.execute("""SELECT COUNT(*) cnt FROM {0}""".format(
            tables['currency']))
        row = cur.fetchone()
        self.assertEqual(row['cnt'], 59)
        cur.execute("""SELECT COUNT(*) cnt FROM {0}""".format(
            tables['currency_historical']))
        row = cur.fetchone()
        self.assertEqual(row['cnt'], 61)
        cur.execute("""SELECT symbol, name, algo
            FROM {0}
            WHERE symbol = '{1}'""".format(
            tables['currency'], 'ALF'))
        newDatumFirst = cur.fetchone()
        self.assertEqual(newDatumFirst, updatedData[0])
        cur.execute("""SELECT symbol, name, algo
            FROM {0}
            WHERE symbol = '{1}'""".format(
            tables['currency'], 'GLC'))
        newDatumFirst = cur.fetchone()
        self.assertEqual(newDatumFirst, updatedData[1])

    def testInsertLatestNetworkStatus(self):
        """Test insertLatestNetworkStatus function."""
        fileString = "{0}/example/api.json"
        f = open(fileString.format(
            os.path.dirname(os.path.abspath(__file__))), 'r')
        jsonDump = f.read()
        f.close()
        now = datetime.utcnow()
        data = coinchoose.parseLatestNetworkStatus(jsonDump, scrapeTime=now)
        insertLatestNetworkStatus(data)

        # Test out some basic count statistics
        cur = dictCursor()
        cur.execute("""SELECT COUNT(*) cnt FROM {0}""".format(
            tables['network_status']))
        row = cur.fetchone()
        self.assertEqual(row['cnt'], 59)
        cur.execute("""SELECT COUNT(*) cnt FROM {0}""".format(
            tables['network_status_latest']))
        row = cur.fetchone()
        self.assertEqual(row['cnt'], 59)

        # Test out contents of first and last row
        expectedFirst = {
            'symbol': 'ALF',
            'scrape_time': now,
            'current_blocks': long(655258),
            'difficulty': Decimal("1.52109832"),
            'reward': Decimal(50),
            'hash_rate': long(10308452),
            'avg_hash_rate': Decimal("10308452.0000")
        }
        cur.execute("""SELECT
                symbol, scrape_time, current_blocks, difficulty,
                reward, hash_rate, avg_hash_rate
            FROM {0}
            WHERE symbol = '{1}'""".format(
            tables['network_status'], 'ALF'))
        datumFirst = cur.fetchone()
        self.assertEqual(datumFirst, expectedFirst)
        expectedLast = {
            'symbol': 'GLC',
            'scrape_time': now,
            'current_blocks': long(300011),
            'difficulty': Decimal("0.768"),
            'reward': Decimal(100),
            'hash_rate': long(0),
            'avg_hash_rate': Decimal("0")
        }
        cur.execute("""SELECT
                symbol, scrape_time, current_blocks, difficulty,
                reward, hash_rate, avg_hash_rate
            FROM {0}
            WHERE symbol = '{1}'""".format(
            tables['network_status'], 'GLC'))
        datumLast = cur.fetchone()
        self.assertEqual(datumLast, expectedLast)

        # Update the data in a way that modifies some of  what's in the DB
        updatedData = [
            {
                'symbol': 'ALF',
                'scrape_time': now + timedelta(days=1),
                'current_blocks': long(655258),
                'difficulty': Decimal("1.52109832"),
                'reward': Decimal(50),
                'hash_rate': long(10308452),
                'avg_hash_rate': Decimal("10308452.0000")
            },
            {
                'symbol': 'GLC',
                'scrape_time': now + timedelta(days=1),
                'current_blocks': long(300155),
                'difficulty': Decimal("1.234"),
                'reward': Decimal(100),
                'hash_rate': long(20),
                'avg_hash_rate': Decimal("20.34")
            }
        ]
        insertLatestNetworkStatus(updatedData)
        cur.execute("""SELECT COUNT(*) cnt FROM {0}""".format(
            tables['network_status']))
        row = cur.fetchone()
        self.assertEqual(row['cnt'], 60)
        cur.execute("""SELECT COUNT(*) cnt FROM {0}""".format(
            tables['network_status_latest']))
        row = cur.fetchone()
        self.assertEqual(row['cnt'], 2)

        cur.execute("""SELECT COUNT(*) cnt
            FROM {0}
            WHERE symbol = '{1}'""".format(
            tables['network_status'], 'ALF'))
        row = cur.fetchone()
        self.assertEqual(row['cnt'], 1)
        cur.execute("""SELECT COUNT(*) cnt
            FROM {0}
            WHERE symbol ='{1}'""".format(
            tables['network_status'], 'GLC'))
        row = cur.fetchone()
        self.assertEqual(row['cnt'], 2)

        cur.execute("""SELECT
                symbol, scrape_time, current_blocks, difficulty,
                reward, hash_rate, avg_hash_rate
            FROM {0}
            WHERE symbol = '{1}'""".format(
            tables['network_status'], 'ALF'))
        newDatumFirst = cur.fetchone()
        self.assertEqual(newDatumFirst, expectedFirst)
        cur.execute("""SELECT
                symbol, scrape_time, current_blocks, difficulty,
                reward, hash_rate, avg_hash_rate
            FROM {0}
            WHERE symbol = '{1}'
            ORDER BY scrape_time
            DESC LIMIT 1""".format(
            tables['network_status'], 'GLC'))
        newDatumLast = cur.fetchone()
        self.assertEqual(newDatumLast, updatedData[-1])

        cur.execute("""SELECT
                symbol, scrape_time, current_blocks, difficulty,
                reward, hash_rate, avg_hash_rate
            FROM {0}
            WHERE symbol = '{1}'""".format(
            tables['network_status_latest'], 'ALF'))
        newDatumFirst = cur.fetchone()
        self.assertEqual(newDatumFirst, updatedData[0])
        cur.execute("""SELECT
                symbol, scrape_time, current_blocks, difficulty,
                reward, hash_rate, avg_hash_rate
            FROM {0}
            WHERE symbol = '{1}'""".format(
            tables['network_status_latest'], 'GLC'))
        newDatumLast = cur.fetchone()
        self.assertEqual(newDatumLast, updatedData[-1])

if __name__ == "__main__":
    unittest.main()

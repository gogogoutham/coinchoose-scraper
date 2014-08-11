coinchoose-scraper
===================

Python-based scraper / crawler for cryptocurrency network information on coinchooose.com

Installation
=============

a) Make sure required python packages are installed

```
pip install psycopg2 requests
```

b) Create tables in target PostgreSQL DB (see sql/)

c) Create .pgpass file in top-level of this directory containing connection info to the DB from previous step. Use the following format (9.1):

http://www.postgresql.org/docs/9.1/static/libpq-pgpass.html

d) Create "data" folder within the application folder, or change the _saveToFile method in memoizer.py to point to a different data directory.

Usage
=====



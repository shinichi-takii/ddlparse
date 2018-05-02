DDL Parse
=========

`PyPI version <https://pypi.python.org/pypi/ddlparse>`__ `Python
version <https://pypi.python.org/pypi/ddlparse>`__ `Travis CI Build
Status <https://travis-ci.org/shinichi-takii/ddlparse>`__ `Coveralls
Coverage
Status <https://coveralls.io/github/shinichi-takii/ddlparse?branch=master>`__
`codecov Coverage
Status <https://codecov.io/gh/shinichi-takii/ddlparse>`__ `Requirements
Status <https://requires.io/github/shinichi-takii/ddlparse/requirements/?branch=master>`__
`License <https://github.com/shinichi-takii/ddlparse/blob/master/LICENSE.md>`__

*DDL parase and Convert to BigQuery JSON schema module, available in
Python.*

--------------

Features
--------

-  DDL parse and get table schema information.
-  Currently, only the ``CREATE TABLE`` statement is supported.
-  Supported databases are MySQL, PostgreSQL, Oracle, Redshift.
-  Convert to `BigQuery JSON
   schema <https://cloud.google.com/bigquery/docs/schemas#creating_a_json_schema_file>`__.

Requirement
-----------

1. Python >= 3.4
2. `pyparsing <http://pyparsing.wikispaces.com/>`__

Installation
------------

Install
~~~~~~~

pip install:

.. code:: bash

   $ pip install ddlparse

command install:

.. code:: bash

   $ python setup.py install

Update
~~~~~~

pip update:

.. code:: bash

   $ pip install ddlparse --upgrade

Usage
-----

Example
~~~~~~~

.. code:: python

   from ddlparse import DdlParse

   sample_ddl = """
   CREATE TABLE My_Schema.Sample_Table (
     ID integer PRIMARY KEY,
     NAME varchar(100) NOT NULL,
     TOTAL bigint NOT NULL,
     AVG decimal(5,1) NOT NULL,
     CREATED_AT date, -- Oracle 'DATE' -> BigQuery 'DATETIME'
     UNIQUE (NAME)
   );
   """


   # parse pattern (1-1)
   table = DdlParse().parse(sample_ddl)

   # parse pattern (1-2) : Specify source database
   table = DdlParse().parse(ddl=sample_ddl, source_database=DdlParse.DATABASE.oracle)


   # parse pattern (2-1)
   parser = DdlParse(sample_ddl)
   table = parser.parse()

   print("* BigQuery Fields * : normal")
   print(table.to_bigquery_fields())


   # parse pattern (2-2) : Specify source database
   parser = DdlParse(ddl=sample_ddl, source_database=DdlParse.DATABASE.oracle)
   table = parser.parse()


   # parse pattern (3-1)
   parser = DdlParse()
   parser.ddl = sample_ddl
   table = parser.parse()

   # parse pattern (3-2) : Specify source database
   parser = DdlParse()
   parser.source_database = DdlParse.DATABASE.oracle
   parser.ddl = sample_ddl
   table = parser.parse()

   print("* BigQuery Fields * : Oracle")
   print(table.to_bigquery_fields())


   print("* TABLE *")
   print("schema = {} : name = {} : is_temp = {}".format(table.schema, table.name, table.is_temp))

   print("* BigQuery Fields *")
   print(table.to_bigquery_fields())

   print("* BigQuery Fields - column name to lower case / upper case *")
   print(table.to_bigquery_fields(DdlParse.NAME_CASE.lower))
   print(table.to_bigquery_fields(DdlParse.NAME_CASE.upper))

   print("* COLUMN *")
   for col in table.columns.values():
       print("name = {} : data_type = {} : length = {} : precision(=length) = {} : scale = {} : constraint = {} : not_null =  {} : PK =  {} : unique =  {} : BQ {}".format(
           col.name,
           col.data_type,
           col.length,
           col.precision,
           col.scale,
           col.constraint,
           col.not_null,
           col.primary_key,
           col.unique,
           col.to_bigquery_field()
           ))

   print("* Get Column object (case insensitive) *")
   print(table.columns["total"])

License
-------

`BSD 3-Clause
License <https://github.com/shinichi-takii/ddlparse/blob/master/LICENSE.md>`__

Author
------

Shinichi Takii shinichi.takii@gmail.com

Links
-----

-  Repository : https://github.com/shinichi-takii/ddlparse
-  PyPI Package : https://pypi.python.org/pypi/ddlparse

Special Thanks
--------------

-  pyparsing : http://pyparsing.wikispaces.com/

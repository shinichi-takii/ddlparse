DDL Parse
=========

|PyPI version| |Python version| |Travis CI Build Status| |Coveralls
Coverage Status| |codecov Coverage Status| |Requirements Status|
|License|

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
      CREATED_AT timestamp,
      UNIQUE (NAME)
    );
    """

    table = DdlParse().parse(sample_ddl)

    print("* TABLE *")
    print("schema = {} : name = {} : is_temp = {}".format(table.schema, table.name, table.is_temp))

    print("* BigQuery Fields *")
    print(table.to_bigquery_fields())

    print("* BigQuery Fields - column name to lower *")
    print(table.to_bigquery_fields(DdlParse.NAME_CASE.lower))

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

    print("* Get Column object *")
    print(table.columns["Name"])

License
-------

`BSD 3-Clause License <LICENSE>`__

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

.. |PyPI version| image:: https://img.shields.io/pypi/v/ddlparse.svg
   :target: https://pypi.python.org/pypi/ddlparse
.. |Python version| image:: https://img.shields.io/pypi/pyversions/ddlparse.svg
   :target: https://pypi.python.org/pypi/ddlparse
.. |Travis CI Build Status| image:: https://travis-ci.org/shinichi-takii/ddlparse.svg?branch=master
   :target: https://travis-ci.org/shinichi-takii/ddlparse
.. |Coveralls Coverage Status| image:: https://coveralls.io/repos/github/shinichi-takii/ddlparse/badge.svg?branch=master
   :target: https://coveralls.io/github/shinichi-takii/ddlparse?branch=master
.. |codecov Coverage Status| image:: https://codecov.io/gh/shinichi-takii/ddlparse/branch/master/graph/badge.svg
   :target: https://codecov.io/gh/shinichi-takii/ddlparse
.. |Requirements Status| image:: https://requires.io/github/shinichi-takii/ddlparse/requirements.svg?branch=master
   :target: https://requires.io/github/shinichi-takii/ddlparse/requirements/?branch=master
.. |License| image:: https://img.shields.io/badge/License-BSD%203--Clause-blue.svg
   :target: https://github.com/shinichi-takii/ddlparse/blob/master/LICENSE

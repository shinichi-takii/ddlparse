# DDL Parse

[![PyPI version](https://img.shields.io/pypi/v/ddlparse.svg)](https://pypi.python.org/pypi/ddlparse)
[![Python version](https://img.shields.io/pypi/pyversions/ddlparse.svg)](https://pypi.python.org/pypi/ddlparse)
[![Travis CI Build Status](https://travis-ci.org/shinichi-takii/ddlparse.svg?branch=master)](https://travis-ci.org/shinichi-takii/ddlparse)
[![Coveralls Coverage Status](https://coveralls.io/repos/github/shinichi-takii/ddlparse/badge.svg?branch=master)](https://coveralls.io/github/shinichi-takii/ddlparse?branch=master)
[![codecov Coverage Status](https://codecov.io/gh/shinichi-takii/ddlparse/branch/master/graph/badge.svg)](https://codecov.io/gh/shinichi-takii/ddlparse)
[![Requirements Status](https://requires.io/github/shinichi-takii/ddlparse/requirements.svg?branch=master)](https://requires.io/github/shinichi-takii/ddlparse/requirements/?branch=master)
[![License](https://img.shields.io/badge/License-BSD%203--Clause-blue.svg)](https://github.com/shinichi-takii/ddlparse/blob/master/LICENSE.md)

*DDL parase and Convert to BigQuery JSON schema module, available in Python.*

----

## Features

- DDL parse and get table schema information.
- Currently, only the `CREATE TABLE` statement is supported.
- Supported databases are MySQL, PostgreSQL, Oracle, Redshift.
- Convert to [BigQuery JSON schema](https://cloud.google.com/bigquery/docs/schemas#creating_a_json_schema_file).

## Requirement

1. Python >= 3.4
1. [pyparsing](http://pyparsing.wikispaces.com/)

## Installation

### Install

pip install:
```bash
$ pip install ddlparse
```

command install:
```bash
$ python setup.py install
```

### Update

pip update:
```bash
$ pip install ddlparse --upgrade
```

## Usage

### Example

```python
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

# parse pattern (1)
table = DdlParse().parse(sample_ddl)

# parse pattern (2)
parser = DdlParse()
parser.ddl = sample_ddl
table = parser.parse()

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
```

## License

[BSD 3-Clause License](LICENSE.md)

## Author

Shinichi Takii <shinichi.takii@gmail.com>

## Links

- Repository : https://github.com/shinichi-takii/ddlparse
- PyPI Package : https://pypi.python.org/pypi/ddlparse

## Special Thanks

- pyparsing : http://pyparsing.wikispaces.com/

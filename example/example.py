# -*- coding: utf-8 -*-
#
# Copyright (C) 2018 Shinichi Takii, shinichi.takii@gmail.com
#
# This module is part of python-ddlparse and is released under
# the BSD License: https://opensource.org/licenses/BSD-3-Clause

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

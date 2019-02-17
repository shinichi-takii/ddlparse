# -*- coding: utf-8 -*-
#
# Copyright (C) 2018 Shinichi Takii, shinichi.takii@gmail.com
#
# This module is part of python-ddlparse and is released under
# the BSD License: https://opensource.org/licenses/BSD-3-Clause

from ddlparse import DdlParse

sample_ddl = """
CREATE TABLE My_Schema.Sample_Table (
  Id integer PRIMARY KEY,
  Name varchar(100) NOT NULL,
  Total bigint NOT NULL,
  Avg decimal(5,1) NOT NULL,
  Created_At date, -- Oracle 'DATE' -> BigQuery 'DATETIME'
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
    col_info = []
    col_info.append("name = {}".format(col.name))
    col_info.append("data_type = {}".format(col.data_type))
    col_info.append("length = {}".format(col.length))
    col_info.append("precision(=length) = {}".format(col.precision))
    col_info.append("scale = {}".format(col.scale))
    col_info.append("constraint = {}".format(col.constraint))
    col_info.append("not_null = {}".format(col.not_null))
    col_info.append("PK = {}".format(col.primary_key))
    col_info.append("unique = {}".format(col.unique))
    col_info.append("bq_legacy_data_type = {}".format(col.bigquery_legacy_data_type))
    col_info.append("bq_standard_data_type = {}".format(col.bigquery_standard_data_type))
    col_info.append("BQ {}".format(col.to_bigquery_field()))
    print(" : ".join(col_info))

print("* DDL (CREATE TABLE) statements *")
print(table.to_bigquery_ddl())

print("* DDL (CREATE TABLE) statements - dataset name, table name and column name to lower case / upper case *")
print(table.to_bigquery_ddl(DdlParse.NAME_CASE.lower))
print(table.to_bigquery_ddl(DdlParse.NAME_CASE.upper))

print("* Get Column object (case insensitive) *")
print(table.columns["total"])

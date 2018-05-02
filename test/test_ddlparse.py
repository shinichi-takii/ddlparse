# -*- coding: utf-8 -*-

import pytest, re
from enum import IntEnum

from ddlparse.ddlparse import DdlParse


TEST_DATA = {
    "basic" :
    {
        "ddl" :
            """
            CREATE TABLE Sample_Table (
              Col_01 varchar(100) PRIMARY KEY,
              Col_02 char(200) NOT NULL UNIQUE,
              Col_03 text UNIQUE,
              Col_04 integer,
              Col_05 bigint,
              Col_06 serial,
              Col_07 year,
              Col_08 float,
              Col_09 double,
              Col_10 double precision,
              Col_11 real,
              Col_12 money,
              Col_13 date,
              Col_14 time,
              Col_15 datetime,
              Col_16 timestamp,
              Col_17 timestamp without time zone,
              Col_18 timestamptz,
              Col_19 timestamp with time zone,
              Col_20 bool,
              Col_21 numeric(10),
              Col_22 number(10),
              Col_23 decimal(20),
              Col_24 numeric(30,0),
              Col_25 number(30,0),
              Col_26 decimal(40,0),
              Col_27 numeric(50,1),
              Col_28 number(50,1),
              Col_29 decimal(60,2)
            );
            """,
        "database" : None,
        "table" : {"schema" : None, "name" : "Sample_Table", "temp" : False},
        "columns" : [
            {"name" : "Col_01", "type" : "VARCHAR", "length" : 100, "scale" : None, "not_null" : True, "pk" : True, "unique" : False, "constraint" : "PRIMARY KEY"},
            {"name" : "Col_02", "type" : "CHAR", "length" : 200, "scale" : None, "not_null" : True, "pk" : False, "unique" : True, "constraint" : "NOT NULL UNIQUE"},
            {"name" : "Col_03", "type" : "TEXT", "length" : None, "scale" : None, "not_null" : False, "pk" : False, "unique" : True, "constraint" : "UNIQUE"},
            {"name" : "Col_04", "type" : "INTEGER", "length" : None, "scale" : None, "not_null" : False, "pk" : False, "unique" : False, "constraint" : ""},
            {"name" : "Col_05", "type" : "BIGINT", "length" : None, "scale" : None, "not_null" : False, "pk" : False, "unique" : False, "constraint" : ""},
            {"name" : "Col_06", "type" : "SERIAL", "length" : None, "scale" : None, "not_null" : False, "pk" : False, "unique" : False, "constraint" : ""},
            {"name" : "Col_07", "type" : "YEAR", "length" : None, "scale" : None, "not_null" : False, "pk" : False, "unique" : False, "constraint" : ""},
            {"name" : "Col_08", "type" : "FLOAT", "length" : None, "scale" : None, "not_null" : False, "pk" : False, "unique" : False, "constraint" : ""},
            {"name" : "Col_09", "type" : "DOUBLE", "length" : None, "scale" : None, "not_null" : False, "pk" : False, "unique" : False, "constraint" : ""},
            {"name" : "Col_10", "type" : "DOUBLE PRECISION", "length" : None, "scale" : None, "not_null" : False, "pk" : False, "unique" : False, "constraint" : ""},
            {"name" : "Col_11", "type" : "REAL", "length" : None, "scale" : None, "not_null" : False, "pk" : False, "unique" : False, "constraint" : ""},
            {"name" : "Col_12", "type" : "MONEY", "length" : None, "scale" : None, "not_null" : False, "pk" : False, "unique" : False, "constraint" : ""},
            {"name" : "Col_13", "type" : "DATE", "length" : None, "scale" : None, "not_null" : False, "pk" : False, "unique" : False, "constraint" : ""},
            {"name" : "Col_14", "type" : "TIME", "length" : None, "scale" : None, "not_null" : False, "pk" : False, "unique" : False, "constraint" : ""},
            {"name" : "Col_15", "type" : "DATETIME", "length" : None, "scale" : None, "not_null" : False, "pk" : False, "unique" : False, "constraint" : ""},
            {"name" : "Col_16", "type" : "TIMESTAMP", "length" : None, "scale" : None, "not_null" : False, "pk" : False, "unique" : False, "constraint" : ""},
            {"name" : "Col_17", "type" : "TIMESTAMP WITHOUT TIME ZONE", "length" : None, "scale" : None, "not_null" : False, "pk" : False, "unique" : False, "constraint" : ""},
            {"name" : "Col_18", "type" : "TIMESTAMPTZ", "length" : None, "scale" : None, "not_null" : False, "pk" : False, "unique" : False, "constraint" : ""},
            {"name" : "Col_19", "type" : "TIMESTAMP WITH TIME ZONE", "length" : None, "scale" : None, "not_null" : False, "pk" : False, "unique" : False, "constraint" : ""},
            {"name" : "Col_20", "type" : "BOOL", "length" : None, "scale" : None, "not_null" : False, "pk" : False, "unique" : False, "constraint" : ""},
            {"name" : "Col_21", "type" : "NUMERIC", "length" : 10, "scale" : None, "not_null" : False, "pk" : False, "unique" : False, "constraint" : ""},
            {"name" : "Col_22", "type" : "NUMBER", "length" : 10, "scale" : None, "not_null" : False, "pk" : False, "unique" : False, "constraint" : ""},
            {"name" : "Col_23", "type" : "DECIMAL", "length" : 20, "scale" : None, "not_null" : False, "pk" : False, "unique" : False, "constraint" : ""},
            {"name" : "Col_24", "type" : "NUMERIC", "length" : 30, "scale" : None, "not_null" : False, "pk" : False, "unique" : False, "constraint" : ""},
            {"name" : "Col_25", "type" : "NUMBER", "length" : 30, "scale" : None, "not_null" : False, "pk" : False, "unique" : False, "constraint" : ""},
            {"name" : "Col_26", "type" : "DECIMAL", "length" : 40, "scale" : None, "not_null" : False, "pk" : False, "unique" : False, "constraint" : ""},
            {"name" : "Col_27", "type" : "NUMERIC", "length" : 50, "scale" : 1, "not_null" : False, "pk" : False, "unique" : False, "constraint" : ""},
            {"name" : "Col_28", "type" : "NUMBER", "length" : 50, "scale" : 1, "not_null" : False, "pk" : False, "unique" : False, "constraint" : ""},
            {"name" : "Col_29", "type" : "DECIMAL", "length" : 60, "scale" : 2, "not_null" : False, "pk" : False, "unique" : False, "constraint" : ""},
        ],
        "bq_field" : [
            '{"name": "Col_01", "type": "STRING", "mode": "REQUIRED"}',
            '{"name": "Col_02", "type": "STRING", "mode": "REQUIRED"}',
            '{"name": "Col_03", "type": "STRING", "mode": "NULLABLE"}',
            '{"name": "Col_04", "type": "INTEGER", "mode": "NULLABLE"}',
            '{"name": "Col_05", "type": "INTEGER", "mode": "NULLABLE"}',
            '{"name": "Col_06", "type": "INTEGER", "mode": "NULLABLE"}',
            '{"name": "Col_07", "type": "INTEGER", "mode": "NULLABLE"}',
            '{"name": "Col_08", "type": "FLOAT", "mode": "NULLABLE"}',
            '{"name": "Col_09", "type": "FLOAT", "mode": "NULLABLE"}',
            '{"name": "Col_10", "type": "FLOAT", "mode": "NULLABLE"}',
            '{"name": "Col_11", "type": "FLOAT", "mode": "NULLABLE"}',
            '{"name": "Col_12", "type": "FLOAT", "mode": "NULLABLE"}',
            '{"name": "Col_13", "type": "DATE", "mode": "NULLABLE"}',
            '{"name": "Col_14", "type": "TIME", "mode": "NULLABLE"}',
            '{"name": "Col_15", "type": "DATETIME", "mode": "NULLABLE"}',
            '{"name": "Col_16", "type": "DATETIME", "mode": "NULLABLE"}',
            '{"name": "Col_17", "type": "DATETIME", "mode": "NULLABLE"}',
            '{"name": "Col_18", "type": "TIMESTAMP", "mode": "NULLABLE"}',
            '{"name": "Col_19", "type": "TIMESTAMP", "mode": "NULLABLE"}',
            '{"name": "Col_20", "type": "BOOLEAN", "mode": "NULLABLE"}',
            '{"name": "Col_21", "type": "INTEGER", "mode": "NULLABLE"}',
            '{"name": "Col_22", "type": "INTEGER", "mode": "NULLABLE"}',
            '{"name": "Col_23", "type": "INTEGER", "mode": "NULLABLE"}',
            '{"name": "Col_24", "type": "INTEGER", "mode": "NULLABLE"}',
            '{"name": "Col_25", "type": "INTEGER", "mode": "NULLABLE"}',
            '{"name": "Col_26", "type": "INTEGER", "mode": "NULLABLE"}',
            '{"name": "Col_27", "type": "FLOAT", "mode": "NULLABLE"}',
            '{"name": "Col_28", "type": "FLOAT", "mode": "NULLABLE"}',
            '{"name": "Col_29", "type": "FLOAT", "mode": "NULLABLE"}',
        ],
    },

    "constraint_mysql" :
    {
        "ddl" :
            """
            CREATE TABLE Sample_Table (
              Col_01 varchar(100),
              Col_02 char(200),
              Col_03 integer,
              Col_04 double,
              Col_05 datetime,
              CONSTRAINT const_01 PRIMARY KEY (Col_01, Col_02),
              CONSTRAINT const_02 UNIQUE (Col_03, Col_04)
            );
            """,
        "database" : DdlParse.DATABASE.mysql,
        "table" : {"schema" : None, "name" : "Sample_Table", "temp" : False},
        "columns" : [
            {"name" : "Col_01", "type" : "VARCHAR", "length" : 100, "scale" : None, "not_null" : True, "pk" : True, "unique" : False, "constraint" : "PRIMARY KEY"},
            {"name" : "Col_02", "type" : "CHAR", "length" : 200, "scale" : None, "not_null" : True, "pk" : True, "unique" : False, "constraint" : "PRIMARY KEY"},
            {"name" : "Col_03", "type" : "INTEGER", "length" : None, "scale" : None, "not_null" : False, "pk" : False, "unique" : True, "constraint" : "UNIQUE"},
            {"name" : "Col_04", "type" : "DOUBLE", "length" : None, "scale" : None, "not_null" : False, "pk" : False, "unique" : True, "constraint" : "UNIQUE"},
            {"name" : "Col_05", "type" : "DATETIME", "length" : None, "scale" : None, "not_null" : False, "pk" : False, "unique" : False, "constraint" : ""},
        ],
        "bq_field" : [
            '{"name": "Col_01", "type": "STRING", "mode": "REQUIRED"}',
            '{"name": "Col_02", "type": "STRING", "mode": "REQUIRED"}',
            '{"name": "Col_03", "type": "INTEGER", "mode": "NULLABLE"}',
            '{"name": "Col_04", "type": "FLOAT", "mode": "NULLABLE"}',
            '{"name": "Col_05", "type": "DATETIME", "mode": "NULLABLE"}',
        ],
    },


    "constraint_postgres_oracle_redshift" :
    {
        "ddl" :
            """
            CREATE TABLE Sample_Table (
              Col_01 varchar(100),
              Col_02 char(200),
              Col_03 integer,
              Col_04 double,
              Col_05 datetime,
              PRIMARY KEY (Col_01, Col_02),
              UNIQUE (Col_03, Col_04),
              NOT NULL (Col_04, Col_05)
            );
            """,
        "database" : None,
        "table" : {"schema" : None, "name" : "Sample_Table", "temp" : False},
        "columns" : [
            {"name" : "Col_01", "type" : "VARCHAR", "length" : 100, "scale" : None, "not_null" : True, "pk" : True, "unique" : False, "constraint" : "PRIMARY KEY"},
            {"name" : "Col_02", "type" : "CHAR", "length" : 200, "scale" : None, "not_null" : True, "pk" : True, "unique" : False, "constraint" : "PRIMARY KEY"},
            {"name" : "Col_03", "type" : "INTEGER", "length" : None, "scale" : None, "not_null" : False, "pk" : False, "unique" : True, "constraint" : "UNIQUE"},
            {"name" : "Col_04", "type" : "DOUBLE", "length" : None, "scale" : None, "not_null" : True, "pk" : False, "unique" : True, "constraint" : "NOT NULL UNIQUE"},
            {"name" : "Col_05", "type" : "DATETIME", "length" : None, "scale" : None, "not_null" : True, "pk" : False, "unique" : False, "constraint" : "NOT NULL"},
        ],
        "bq_field" : [
            '{"name": "Col_01", "type": "STRING", "mode": "REQUIRED"}',
            '{"name": "Col_02", "type": "STRING", "mode": "REQUIRED"}',
            '{"name": "Col_03", "type": "INTEGER", "mode": "NULLABLE"}',
            '{"name": "Col_04", "type": "FLOAT", "mode": "REQUIRED"}',
            '{"name": "Col_05", "type": "DATETIME", "mode": "REQUIRED"}',
        ],
    },

    "default_postgres_redshift" :
    {
        "ddl" :
            """
            CREATE TABLE Sample_Table (
              Col_01 char(1) DEFAULT '0'::bpchar,
              Col_02 char(1) DEFAULT '0'::bpchar
            );
            """,
        "database" : None,
        "table" : {"schema" : None, "name" : "Sample_Table", "temp" : False},
        "columns" : [
            {"name" : "Col_01", "type" : "CHAR", "length" : 1, "scale" : None, "not_null" : False, "pk" : False, "unique" : False, "constraint" : ""},
            {"name" : "Col_02", "type" : "CHAR", "length" : 1, "scale" : None, "not_null" : False, "pk" : False, "unique" : False, "constraint" : ""},
        ],
        "bq_field" : [
            '{"name": "Col_01", "type": "STRING", "mode": "NULLABLE"}',
            '{"name": "Col_02", "type": "STRING", "mode": "NULLABLE"}',
        ],
    },

    "datatype_oracle" :
    {
        "ddl" :
            """
            CREATE TABLE Sample_Table (
              Col_01 date,
              Col_02 number(1),
              Col_03 number(1,2),
              Col_04 number,
              Col_05 clob,
              Col_06 nclob
            );
            """,
        "database" : DdlParse.DATABASE.oracle,
        "table" : {"schema" : None, "name" : "Sample_Table", "temp" : False},
        "columns" : [
            {"name" : "Col_01", "type" : "DATE", "length" : None, "scale" : None, "not_null" : False, "pk" : False, "unique" : False, "constraint" : ""},
            {"name" : "Col_02", "type" : "NUMBER", "length" : 1, "scale" : None, "not_null" : False, "pk" : False, "unique" : False, "constraint" : ""},
            {"name" : "Col_03", "type" : "NUMBER", "length" : 1, "scale" : 2, "not_null" : False, "pk" : False, "unique" : False, "constraint" : ""},
            {"name" : "Col_04", "type" : "NUMBER", "length" : None, "scale" : None, "not_null" : False, "pk" : False, "unique" : False, "constraint" : ""},
            {"name" : "Col_05", "type" : "CLOB", "length" : None, "scale" : None, "not_null" : False, "pk" : False, "unique" : False, "constraint" : ""},
            {"name" : "Col_06", "type" : "NCLOB", "length" : None, "scale" : None, "not_null" : False, "pk" : False, "unique" : False, "constraint" : ""},
        ],
        "bq_field" : [
            '{"name": "Col_01", "type": "DATETIME", "mode": "NULLABLE"}',
            '{"name": "Col_02", "type": "INTEGER", "mode": "NULLABLE"}',
            '{"name": "Col_03", "type": "FLOAT", "mode": "NULLABLE"}',
            '{"name": "Col_04", "type": "FLOAT", "mode": "NULLABLE"}',
            '{"name": "Col_05", "type": "STRING", "mode": "NULLABLE"}',
            '{"name": "Col_06", "type": "STRING", "mode": "NULLABLE"}',
        ],
    },

    "name_backquote" :
    {
        "ddl" :
            """
            CREATE TABLE `Sample_Table` (
              `Col_01` varchar(100),
              `Col_02` char(200),
              `Col_03` integer,
              `Col_04` double,
              `Col_05` datetime,
              PRIMARY KEY (`Col_01`, `Col_02`),
              UNIQUE (`Col_03`, `Col_04`)
            );
            """,
        "database" : None,
        "table" : {"schema" : None, "name" : "Sample_Table", "temp" : False},
        "columns" : [
            {"name" : "Col_01", "type" : "VARCHAR", "length" : 100, "scale" : None, "not_null" : True, "pk" : True, "unique" : False, "constraint" : "PRIMARY KEY"},
            {"name" : "Col_02", "type" : "CHAR", "length" : 200, "scale" : None, "not_null" : True, "pk" : True, "unique" : False, "constraint" : "PRIMARY KEY"},
            {"name" : "Col_03", "type" : "INTEGER", "length" : None, "scale" : None, "not_null" : False, "pk" : False, "unique" : True, "constraint" : "UNIQUE"},
            {"name" : "Col_04", "type" : "DOUBLE", "length" : None, "scale" : None, "not_null" : False, "pk" : False, "unique" : True, "constraint" : "UNIQUE"},
            {"name" : "Col_05", "type" : "DATETIME", "length" : None, "scale" : None, "not_null" : False, "pk" : False, "unique" : False, "constraint" : ""},
        ],
        "bq_field" : [
            '{"name": "Col_01", "type": "STRING", "mode": "REQUIRED"}',
            '{"name": "Col_02", "type": "STRING", "mode": "REQUIRED"}',
            '{"name": "Col_03", "type": "INTEGER", "mode": "NULLABLE"}',
            '{"name": "Col_04", "type": "FLOAT", "mode": "NULLABLE"}',
            '{"name": "Col_05", "type": "DATETIME", "mode": "NULLABLE"}',
        ],
    },

    "name_doublequote" :
    {
        "ddl" :
            """
            CREATE TABLE "Sample_Table" (
              "Col_01" varchar(100),
              "Col_02" char(200),
              "Col_03" integer,
              "Col_04" double,
              "Col_05" datetime,
              PRIMARY KEY ("Col_01", "Col_02"),
              UNIQUE ("Col_03", "Col_04")
            );
            """,
        "database" : None,
        "table" : {"schema" : None, "name" : "Sample_Table", "temp" : False},
        "columns" : [
            {"name" : "Col_01", "type" : "VARCHAR", "length" : 100, "scale" : None, "not_null" : True, "pk" : True, "unique" : False, "constraint" : "PRIMARY KEY"},
            {"name" : "Col_02", "type" : "CHAR", "length" : 200, "scale" : None, "not_null" : True, "pk" : True, "unique" : False, "constraint" : "PRIMARY KEY"},
            {"name" : "Col_03", "type" : "INTEGER", "length" : None, "scale" : None, "not_null" : False, "pk" : False, "unique" : True, "constraint" : "UNIQUE"},
            {"name" : "Col_04", "type" : "DOUBLE", "length" : None, "scale" : None, "not_null" : False, "pk" : False, "unique" : True, "constraint" : "UNIQUE"},
            {"name" : "Col_05", "type" : "DATETIME", "length" : None, "scale" : None, "not_null" : False, "pk" : False, "unique" : False, "constraint" : ""},
        ],
        "bq_field" : [
            '{"name": "Col_01", "type": "STRING", "mode": "REQUIRED"}',
            '{"name": "Col_02", "type": "STRING", "mode": "REQUIRED"}',
            '{"name": "Col_03", "type": "INTEGER", "mode": "NULLABLE"}',
            '{"name": "Col_04", "type": "FLOAT", "mode": "NULLABLE"}',
            '{"name": "Col_05", "type": "DATETIME", "mode": "NULLABLE"}',
        ],
    },

    "temp_table" :
    {
        "ddl" :
            """
            CREATE TEMP TABLE Sample_Table (
              Col_01 varchar(100)
            );
            """,
        "database" : None,
        "table" : {"schema" : None, "name" : "Sample_Table", "temp" : True},
        "columns" : [
            {"name" : "Col_01", "type" : "VARCHAR", "length" : 100, "scale" : None, "not_null" : False, "pk" : False, "unique" : False, "constraint" : ""},
        ],
        "bq_field" : [
            '{"name": "Col_01", "type": "STRING", "mode": "NULLABLE"}',
        ],
    },

    "table_with_schema" :
    {
        "ddl" :
            """
            CREATE TABLE "My_Schema"."Sample_Table" (
              Col_01 varchar(100)
            );
            """,
        "database" : None,
        "table" : {"schema" : "My_Schema", "name" : "Sample_Table", "temp" : False},
        "columns" : [
            {"name" : "Col_01", "type" : "VARCHAR", "length" : 100, "scale" : None, "not_null" : False, "pk" : False, "unique" : False, "constraint" : ""},
        ],
        "bq_field" : [
            '{"name": "Col_01", "type": "STRING", "mode": "NULLABLE"}',
        ],
    },
}


DDL_SET_PATTERN = IntEnum("DDL_SET_PATTERN", "method property")


@pytest.mark.parametrize(("test_case", "parse_pattern"), [
    ("basic", DDL_SET_PATTERN.method),
    ("basic", DDL_SET_PATTERN.property),
    ("constraint_mysql", DDL_SET_PATTERN.method),
    ("constraint_postgres_oracle_redshift", DDL_SET_PATTERN.method),
    ("default_postgres_redshift", DDL_SET_PATTERN.method),
    ("datatype_oracle", DDL_SET_PATTERN.method),
    ("name_backquote", DDL_SET_PATTERN.method),
    ("name_doublequote", DDL_SET_PATTERN.method),
    ("temp_table", DDL_SET_PATTERN.method),
    ("table_with_schema", DDL_SET_PATTERN.method),
])
def test_parse(test_case, parse_pattern):
    # Get test ddl script
    data = TEST_DATA[test_case]

    # Create parse object instance
    ddlparse = DdlParse()

    # Set source database option

    # Set source database option & Parse ddl
    if parse_pattern == DDL_SET_PATTERN.method:
        if data["database"] is not None:
            table = ddlparse.parse(data["ddl"], data["database"])
        else:
            table = ddlparse.parse(data["ddl"])
    else:
        if data["database"] is not None:
            ddlparse.source_database = data["database"]
        ddlparse.ddl = data["ddl"]
        table = ddlparse.parse()


    # Check DDL
    assert ddlparse.ddl == data["ddl"]

    # Check source database option
    assert ddlparse.source_database == data["database"]

    # Check table
    assert table.schema == data["table"]["schema"] if data["table"]["schema"] is not None else table.schema is None
    assert table.name == data["table"]["name"]
    assert table.is_temp == data["table"]["temp"]
    assert table.source_database == data["database"]


    # Check columns
    data_bq_field_lower = []
    data_bq_field_upper = []

    i = 0
    for col in table.columns.values():
        data_col = data["columns"][i]

        assert col.name == data_col["name"]
        assert col.data_type == data_col["type"]
        assert col.length == data_col["length"] if data_col["length"] is not None else col.length is None
        assert col.precision == data_col["length"] if data_col["length"] is not None else col.length is None
        assert col.scale == data_col["scale"] if data_col["scale"] is not None else col.scale is None
        assert col.not_null == data_col["not_null"]
        assert col.primary_key == data_col["pk"]
        assert col.unique == data_col["unique"]
        assert col.constraint == data_col["constraint"]
        assert col.source_database == data["database"]

        data_bq_field = data["bq_field"][i]
        assert col.to_bigquery_field() == data_bq_field

        data_bq_field_lower.append(re.sub(r'({"name": ")Col', r'\1col', data_bq_field))
        assert col.to_bigquery_field(col.NAME_CASE.lower) == data_bq_field_lower[-1]

        data_bq_field_upper.append(re.sub(r'({"name": ")Col', r'\1COL', data_bq_field))
        assert col.to_bigquery_field(col.NAME_CASE.upper) == data_bq_field_upper[-1]

        i += 1


    # Check BigQuery fields format of DdlParseColumnDict
    assert table.columns.to_bigquery_fields() == "[{}]".format(",".join(data["bq_field"]))
    assert table.columns.to_bigquery_fields(col.NAME_CASE.lower) == "[{}]".format(",".join(data_bq_field_lower))
    assert table.columns.to_bigquery_fields(col.NAME_CASE.upper) == "[{}]".format(",".join(data_bq_field_upper))
    assert table.columns.source_database == data["database"]

    # Check BigQuery fields format of DdlParseTable
    assert table.columns.to_bigquery_fields() == table.to_bigquery_fields()
    assert table.columns.to_bigquery_fields(col.NAME_CASE.lower) == table.to_bigquery_fields(col.NAME_CASE.lower)
    assert table.columns.to_bigquery_fields(col.NAME_CASE.upper) == table.to_bigquery_fields(col.NAME_CASE.upper)


def test_exception_ddl():
    # Create parse object instance
    ddlparse = DdlParse()

    # Do not set DDL

    # Error : DDL is not specified
    with pytest.raises(ValueError):
        ddlparse.parse()


def test_exception_bq_data_type():
    ddl = """
        CREATE TABLE Sample_Table (
          Col_01 NG_DATA_TYPE,
        );
        """

    # Parse DDL
    table = DdlParse().parse(ddl)

    # Error: Unknown data type
    with pytest.raises(ValueError):
        print(table.columns["col_01"].bigquery_data_type)

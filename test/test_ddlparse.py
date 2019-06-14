# -*- coding: utf-8 -*-

import pytest, re, textwrap
from enum import IntEnum

from ddlparse.ddlparse import DdlParse


TEST_DATA = {
    "basic" :
    {
        "ddl" :
            """
            CREATE TABLE Sample_Table (
              Col_01 varchar(100) PRIMARY KEY,
              -- comment
              -- comment
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
              Col_28 number(50,1), -- comment
              Col_29 decimal(60,2)  -- comment
            );
            """,
        "database" : None,
        "table" : {"schema" : None, "name" : "Sample_Table", "temp" : False},
        "columns" : [
            {"name" : "Col_01", "type" : "VARCHAR", "length" : 100, "scale" : None, "array_dimensional" : 0, "not_null" : True, "pk" : True, "unique" : False, "constraint" : "PRIMARY KEY", "description" : None},
            {"name" : "Col_02", "type" : "CHAR", "length" : 200, "scale" : None, "array_dimensional" : 0, "not_null" : True, "pk" : False, "unique" : True, "constraint" : "NOT NULL UNIQUE", "description" : None},
            {"name" : "Col_03", "type" : "TEXT", "length" : None, "scale" : None, "array_dimensional" : 0, "not_null" : False, "pk" : False, "unique" : True, "constraint" : "UNIQUE", "description" : None},
            {"name" : "Col_04", "type" : "INTEGER", "length" : None, "scale" : None, "array_dimensional" : 0, "not_null" : False, "pk" : False, "unique" : False, "constraint" : "", "description" : None},
            {"name" : "Col_05", "type" : "BIGINT", "length" : None, "scale" : None, "array_dimensional" : 0, "not_null" : False, "pk" : False, "unique" : False, "constraint" : "", "description" : None},
            {"name" : "Col_06", "type" : "SERIAL", "length" : None, "scale" : None, "array_dimensional" : 0, "not_null" : False, "pk" : False, "unique" : False, "constraint" : "", "description" : None},
            {"name" : "Col_07", "type" : "YEAR", "length" : None, "scale" : None, "array_dimensional" : 0, "not_null" : False, "pk" : False, "unique" : False, "constraint" : "", "description" : None},
            {"name" : "Col_08", "type" : "FLOAT", "length" : None, "scale" : None, "array_dimensional" : 0, "not_null" : False, "pk" : False, "unique" : False, "constraint" : "", "description" : None},
            {"name" : "Col_09", "type" : "DOUBLE", "length" : None, "scale" : None, "array_dimensional" : 0, "not_null" : False, "pk" : False, "unique" : False, "constraint" : "", "description" : None},
            {"name" : "Col_10", "type" : "DOUBLE PRECISION", "length" : None, "scale" : None, "array_dimensional" : 0, "not_null" : False, "pk" : False, "unique" : False, "constraint" : "", "description" : None},
            {"name" : "Col_11", "type" : "REAL", "length" : None, "scale" : None, "array_dimensional" : 0, "not_null" : False, "pk" : False, "unique" : False, "constraint" : "", "description" : None},
            {"name" : "Col_12", "type" : "MONEY", "length" : None, "scale" : None, "array_dimensional" : 0, "not_null" : False, "pk" : False, "unique" : False, "constraint" : "", "description" : None},
            {"name" : "Col_13", "type" : "DATE", "length" : None, "scale" : None, "array_dimensional" : 0, "not_null" : False, "pk" : False, "unique" : False, "constraint" : "", "description" : None},
            {"name" : "Col_14", "type" : "TIME", "length" : None, "scale" : None, "array_dimensional" : 0, "not_null" : False, "pk" : False, "unique" : False, "constraint" : "", "description" : None},
            {"name" : "Col_15", "type" : "DATETIME", "length" : None, "scale" : None, "array_dimensional" : 0, "not_null" : False, "pk" : False, "unique" : False, "constraint" : "", "description" : None},
            {"name" : "Col_16", "type" : "TIMESTAMP", "length" : None, "scale" : None, "array_dimensional" : 0, "not_null" : False, "pk" : False, "unique" : False, "constraint" : "", "description" : None},
            {"name" : "Col_17", "type" : "TIMESTAMP WITHOUT TIME ZONE", "length" : None, "scale" : None, "array_dimensional" : 0, "not_null" : False, "pk" : False, "unique" : False, "constraint" : "", "description" : None},
            {"name" : "Col_18", "type" : "TIMESTAMPTZ", "length" : None, "scale" : None, "array_dimensional" : 0, "not_null" : False, "pk" : False, "unique" : False, "constraint" : "", "description" : None},
            {"name" : "Col_19", "type" : "TIMESTAMP WITH TIME ZONE", "length" : None, "scale" : None, "array_dimensional" : 0, "not_null" : False, "pk" : False, "unique" : False, "constraint" : "", "description" : None},
            {"name" : "Col_20", "type" : "BOOL", "length" : None, "scale" : None, "array_dimensional" : 0, "not_null" : False, "pk" : False, "unique" : False, "constraint" : "", "description" : None},
            {"name" : "Col_21", "type" : "NUMERIC", "length" : 10, "scale" : None, "array_dimensional" : 0, "not_null" : False, "pk" : False, "unique" : False, "constraint" : "", "description" : None},
            {"name" : "Col_22", "type" : "NUMBER", "length" : 10, "scale" : None, "array_dimensional" : 0, "not_null" : False, "pk" : False, "unique" : False, "constraint" : "", "description" : None},
            {"name" : "Col_23", "type" : "DECIMAL", "length" : 20, "scale" : None, "array_dimensional" : 0, "not_null" : False, "pk" : False, "unique" : False, "constraint" : "", "description" : None},
            {"name" : "Col_24", "type" : "NUMERIC", "length" : 30, "scale" : None, "array_dimensional" : 0, "not_null" : False, "pk" : False, "unique" : False, "constraint" : "", "description" : None},
            {"name" : "Col_25", "type" : "NUMBER", "length" : 30, "scale" : None, "array_dimensional" : 0, "not_null" : False, "pk" : False, "unique" : False, "constraint" : "", "description" : None},
            {"name" : "Col_26", "type" : "DECIMAL", "length" : 40, "scale" : None, "array_dimensional" : 0, "not_null" : False, "pk" : False, "unique" : False, "constraint" : "", "description" : None},
            {"name" : "Col_27", "type" : "NUMERIC", "length" : 50, "scale" : 1, "array_dimensional" : 0, "not_null" : False, "pk" : False, "unique" : False, "constraint" : "", "description" : None},
            {"name" : "Col_28", "type" : "NUMBER", "length" : 50, "scale" : 1, "array_dimensional" : 0, "not_null" : False, "pk" : False, "unique" : False, "constraint" : "", "description" : None},
            {"name" : "Col_29", "type" : "DECIMAL", "length" : 60, "scale" : 2, "array_dimensional" : 0, "not_null" : False, "pk" : False, "unique" : False, "constraint" : "", "description" : None},
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
        "bq_standard_data_type" : [
            "STRING",
            "STRING",
            "STRING",
            "INT64",
            "INT64",
            "INT64",
            "INT64",
            "FLOAT64",
            "FLOAT64",
            "FLOAT64",
            "FLOAT64",
            "FLOAT64",
            "DATE",
            "TIME",
            "DATETIME",
            "DATETIME",
            "DATETIME",
            "TIMESTAMP",
            "TIMESTAMP",
            "BOOL",
            "INT64",
            "INT64",
            "INT64",
            "INT64",
            "INT64",
            "INT64",
            "FLOAT64",
            "FLOAT64",
            "FLOAT64",
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
              CONSTRAINT \"const_02\" UNIQUE (Col_03, Col_04),
              CONSTRAINT \"const_03\" FOREIGN KEY (Col_04, \"Col_05\") REFERENCES ref_table_01 (\"Col_04\", Col_05)
            );
            """,
        "database" : DdlParse.DATABASE.mysql,
        "table" : {"schema" : None, "name" : "Sample_Table", "temp" : False},
        "columns" : [
            {"name" : "Col_01", "type" : "VARCHAR", "length" : 100, "scale" : None, "array_dimensional" : 0, "not_null" : True, "pk" : True, "unique" : False, "constraint" : "PRIMARY KEY", "description" : None},
            {"name" : "Col_02", "type" : "CHAR", "length" : 200, "scale" : None, "array_dimensional" : 0, "not_null" : True, "pk" : True, "unique" : False, "constraint" : "PRIMARY KEY", "description" : None},
            {"name" : "Col_03", "type" : "INTEGER", "length" : None, "scale" : None, "array_dimensional" : 0, "not_null" : False, "pk" : False, "unique" : True, "constraint" : "UNIQUE", "description" : None},
            {"name" : "Col_04", "type" : "DOUBLE", "length" : None, "scale" : None, "array_dimensional" : 0, "not_null" : False, "pk" : False, "unique" : True, "constraint" : "UNIQUE", "description" : None},
            {"name" : "Col_05", "type" : "DATETIME", "length" : None, "scale" : None, "array_dimensional" : 0, "not_null" : False, "pk" : False, "unique" : False, "constraint" : "", "description" : None},
        ],
        "bq_field" : [
            '{"name": "Col_01", "type": "STRING", "mode": "REQUIRED"}',
            '{"name": "Col_02", "type": "STRING", "mode": "REQUIRED"}',
            '{"name": "Col_03", "type": "INTEGER", "mode": "NULLABLE"}',
            '{"name": "Col_04", "type": "FLOAT", "mode": "NULLABLE"}',
            '{"name": "Col_05", "type": "DATETIME", "mode": "NULLABLE"}',
        ],
        "bq_standard_data_type" : [
            "STRING",
            "STRING",
            "INT64",
            "FLOAT64",
            "DATETIME",
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
              NOT NULL (Col_04, Col_05),
              FOREIGN KEY (Col_04, Col_05) REFERENCES ref_table_01 (Col_04, Col_05)
            );
            """,
        "database" : None,
        "table" : {"schema" : None, "name" : "Sample_Table", "temp" : False},
        "columns" : [
            {"name" : "Col_01", "type" : "VARCHAR", "length" : 100, "scale" : None, "array_dimensional" : 0, "not_null" : True, "pk" : True, "unique" : False, "constraint" : "PRIMARY KEY", "description" : None},
            {"name" : "Col_02", "type" : "CHAR", "length" : 200, "scale" : None, "array_dimensional" : 0, "not_null" : True, "pk" : True, "unique" : False, "constraint" : "PRIMARY KEY", "description" : None},
            {"name" : "Col_03", "type" : "INTEGER", "length" : None, "scale" : None, "array_dimensional" : 0, "not_null" : False, "pk" : False, "unique" : True, "constraint" : "UNIQUE", "description" : None},
            {"name" : "Col_04", "type" : "DOUBLE", "length" : None, "scale" : None, "array_dimensional" : 0, "not_null" : True, "pk" : False, "unique" : True, "constraint" : "NOT NULL UNIQUE", "description" : None},
            {"name" : "Col_05", "type" : "DATETIME", "length" : None, "scale" : None, "array_dimensional" : 0, "not_null" : True, "pk" : False, "unique" : False, "constraint" : "NOT NULL", "description" : None},
        ],
        "bq_field" : [
            '{"name": "Col_01", "type": "STRING", "mode": "REQUIRED"}',
            '{"name": "Col_02", "type": "STRING", "mode": "REQUIRED"}',
            '{"name": "Col_03", "type": "INTEGER", "mode": "NULLABLE"}',
            '{"name": "Col_04", "type": "FLOAT", "mode": "REQUIRED"}',
            '{"name": "Col_05", "type": "DATETIME", "mode": "REQUIRED"}',
        ],
        "bq_standard_data_type" : [
            "STRING",
            "STRING",
            "INT64",
            "FLOAT64",
            "DATETIME",
        ],
    },

    "default_postgres_redshift" :
    {
        "ddl" :
            """
            CREATE TABLE Sample_Table (
              Col_01 char(1) DEFAULT '0'::bpchar,
              Col_02 char(1) DEFAULT '0'::bpchar,
              Col_03 integer DEFAULT 0,
              Col_04 numeric(10) DEFAULT 0,
              Col_05 numeric(20,3) DEFAULT 0.0,
              Col_06 character varying[] DEFAULT '{}'::character varying[]
            );
            """,
        "database" : None,
        "table" : {"schema" : None, "name" : "Sample_Table", "temp" : False},
        "columns" : [
            {"name" : "Col_01", "type" : "CHAR", "length" : 1, "scale" : None, "array_dimensional" : 0, "not_null" : False, "pk" : False, "unique" : False, "constraint" : "", "description" : None},
            {"name" : "Col_02", "type" : "CHAR", "length" : 1, "scale" : None, "array_dimensional" : 0, "not_null" : False, "pk" : False, "unique" : False, "constraint" : "", "description" : None},
            {"name" : "Col_03", "type" : "INTEGER", "length" : None, "scale" : None, "array_dimensional" : 0, "not_null" : False, "pk" : False, "unique" : False, "constraint" : "", "description" : None},
            {"name" : "Col_04", "type" : "NUMERIC", "length" : 10, "scale" : None, "array_dimensional" : 0, "not_null" : False, "pk" : False, "unique" : False, "constraint" : "", "description" : None},
            {"name" : "Col_05", "type" : "NUMERIC", "length" : 20, "scale" : 3, "array_dimensional" : 0, "not_null" : False, "pk" : False, "unique" : False, "constraint" : "", "description" : None},
            {"name" : "Col_06", "type" : "CHARACTER VARYING", "length" : None, "scale" : None, "array_dimensional" : 1, "not_null" : False, "pk" : False, "unique" : False, "constraint" : "", "description" : None},
        ],
        "bq_field" : [
            '{"name": "Col_01", "type": "STRING", "mode": "NULLABLE"}',
            '{"name": "Col_02", "type": "STRING", "mode": "NULLABLE"}',
            '{"name": "Col_03", "type": "INTEGER", "mode": "NULLABLE"}',
            '{"name": "Col_04", "type": "INTEGER", "mode": "NULLABLE"}',
            '{"name": "Col_05", "type": "FLOAT", "mode": "NULLABLE"}',
            '{"name": "Col_06", "type": "STRING", "mode": "REPEATED"}',
        ],
        "bq_standard_data_type" : [
            "STRING",
            "STRING",
            "INT64",
            "INT64",
            "FLOAT64",
            "STRING",
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
              Col_05 varchar2(3 char), -- character semantics
              Col_06 varchar2(4 byte), -- byte semantics
              Col_07 clob,
              Col_08 nclob
            );
            """,
        "database" : DdlParse.DATABASE.oracle,
        "table" : {"schema" : None, "name" : "Sample_Table", "temp" : False},
        "columns" : [
            {"name" : "Col_01", "type" : "DATE", "length" : None, "scale" : None, "array_dimensional" : 0, "not_null" : False, "pk" : False, "unique" : False, "constraint" : "", "description" : None},
            {"name" : "Col_02", "type" : "NUMBER", "length" : 1, "scale" : None, "array_dimensional" : 0, "not_null" : False, "pk" : False, "unique" : False, "constraint" : "", "description" : None},
            {"name" : "Col_03", "type" : "NUMBER", "length" : 1, "scale" : 2, "array_dimensional" : 0, "not_null" : False, "pk" : False, "unique" : False, "constraint" : "", "description" : None},
            {"name" : "Col_04", "type" : "NUMBER", "length" : None, "scale" : None, "array_dimensional" : 0, "not_null" : False, "pk" : False, "unique" : False, "constraint" : "", "description" : None},
            {"name" : "Col_05", "type" : "VARCHAR2", "length" : 3, "scale" : None, "array_dimensional" : 0, "not_null" : False, "pk" : False, "unique" : False, "constraint" : "", "description" : None},
            {"name" : "Col_06", "type" : "VARCHAR2", "length" : 4, "scale" : None, "array_dimensional" : 0, "not_null" : False, "pk" : False, "unique" : False, "constraint" : "", "description" : None},
            {"name" : "Col_07", "type" : "CLOB", "length" : None, "scale" : None, "array_dimensional" : 0, "not_null" : False, "pk" : False, "unique" : False, "constraint" : "", "description" : None},
            {"name" : "Col_08", "type" : "NCLOB", "length" : None, "scale" : None, "array_dimensional" : 0, "not_null" : False, "pk" : False, "unique" : False, "constraint" : "", "description" : None},
        ],
        "bq_field" : [
            '{"name": "Col_01", "type": "DATETIME", "mode": "NULLABLE"}',
            '{"name": "Col_02", "type": "INTEGER", "mode": "NULLABLE"}',
            '{"name": "Col_03", "type": "FLOAT", "mode": "NULLABLE"}',
            '{"name": "Col_04", "type": "FLOAT", "mode": "NULLABLE"}',
            '{"name": "Col_05", "type": "STRING", "mode": "NULLABLE"}',
            '{"name": "Col_06", "type": "STRING", "mode": "NULLABLE"}',
            '{"name": "Col_07", "type": "STRING", "mode": "NULLABLE"}',
            '{"name": "Col_08", "type": "STRING", "mode": "NULLABLE"}',
        ],
        "bq_standard_data_type" : [
            "DATETIME",
            "INT64",
            "FLOAT64",
            "FLOAT64",
            "STRING",
            "STRING",
            "STRING",
            "STRING",
        ],
    },

    "datatype_postgres" :
    {
        "ddl" :
            """
            CREATE TABLE Sample_Table (
              Col_01 character varying(100) PRIMARY KEY,
              Col_02 json NOT NULL,
              Col_03 integer[], -- one dimensional array
              Col_04 integer[][], -- two dimensional array
              Col_05 integer[][][], -- multiple dimensional array
              Col_06 character varying[], -- character varying array
              Col_07 uuid NOT NULL
            );
            """,
        "database" : None,
        "table" : {"schema" : None, "name" : "Sample_Table", "temp" : False},
        "columns" : [
            {"name" : "Col_01", "type" : "CHARACTER VARYING", "length" : 100, "scale" : None, "array_dimensional" : 0, "not_null" : True, "pk" : True, "unique" : False, "constraint" : "PRIMARY KEY", "description" : None},
            {"name" : "Col_02", "type" : "JSON", "length" : None, "scale" : None, "array_dimensional" : 0, "not_null" : True, "pk" : False, "unique" : False, "constraint" : "NOT NULL", "description" : None},
            {"name" : "Col_03", "type" : "INTEGER", "length" : None, "scale" : None, "array_dimensional" : 1, "not_null" : False, "pk" : False, "unique" : False, "constraint" : "", "description" : None},
            {"name" : "Col_04", "type" : "INTEGER", "length" : None, "scale" : None, "array_dimensional" : 2, "not_null" : False, "pk" : False, "unique" : False, "constraint" : "", "description" : None},
            {"name" : "Col_05", "type" : "INTEGER", "length" : None, "scale" : None, "array_dimensional" : 3, "not_null" : False, "pk" : False, "unique" : False, "constraint" : "", "description" : None},
            {"name" : "Col_06", "type" : "CHARACTER VARYING", "length" : None, "scale" : None, "array_dimensional" : 1, "not_null" : False, "pk" : False, "unique" : False, "constraint" : "", "description" : None},
            {"name" : "Col_07", "type" : "UUID", "length" : None, "scale" : None, "array_dimensional" : 0, "not_null" : True, "pk" : False, "unique" : False, "constraint" : "NOT NULL", "description" : None},
        ],
        "bq_field" : [
            '{"name": "Col_01", "type": "STRING", "mode": "REQUIRED"}',
            '{"name": "Col_02", "type": "STRING", "mode": "REQUIRED"}',
            '{"name": "Col_03", "type": "INTEGER", "mode": "REPEATED"}',
            '{"name": "Col_04", "type": "RECORD", "mode": "REPEATED", "fields": [{"name": "dimension_1", "type": "INTEGER", "mode": "REPEATED"}]}',
            '{"name": "Col_05", "type": "RECORD", "mode": "REPEATED", "fields": [{"name": "dimension_1", "type": "RECORD", "mode": "REPEATED", "fields": [{"name": "dimension_2", "type": "INTEGER", "mode": "REPEATED"}]}]}',
            '{"name": "Col_06", "type": "STRING", "mode": "REPEATED"}',
            '{"name": "Col_07", "type": "STRING", "mode": "REQUIRED"}',
        ],
        "bq_standard_data_type" : [
            "STRING",
            "STRING",
            "INT64",
            "INT64",
            "INT64",
            "STRING",
            "STRING",
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
            {"name" : "Col_01", "type" : "VARCHAR", "length" : 100, "scale" : None, "array_dimensional" : 0, "not_null" : True, "pk" : True, "unique" : False, "constraint" : "PRIMARY KEY", "description" : None},
            {"name" : "Col_02", "type" : "CHAR", "length" : 200, "scale" : None, "array_dimensional" : 0, "not_null" : True, "pk" : True, "unique" : False, "constraint" : "PRIMARY KEY", "description" : None},
            {"name" : "Col_03", "type" : "INTEGER", "length" : None, "scale" : None, "array_dimensional" : 0, "not_null" : False, "pk" : False, "unique" : True, "constraint" : "UNIQUE", "description" : None},
            {"name" : "Col_04", "type" : "DOUBLE", "length" : None, "scale" : None, "array_dimensional" : 0, "not_null" : False, "pk" : False, "unique" : True, "constraint" : "UNIQUE", "description" : None},
            {"name" : "Col_05", "type" : "DATETIME", "length" : None, "scale" : None, "array_dimensional" : 0, "not_null" : False, "pk" : False, "unique" : False, "constraint" : "", "description" : None},
        ],
        "bq_field" : [
            '{"name": "Col_01", "type": "STRING", "mode": "REQUIRED"}',
            '{"name": "Col_02", "type": "STRING", "mode": "REQUIRED"}',
            '{"name": "Col_03", "type": "INTEGER", "mode": "NULLABLE"}',
            '{"name": "Col_04", "type": "FLOAT", "mode": "NULLABLE"}',
            '{"name": "Col_05", "type": "DATETIME", "mode": "NULLABLE"}',
        ],
        "bq_standard_data_type" : [
            "STRING",
            "STRING",
            "INT64",
            "FLOAT64",
            "DATETIME",
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
            {"name" : "Col_01", "type" : "VARCHAR", "length" : 100, "scale" : None, "array_dimensional" : 0, "not_null" : True, "pk" : True, "unique" : False, "constraint" : "PRIMARY KEY", "description" : None},
            {"name" : "Col_02", "type" : "CHAR", "length" : 200, "scale" : None, "array_dimensional" : 0, "not_null" : True, "pk" : True, "unique" : False, "constraint" : "PRIMARY KEY", "description" : None},
            {"name" : "Col_03", "type" : "INTEGER", "length" : None, "scale" : None, "array_dimensional" : 0, "not_null" : False, "pk" : False, "unique" : True, "constraint" : "UNIQUE", "description" : None},
            {"name" : "Col_04", "type" : "DOUBLE", "length" : None, "scale" : None, "array_dimensional" : 0, "not_null" : False, "pk" : False, "unique" : True, "constraint" : "UNIQUE", "description" : None},
            {"name" : "Col_05", "type" : "DATETIME", "length" : None, "scale" : None, "array_dimensional" : 0, "not_null" : False, "pk" : False, "unique" : False, "constraint" : "", "description" : None},
        ],
        "bq_field" : [
            '{"name": "Col_01", "type": "STRING", "mode": "REQUIRED"}',
            '{"name": "Col_02", "type": "STRING", "mode": "REQUIRED"}',
            '{"name": "Col_03", "type": "INTEGER", "mode": "NULLABLE"}',
            '{"name": "Col_04", "type": "FLOAT", "mode": "NULLABLE"}',
            '{"name": "Col_05", "type": "DATETIME", "mode": "NULLABLE"}',
        ],
        "bq_standard_data_type" : [
            "STRING",
            "STRING",
            "INT64",
            "FLOAT64",
            "DATETIME",
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
            {"name" : "Col_01", "type" : "VARCHAR", "length" : 100, "scale" : None, "array_dimensional" : 0, "not_null" : False, "pk" : False, "unique" : False, "constraint" : "", "description" : None},
        ],
        "bq_field" : [
            '{"name": "Col_01", "type": "STRING", "mode": "NULLABLE"}',
        ],
        "bq_standard_data_type" : [
            "STRING",
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
            {"name" : "Col_01", "type" : "VARCHAR", "length" : 100, "scale" : None, "array_dimensional" : 0, "not_null" : False, "pk" : False, "unique" : False, "constraint" : "", "description" : None},
        ],
        "bq_field" : [
            '{"name": "Col_01", "type": "STRING", "mode": "NULLABLE"}',
        ],
        "bq_standard_data_type" : [
            "STRING",
        ],
    },

    "column_comment": {
        "ddl" :
            """
            CREATE TABLE Sample_Table (
              Col_01 character varying(100) PRIMARY KEY COMMENT 'Single Quote',
              Col_02 json NOT NULL COMMENT "Double Quote",
              Col_03 integer[] COMMENT 'in "Quote"', -- one dimensional array
              Col_04 integer[][] COMMENT "in 'Quote'", -- two dimensional array
              Col_05 integer[][][] COMMENT 'コメント is full-width(Japanese) character', -- multiple dimensional array
              Col_06 character varying[] -- COMMENT 'Comment out' -- character varying array
            );
            """,
        "database" : None,
        "table" : {"schema" : None, "name" : "Sample_Table", "temp" : False},
        "columns" : [
            {"name" : "Col_01", "type" : "CHARACTER VARYING", "length" : 100, "scale" : None, "array_dimensional" : 0, "not_null" : True, "pk" : True, "unique" : False, "constraint" : "PRIMARY KEY", "description" : "Single Quote"},
            {"name" : "Col_02", "type" : "JSON", "length" : None, "scale" : None, "array_dimensional" : 0, "not_null" : True, "pk" : False, "unique" : False, "constraint" : "NOT NULL", "description" : "Double Quote"},
            {"name" : "Col_03", "type" : "INTEGER", "length" : None, "scale" : None, "array_dimensional" : 1, "not_null" : False, "pk" : False, "unique" : False, "constraint" : "", "description" : "in \"Quote\""},
            {"name" : "Col_04", "type" : "INTEGER", "length" : None, "scale" : None, "array_dimensional" : 2, "not_null" : False, "pk" : False, "unique" : False, "constraint" : "", "description" : "in 'Quote'"},
            {"name" : "Col_05", "type" : "INTEGER", "length" : None, "scale" : None, "array_dimensional" : 3, "not_null" : False, "pk" : False, "unique" : False, "constraint" : "", "description" : "コメント is full-width(Japanese) character"},
            {"name" : "Col_06", "type" : "CHARACTER VARYING", "length" : None, "scale" : None, "array_dimensional" : 1, "not_null" : False, "pk" : False, "unique" : False, "constraint" : "", "description" : None},
        ],
        "bq_field" : [
            '{"name": "Col_01", "type": "STRING", "mode": "REQUIRED", "description": "Single Quote"}',
            '{"name": "Col_02", "type": "STRING", "mode": "REQUIRED", "description": "Double Quote"}',
            '{"name": "Col_03", "type": "INTEGER", "mode": "REPEATED", "description": "in \\"Quote\\""}',
            '{"name": "Col_04", "type": "RECORD", "mode": "REPEATED", "description": "in \'Quote\'", "fields": [{"name": "dimension_1", "type": "INTEGER", "mode": "REPEATED"}]}',
            '{"name": "Col_05", "type": "RECORD", "mode": "REPEATED", "description": "コメント is full-width(Japanese) character", "fields": [{"name": "dimension_1", "type": "RECORD", "mode": "REPEATED", "fields": [{"name": "dimension_2", "type": "INTEGER", "mode": "REPEATED"}]}]}',
            '{"name": "Col_06", "type": "STRING", "mode": "REPEATED"}',
        ],
        "bq_standard_data_type" : [
            "STRING",
            "STRING",
            "INT64",
            "INT64",
            "INT64",
            "STRING",
        ],
    },
}


TEST_DATA_DDL = {
    "exist_schema_name": {
        "source_ddl":
            """
            CREATE TABLE Test_Schema.Test_Table (
              Col_01 varchar(100) PRIMARY KEY,
              Col_02 char(200) NOT NULL UNIQUE,
              Col_03 integer UNIQUE,
              Col_04 double,
              Col_05 datetime NOT NULL,
              Col_06 bool
            );
            """,
        "bq_ddl": {
            DdlParse.NAME_CASE.original:
                """\
                #standardSQL
                CREATE TABLE `project.Test_Schema.Test_Table`
                (
                  Col_01 STRING NOT NULL,
                  Col_02 STRING NOT NULL,
                  Col_03 INT64,
                  Col_04 FLOAT64,
                  Col_05 DATETIME NOT NULL,
                  Col_06 BOOL
                )""",
            DdlParse.NAME_CASE.lower:
                """\
                #standardSQL
                CREATE TABLE `project.test_schema.test_table`
                (
                  col_01 STRING NOT NULL,
                  col_02 STRING NOT NULL,
                  col_03 INT64,
                  col_04 FLOAT64,
                  col_05 DATETIME NOT NULL,
                  col_06 BOOL
                )""",
            DdlParse.NAME_CASE.upper:
                """\
                #standardSQL
                CREATE TABLE `project.TEST_SCHEMA.TEST_TABLE`
                (
                  COL_01 STRING NOT NULL,
                  COL_02 STRING NOT NULL,
                  COL_03 INT64,
                  COL_04 FLOAT64,
                  COL_05 DATETIME NOT NULL,
                  COL_06 BOOL
                )""",
        },
    },
    "no_schema_name": {
        "source_ddl":
            """
            CREATE TABLE Test_Table (
              Col_01 varchar(100) PRIMARY KEY,
              Col_02 char(200) NOT NULL UNIQUE,
              Col_03 integer UNIQUE,
              Col_04 double,
              Col_05 datetime NOT NULL,
              Col_06 bool
            );
            """,
        "bq_ddl": {
            DdlParse.NAME_CASE.original:
                """\
                #standardSQL
                CREATE TABLE `project.dataset.Test_Table`
                (
                  Col_01 STRING NOT NULL,
                  Col_02 STRING NOT NULL,
                  Col_03 INT64,
                  Col_04 FLOAT64,
                  Col_05 DATETIME NOT NULL,
                  Col_06 BOOL
                )""",
            DdlParse.NAME_CASE.lower:
                """\
                #standardSQL
                CREATE TABLE `project.dataset.test_table`
                (
                  col_01 STRING NOT NULL,
                  col_02 STRING NOT NULL,
                  col_03 INT64,
                  col_04 FLOAT64,
                  col_05 DATETIME NOT NULL,
                  col_06 BOOL
                )""",
            DdlParse.NAME_CASE.upper:
                """\
                #standardSQL
                CREATE TABLE `project.dataset.TEST_TABLE`
                (
                  COL_01 STRING NOT NULL,
                  COL_02 STRING NOT NULL,
                  COL_03 INT64,
                  COL_04 FLOAT64,
                  COL_05 DATETIME NOT NULL,
                  COL_06 BOOL
                )""",
        },
    },
    "datatype_postgres": {
        "source_ddl":
            """
            CREATE TABLE Test_Table (
              Col_01 character varying(100) PRIMARY KEY,
              Col_02 json NOT NULL,
              Col_03 integer[], -- one dimensional array
              Col_04 integer[][], -- two dimensional array
              Col_05 integer[][][], -- multiple dimensional array
              Col_06 character varying[] -- character varying array
            );
            """,
        "bq_ddl": {
            DdlParse.NAME_CASE.original:
                """\
                #standardSQL
                CREATE TABLE `project.dataset.Test_Table`
                (
                  Col_01 STRING NOT NULL,
                  Col_02 STRING NOT NULL,
                  Col_03 ARRAY<INT64>,
                  Col_04 ARRAY<STRUCT<dimension_1 ARRAY<INT64>>>,
                  Col_05 ARRAY<STRUCT<dimension_1 ARRAY<STRUCT<dimension_2 ARRAY<INT64>>>>>,
                  Col_06 ARRAY<STRING>
                )""",
            DdlParse.NAME_CASE.lower:
                """\
                #standardSQL
                CREATE TABLE `project.dataset.test_table`
                (
                  col_01 STRING NOT NULL,
                  col_02 STRING NOT NULL,
                  col_03 ARRAY<INT64>,
                  col_04 ARRAY<STRUCT<dimension_1 ARRAY<INT64>>>,
                  col_05 ARRAY<STRUCT<dimension_1 ARRAY<STRUCT<dimension_2 ARRAY<INT64>>>>>,
                  col_06 ARRAY<STRING>
                )""",
            DdlParse.NAME_CASE.upper:
                """\
                #standardSQL
                CREATE TABLE `project.dataset.TEST_TABLE`
                (
                  COL_01 STRING NOT NULL,
                  COL_02 STRING NOT NULL,
                  COL_03 ARRAY<INT64>,
                  COL_04 ARRAY<STRUCT<dimension_1 ARRAY<INT64>>>,
                  COL_05 ARRAY<STRUCT<dimension_1 ARRAY<STRUCT<dimension_2 ARRAY<INT64>>>>>,
                  COL_06 ARRAY<STRING>
                )""",
        },
    },
    "column_comment": {
        "source_ddl":
        """
            CREATE TABLE Test_Table (
              Col_01 character varying(100) PRIMARY KEY COMMENT 'Single Quote',
              Col_02 json NOT NULL COMMENT "Double Quote",
              Col_03 integer[] COMMENT 'in "Quote"', -- one dimensional array
              Col_04 integer[][] COMMENT "in 'Quote'", -- two dimensional array
              Col_05 integer[][][] COMMENT 'コメント is full-width(Japanese) character', -- multiple dimensional array
              Col_06 character varying[] -- COMMENT 'Comment out' -- character varying array
            );
            """,
        "bq_ddl": {
            DdlParse.NAME_CASE.original:
                """\
                #standardSQL
                CREATE TABLE `project.dataset.Test_Table`
                (
                  Col_01 STRING NOT NULL OPTIONS (description = "Single Quote"),
                  Col_02 STRING NOT NULL OPTIONS (description = "Double Quote"),
                  Col_03 ARRAY<INT64> OPTIONS (description = "in \\"Quote\\""),
                  Col_04 ARRAY<STRUCT<dimension_1 ARRAY<INT64>>> OPTIONS (description = "in 'Quote'"),
                  Col_05 ARRAY<STRUCT<dimension_1 ARRAY<STRUCT<dimension_2 ARRAY<INT64>>>>> OPTIONS (description = "コメント is full-width(Japanese) character"),
                  Col_06 ARRAY<STRING>
                )""",
            DdlParse.NAME_CASE.lower:
                """\
                #standardSQL
                CREATE TABLE `project.dataset.test_table`
                (
                  col_01 STRING NOT NULL OPTIONS (description = "Single Quote"),
                  col_02 STRING NOT NULL OPTIONS (description = "Double Quote"),
                  col_03 ARRAY<INT64> OPTIONS (description = "in \\"Quote\\""),
                  col_04 ARRAY<STRUCT<dimension_1 ARRAY<INT64>>> OPTIONS (description = "in 'Quote'"),
                  col_05 ARRAY<STRUCT<dimension_1 ARRAY<STRUCT<dimension_2 ARRAY<INT64>>>>> OPTIONS (description = "コメント is full-width(Japanese) character"),
                  col_06 ARRAY<STRING>
                )""",
            DdlParse.NAME_CASE.upper:
                """\
                #standardSQL
                CREATE TABLE `project.dataset.TEST_TABLE`
                (
                  COL_01 STRING NOT NULL OPTIONS (description = "Single Quote"),
                  COL_02 STRING NOT NULL OPTIONS (description = "Double Quote"),
                  COL_03 ARRAY<INT64> OPTIONS (description = "in \\"Quote\\""),
                  COL_04 ARRAY<STRUCT<dimension_1 ARRAY<INT64>>> OPTIONS (description = "in 'Quote'"),
                  COL_05 ARRAY<STRUCT<dimension_1 ARRAY<STRUCT<dimension_2 ARRAY<INT64>>>>> OPTIONS (description = "コメント is full-width(Japanese) character"),
                  COL_06 ARRAY<STRING>
                )""",
        },
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
    ("datatype_postgres", DDL_SET_PATTERN.method),
    ("name_backquote", DDL_SET_PATTERN.method),
    ("name_doublequote", DDL_SET_PATTERN.method),
    ("temp_table", DDL_SET_PATTERN.method),
    ("table_with_schema", DDL_SET_PATTERN.method),
    ("column_comment", DDL_SET_PATTERN.method),
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
        assert col.array_dimensional == data_col["array_dimensional"]
        assert col.not_null == data_col["not_null"]
        assert col.primary_key == data_col["pk"]
        assert col.unique == data_col["unique"]
        assert col.constraint == data_col["constraint"]
        assert col.source_database == data["database"]
        assert col.comment == data_col["description"] if data_col["description"] is not None else col.comment is None
        assert col.description == data_col["description"] if data_col["description"] is not None else col.description is None

        data_bq_field = data["bq_field"][i]
        assert col.to_bigquery_field() == data_bq_field

        data_bq_field_lower.append(re.sub(r'({"name": ")Col', r'\1col', data_bq_field))
        assert col.to_bigquery_field(col.NAME_CASE.lower) == data_bq_field_lower[-1]

        data_bq_field_upper.append(re.sub(r'({"name": ")Col', r'\1COL', data_bq_field))
        assert col.to_bigquery_field(col.NAME_CASE.upper) == data_bq_field_upper[-1]

        assert col.bigquery_legacy_data_type == col.bigquery_data_type
        assert col.bigquery_standard_data_type == data["bq_standard_data_type"][i]

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


@pytest.mark.parametrize(("test_case"), [
    ("exist_schema_name"),
    ("no_schema_name"),
    ("datatype_postgres"),
    ("column_comment"),
])
def test_bq_ddl(test_case):
    # Get test data
    data = TEST_DATA_DDL[test_case]

    # Parse ddl
    table = DdlParse().parse(data["source_ddl"])

    # Check generate BigQuery DDL statements of DdlParseTable
    assert table.to_bigquery_ddl() == textwrap.dedent(data["bq_ddl"][DdlParse.NAME_CASE.original])
    assert table.to_bigquery_ddl(DdlParse.NAME_CASE.original) == textwrap.dedent(data["bq_ddl"][DdlParse.NAME_CASE.original])
    assert table.to_bigquery_ddl(DdlParse.NAME_CASE.lower) == textwrap.dedent(data["bq_ddl"][DdlParse.NAME_CASE.lower])
    assert table.to_bigquery_ddl(DdlParse.NAME_CASE.upper) == textwrap.dedent(data["bq_ddl"][DdlParse.NAME_CASE.upper])


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


def test_set_comment():
    ddl = """
        CREATE TABLE Sample_Table (
          Col_01 integer
        );
        """

    # Parse DDL
    table = DdlParse().parse(ddl)

    # Check blank comment
    assert table.columns["Col_01"].comment is None
    assert table.columns["Col_01"].description is None

    # Check comment setter
    table.columns["Col_01"].comment = "comment_1"
    assert table.columns["Col_01"].comment == "comment_1"

    # Check description setter
    table.columns["Col_01"].description = "comment_2"
    assert table.columns["Col_01"].description == "comment_2"

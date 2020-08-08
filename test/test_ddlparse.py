# -*- coding: utf-8 -*-

import pytest, re, textwrap
from enum import IntEnum

from ddlparse.ddlparse import DdlParse


TEST_DATA = {
    "basic":
    {
        "ddl":
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
              Col_21 numeric(18),
              Col_22 number(18),
              Col_23 decimal(18),
              Col_24 numeric(18, 0),
              Col_25 number(18, 0),
              Col_26 decimal(18, 0),
              Col_27 numeric(18, 1),
              Col_28 number(18, 2),
              Col_29 decimal(18, 3),
              Col_30 numeric(19),
              Col_31 number(19),
              Col_32 decimal(19),
              Col_33 numeric(19, 0),
              Col_34 number(19, 0),
              Col_35 decimal(19, 0),
              Col_36 numeric(19, 1),
              Col_37 number(19, 2),
              Col_38 decimal(19, 3),
              Col_39 numeric(*),
              Col_40 number(*),
              Col_41 decimal(*),
              Col_42 numeric(*, 0),
              Col_43 number(*, 0),
              Col_44 decimal(*, 0),
              Col_45 numeric(*, 1), -- comment
              Col_46 number(*, 2),  -- comment
              Col_47 decimal(*, 3),  -- comment
              Col_48 numeric,
              Col_49 number,
              Col_50 decimal
            );
            """,
        "database": None,
        "table": {"schema": None, "name": "Sample_Table", "temp": False},
        "columns": [
            {"name": "Col_01", "type": "VARCHAR", "length": 100, "scale": None, "array_dimensional": 0, "is_unsigned": False, "is_zerofill": False, "not_null": True, "pk": True, "unique": False, "constraint": "PRIMARY KEY", "description": None},
            {"name": "Col_02", "type": "CHAR", "length": 200, "scale": None, "array_dimensional": 0, "is_unsigned": False, "is_zerofill": False, "not_null": True, "pk": False, "unique": True, "constraint": "NOT NULL UNIQUE", "description": None},
            {"name": "Col_03", "type": "TEXT", "length": None, "scale": None, "array_dimensional": 0, "is_unsigned": False, "is_zerofill": False, "not_null": False, "pk": False, "unique": True, "constraint": "UNIQUE", "description": None},
            {"name": "Col_04", "type": "INTEGER", "length": None, "scale": None, "array_dimensional": 0, "is_unsigned": False, "is_zerofill": False, "not_null": False, "pk": False, "unique": False, "constraint": "", "description": None},
            {"name": "Col_05", "type": "BIGINT", "length": None, "scale": None, "array_dimensional": 0, "is_unsigned": False, "is_zerofill": False, "not_null": False, "pk": False, "unique": False, "constraint": "", "description": None},
            {"name": "Col_06", "type": "SERIAL", "length": None, "scale": None, "array_dimensional": 0, "is_unsigned": False, "is_zerofill": False, "not_null": False, "pk": False, "unique": False, "constraint": "", "description": None},
            {"name": "Col_07", "type": "YEAR", "length": None, "scale": None, "array_dimensional": 0, "is_unsigned": False, "is_zerofill": False, "not_null": False, "pk": False, "unique": False, "constraint": "", "description": None},
            {"name": "Col_08", "type": "FLOAT", "length": None, "scale": None, "array_dimensional": 0, "is_unsigned": False, "is_zerofill": False, "not_null": False, "pk": False, "unique": False, "constraint": "", "description": None},
            {"name": "Col_09", "type": "DOUBLE", "length": None, "scale": None, "array_dimensional": 0, "is_unsigned": False, "is_zerofill": False, "not_null": False, "pk": False, "unique": False, "constraint": "", "description": None},
            {"name": "Col_10", "type": "DOUBLE PRECISION", "length": None, "scale": None, "array_dimensional": 0, "is_unsigned": False, "is_zerofill": False, "not_null": False, "pk": False, "unique": False, "constraint": "", "description": None},
            {"name": "Col_11", "type": "REAL", "length": None, "scale": None, "array_dimensional": 0, "is_unsigned": False, "is_zerofill": False, "not_null": False, "pk": False, "unique": False, "constraint": "", "description": None},
            {"name": "Col_12", "type": "MONEY", "length": None, "scale": None, "array_dimensional": 0, "is_unsigned": False, "is_zerofill": False, "not_null": False, "pk": False, "unique": False, "constraint": "", "description": None},
            {"name": "Col_13", "type": "DATE", "length": None, "scale": None, "array_dimensional": 0, "is_unsigned": False, "is_zerofill": False, "not_null": False, "pk": False, "unique": False, "constraint": "", "description": None},
            {"name": "Col_14", "type": "TIME", "length": None, "scale": None, "array_dimensional": 0, "is_unsigned": False, "is_zerofill": False, "not_null": False, "pk": False, "unique": False, "constraint": "", "description": None},
            {"name": "Col_15", "type": "DATETIME", "length": None, "scale": None, "array_dimensional": 0, "is_unsigned": False, "is_zerofill": False, "not_null": False, "pk": False, "unique": False, "constraint": "", "description": None},
            {"name": "Col_16", "type": "TIMESTAMP", "length": None, "scale": None, "array_dimensional": 0, "is_unsigned": False, "is_zerofill": False, "not_null": False, "pk": False, "unique": False, "constraint": "", "description": None},
            {"name": "Col_17", "type": "TIMESTAMP WITHOUT TIME ZONE", "length": None, "scale": None, "array_dimensional": 0, "is_unsigned": False, "is_zerofill": False, "not_null": False, "pk": False, "unique": False, "constraint": "", "description": None},
            {"name": "Col_18", "type": "TIMESTAMPTZ", "length": None, "scale": None, "array_dimensional": 0, "is_unsigned": False, "is_zerofill": False, "not_null": False, "pk": False, "unique": False, "constraint": "", "description": None},
            {"name": "Col_19", "type": "TIMESTAMP WITH TIME ZONE", "length": None, "scale": None, "array_dimensional": 0, "is_unsigned": False, "is_zerofill": False, "not_null": False, "pk": False, "unique": False, "constraint": "", "description": None},
            {"name": "Col_20", "type": "BOOL", "length": None, "scale": None, "array_dimensional": 0, "is_unsigned": False, "is_zerofill": False, "not_null": False, "pk": False, "unique": False, "constraint": "", "description": None},
            {"name": "Col_21", "type": "NUMERIC", "length": 18, "scale": None, "array_dimensional": 0, "is_unsigned": False, "is_zerofill": False, "not_null": False, "pk": False, "unique": False, "constraint": "", "description": None},
            {"name": "Col_22", "type": "NUMBER", "length": 18, "scale": None, "array_dimensional": 0, "is_unsigned": False, "is_zerofill": False, "not_null": False, "pk": False, "unique": False, "constraint": "", "description": None},
            {"name": "Col_23", "type": "DECIMAL", "length": 18, "scale": None, "array_dimensional": 0, "is_unsigned": False, "is_zerofill": False, "not_null": False, "pk": False, "unique": False, "constraint": "", "description": None},
            {"name": "Col_24", "type": "NUMERIC", "length": 18, "scale": None, "array_dimensional": 0, "is_unsigned": False, "is_zerofill": False, "not_null": False, "pk": False, "unique": False, "constraint": "", "description": None},
            {"name": "Col_25", "type": "NUMBER", "length": 18, "scale": None, "array_dimensional": 0, "is_unsigned": False, "is_zerofill": False, "not_null": False, "pk": False, "unique": False, "constraint": "", "description": None},
            {"name": "Col_26", "type": "DECIMAL", "length": 18, "scale": None, "array_dimensional": 0, "is_unsigned": False, "is_zerofill": False, "not_null": False, "pk": False, "unique": False, "constraint": "", "description": None},
            {"name": "Col_27", "type": "NUMERIC", "length": 18, "scale": 1, "array_dimensional": 0, "is_unsigned": False, "is_zerofill": False, "not_null": False, "pk": False, "unique": False, "constraint": "", "description": None},
            {"name": "Col_28", "type": "NUMBER", "length": 18, "scale": 2, "array_dimensional": 0, "is_unsigned": False, "is_zerofill": False, "not_null": False, "pk": False, "unique": False, "constraint": "", "description": None},
            {"name": "Col_29", "type": "DECIMAL", "length": 18, "scale": 3, "array_dimensional": 0, "is_unsigned": False, "is_zerofill": False, "not_null": False, "pk": False, "unique": False, "constraint": "", "description": None},
            {"name": "Col_30", "type": "NUMERIC", "length": 19, "scale": None, "array_dimensional": 0, "is_unsigned": False, "is_zerofill": False, "not_null": False, "pk": False, "unique": False, "constraint": "", "description": None},
            {"name": "Col_31", "type": "NUMBER", "length": 19, "scale": None, "array_dimensional": 0, "is_unsigned": False, "is_zerofill": False, "not_null": False, "pk": False, "unique": False, "constraint": "", "description": None},
            {"name": "Col_32", "type": "DECIMAL", "length": 19, "scale": None, "array_dimensional": 0, "is_unsigned": False, "is_zerofill": False, "not_null": False, "pk": False, "unique": False, "constraint": "", "description": None},
            {"name": "Col_33", "type": "NUMERIC", "length": 19, "scale": None, "array_dimensional": 0, "is_unsigned": False, "is_zerofill": False, "not_null": False, "pk": False, "unique": False, "constraint": "", "description": None},
            {"name": "Col_34", "type": "NUMBER", "length": 19, "scale": None, "array_dimensional": 0, "is_unsigned": False, "is_zerofill": False, "not_null": False, "pk": False, "unique": False, "constraint": "", "description": None},
            {"name": "Col_35", "type": "DECIMAL", "length": 19, "scale": None, "array_dimensional": 0, "is_unsigned": False, "is_zerofill": False, "not_null": False, "pk": False, "unique": False, "constraint": "", "description": None},
            {"name": "Col_36", "type": "NUMERIC", "length": 19, "scale": 1, "array_dimensional": 0, "is_unsigned": False, "is_zerofill": False, "not_null": False, "pk": False, "unique": False, "constraint": "", "description": None},
            {"name": "Col_37", "type": "NUMBER", "length": 19, "scale": 2, "array_dimensional": 0, "is_unsigned": False, "is_zerofill": False, "not_null": False, "pk": False, "unique": False, "constraint": "", "description": None},
            {"name": "Col_38", "type": "DECIMAL", "length": 19, "scale": 3, "array_dimensional": 0, "is_unsigned": False, "is_zerofill": False, "not_null": False, "pk": False, "unique": False, "constraint": "", "description": None},
            {"name": "Col_39", "type": "NUMERIC", "length": "*", "scale": None, "array_dimensional": 0, "is_unsigned": False, "is_zerofill": False, "not_null": False, "pk": False, "unique": False, "constraint": "", "description": None},
            {"name": "Col_40", "type": "NUMBER", "length": "*", "scale": None, "array_dimensional": 0, "is_unsigned": False, "is_zerofill": False, "not_null": False, "pk": False, "unique": False, "constraint": "", "description": None},
            {"name": "Col_41", "type": "DECIMAL", "length": "*", "scale": None, "array_dimensional": 0, "is_unsigned": False, "is_zerofill": False, "not_null": False, "pk": False, "unique": False, "constraint": "", "description": None},
            {"name": "Col_42", "type": "NUMERIC", "length": "*", "scale": None, "array_dimensional": 0, "is_unsigned": False, "is_zerofill": False, "not_null": False, "pk": False, "unique": False, "constraint": "", "description": None},
            {"name": "Col_43", "type": "NUMBER", "length": "*", "scale": None, "array_dimensional": 0, "is_unsigned": False, "is_zerofill": False, "not_null": False, "pk": False, "unique": False, "constraint": "", "description": None},
            {"name": "Col_44", "type": "DECIMAL", "length": "*", "scale": None, "array_dimensional": 0, "is_unsigned": False, "is_zerofill": False, "not_null": False, "pk": False, "unique": False, "constraint": "", "description": None},
            {"name": "Col_45", "type": "NUMERIC", "length": "*", "scale": 1, "array_dimensional": 0, "is_unsigned": False, "is_zerofill": False, "not_null": False, "pk": False, "unique": False, "constraint": "", "description": None},
            {"name": "Col_46", "type": "NUMBER", "length": "*", "scale": 2, "array_dimensional": 0, "is_unsigned": False, "is_zerofill": False, "not_null": False, "pk": False, "unique": False, "constraint": "", "description": None},
            {"name": "Col_47", "type": "DECIMAL", "length": "*", "scale": 3, "array_dimensional": 0, "is_unsigned": False, "is_zerofill": False, "not_null": False, "pk": False, "unique": False, "constraint": "", "description": None},
            {"name": "Col_48", "type": "NUMERIC", "length": None, "scale": None, "array_dimensional": 0, "is_unsigned": False, "is_zerofill": False, "not_null": False, "pk": False, "unique": False, "constraint": "", "description": None},
            {"name": "Col_49", "type": "NUMBER", "length": None, "scale": None, "array_dimensional": 0, "is_unsigned": False, "is_zerofill": False, "not_null": False, "pk": False, "unique": False, "constraint": "", "description": None},
            {"name": "Col_50", "type": "DECIMAL", "length": None, "scale": None, "array_dimensional": 0, "is_unsigned": False, "is_zerofill": False, "not_null": False, "pk": False, "unique": False, "constraint": "", "description": None},
        ],
        "bq_field": [
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
            '{"name": "Col_30", "type": "NUMERIC", "mode": "NULLABLE"}',
            '{"name": "Col_31", "type": "NUMERIC", "mode": "NULLABLE"}',
            '{"name": "Col_32", "type": "NUMERIC", "mode": "NULLABLE"}',
            '{"name": "Col_33", "type": "NUMERIC", "mode": "NULLABLE"}',
            '{"name": "Col_34", "type": "NUMERIC", "mode": "NULLABLE"}',
            '{"name": "Col_35", "type": "NUMERIC", "mode": "NULLABLE"}',
            '{"name": "Col_36", "type": "NUMERIC", "mode": "NULLABLE"}',
            '{"name": "Col_37", "type": "NUMERIC", "mode": "NULLABLE"}',
            '{"name": "Col_38", "type": "NUMERIC", "mode": "NULLABLE"}',
            '{"name": "Col_39", "type": "NUMERIC", "mode": "NULLABLE"}',
            '{"name": "Col_40", "type": "NUMERIC", "mode": "NULLABLE"}',
            '{"name": "Col_41", "type": "NUMERIC", "mode": "NULLABLE"}',
            '{"name": "Col_42", "type": "NUMERIC", "mode": "NULLABLE"}',
            '{"name": "Col_43", "type": "NUMERIC", "mode": "NULLABLE"}',
            '{"name": "Col_44", "type": "NUMERIC", "mode": "NULLABLE"}',
            '{"name": "Col_45", "type": "NUMERIC", "mode": "NULLABLE"}',
            '{"name": "Col_46", "type": "NUMERIC", "mode": "NULLABLE"}',
            '{"name": "Col_47", "type": "NUMERIC", "mode": "NULLABLE"}',
            '{"name": "Col_48", "type": "INTEGER", "mode": "NULLABLE"}',
            '{"name": "Col_49", "type": "INTEGER", "mode": "NULLABLE"}',
            '{"name": "Col_50", "type": "INTEGER", "mode": "NULLABLE"}',
        ],
        "bq_standard_data_type": [
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
            "NUMERIC",
            "NUMERIC",
            "NUMERIC",
            "NUMERIC",
            "NUMERIC",
            "NUMERIC",
            "NUMERIC",
            "NUMERIC",
            "NUMERIC",
            "NUMERIC",
            "NUMERIC",
            "NUMERIC",
            "NUMERIC",
            "NUMERIC",
            "NUMERIC",
            "NUMERIC",
            "NUMERIC",
            "NUMERIC",
            "INT64",
            "INT64",
            "INT64",
        ],
    },

    "constraint_mysql":
    {
        "ddl":
            """
            CREATE TABLE Sample_Table (
              Col_01 varchar(100),
              Col_02 char(200),
              Col_03 integer,
              Col_04 double,
              Col_05 datetime,
              Col_06 decimal(2,1) not null default '0.0',
              Col_07 integer,
              CONSTRAINT const_01 PRIMARY KEY (Col_01, Col_02),
              CONSTRAINT \"const_02\" UNIQUE (Col_03, Col_04),
              CONSTRAINT \"const_03\" FOREIGN KEY (Col_04, \"Col_05\") REFERENCES ref_table_01 (\"Col_04\", Col_05)
            );
            """,
        "database": DdlParse.DATABASE.mysql,
        "table": {"schema": None, "name": "Sample_Table", "temp": False},
        "columns": [
            {"name": "Col_01", "type": "VARCHAR", "length": 100, "scale": None, "array_dimensional": 0, "is_unsigned": False, "is_zerofill": False, "not_null": True, "pk": True, "unique": False, "constraint": "PRIMARY KEY", "description": None},
            {"name": "Col_02", "type": "CHAR", "length": 200, "scale": None, "array_dimensional": 0, "is_unsigned": False, "is_zerofill": False, "not_null": True, "pk": True, "unique": False, "constraint": "PRIMARY KEY", "description": None},
            {"name": "Col_03", "type": "INTEGER", "length": None, "scale": None, "array_dimensional": 0, "is_unsigned": False, "is_zerofill": False, "not_null": False, "pk": False, "unique": True, "constraint": "UNIQUE", "description": None},
            {"name": "Col_04", "type": "DOUBLE", "length": None, "scale": None, "array_dimensional": 0, "is_unsigned": False, "is_zerofill": False, "not_null": False, "pk": False, "unique": True, "constraint": "UNIQUE", "description": None},
            {"name": "Col_05", "type": "DATETIME", "length": None, "scale": None, "array_dimensional": 0, "is_unsigned": False, "is_zerofill": False, "not_null": False, "pk": False, "unique": False, "constraint": "", "description": None},
            {"name": "Col_06", "type": "DECIMAL", "length": 2, "scale": 1, "array_dimensional": 0, "is_unsigned": False, "is_zerofill": False, "not_null": True, "pk": False, "unique": False, "constraint": "NOT NULL", "description": None},
            {"name": "Col_07", "type": "INTEGER", "length": None, "scale": None, "array_dimensional": 0, "is_unsigned": False, "is_zerofill": False, "not_null": False, "pk": False, "unique": False, "constraint": "", "description": None},
        ],
        "bq_field": [
            '{"name": "Col_01", "type": "STRING", "mode": "REQUIRED"}',
            '{"name": "Col_02", "type": "STRING", "mode": "REQUIRED"}',
            '{"name": "Col_03", "type": "INTEGER", "mode": "NULLABLE"}',
            '{"name": "Col_04", "type": "FLOAT", "mode": "NULLABLE"}',
            '{"name": "Col_05", "type": "DATETIME", "mode": "NULLABLE"}',
            '{"name": "Col_06", "type": "FLOAT", "mode": "REQUIRED"}',
            '{"name": "Col_07", "type": "INTEGER", "mode": "NULLABLE"}',
        ],
        "bq_standard_data_type": [
            "STRING",
            "STRING",
            "INT64",
            "FLOAT64",
            "DATETIME",
            "FLOAT64",
            "INT64",
        ],
    },


    "constraint_postgres_oracle_redshift":
    {
        "ddl":
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
        "database": None,
        "table": {"schema": None, "name": "Sample_Table", "temp": False},
        "columns": [
            {"name": "Col_01", "type": "VARCHAR", "length": 100, "scale": None, "array_dimensional": 0, "is_unsigned": False, "is_zerofill": False, "not_null": True, "pk": True, "unique": False, "constraint": "PRIMARY KEY", "description": None},
            {"name": "Col_02", "type": "CHAR", "length": 200, "scale": None, "array_dimensional": 0, "is_unsigned": False, "is_zerofill": False, "not_null": True, "pk": True, "unique": False, "constraint": "PRIMARY KEY", "description": None},
            {"name": "Col_03", "type": "INTEGER", "length": None, "scale": None, "array_dimensional": 0, "is_unsigned": False, "is_zerofill": False, "not_null": False, "pk": False, "unique": True, "constraint": "UNIQUE", "description": None},
            {"name": "Col_04", "type": "DOUBLE", "length": None, "scale": None, "array_dimensional": 0, "is_unsigned": False, "is_zerofill": False, "not_null": True, "pk": False, "unique": True, "constraint": "NOT NULL UNIQUE", "description": None},
            {"name": "Col_05", "type": "DATETIME", "length": None, "scale": None, "array_dimensional": 0, "is_unsigned": False, "is_zerofill": False, "not_null": True, "pk": False, "unique": False, "constraint": "NOT NULL", "description": None},
        ],
        "bq_field": [
            '{"name": "Col_01", "type": "STRING", "mode": "REQUIRED"}',
            '{"name": "Col_02", "type": "STRING", "mode": "REQUIRED"}',
            '{"name": "Col_03", "type": "INTEGER", "mode": "NULLABLE"}',
            '{"name": "Col_04", "type": "FLOAT", "mode": "REQUIRED"}',
            '{"name": "Col_05", "type": "DATETIME", "mode": "REQUIRED"}',
        ],
        "bq_standard_data_type": [
            "STRING",
            "STRING",
            "INT64",
            "FLOAT64",
            "DATETIME",
        ],
    },

    "default_postgres_redshift":
    {
        "ddl":
            """
            CREATE TABLE Sample_Table (
              Col_01 char(1) DEFAULT '0'::bpchar PRIMARY KEY,
              Col_02 char(1) DEFAULT '0'::bpchar ENCODE lzo NOT NULL,
              Col_03 integer DEFAULT 0,
              Col_04 numeric(10) DEFAULT 0,
              Col_05 numeric(20,3) DEFAULT 0.0,
              Col_06 varchar(100) DEFAULT '!\"#$%&\\\'()*+,-./:;<=>?@[\\]^_`{|}~',
              Col_07 character varying[] DEFAULT '{}'::character varying[]
            );
            """,
        "database": None,
        "table": {"schema": None, "name": "Sample_Table", "temp": False},
        "columns": [
            {"name": "Col_01", "type": "CHAR", "length": 1, "scale": None, "array_dimensional": 0, "is_unsigned": False, "is_zerofill": False, "not_null": True, "pk": True, "unique": False, "constraint": "PRIMARY KEY", "description": None},
            {"name": "Col_02", "type": "CHAR", "length": 1, "scale": None, "array_dimensional": 0, "is_unsigned": False, "is_zerofill": False, "not_null": True, "pk": False, "unique": False, "constraint": "NOT NULL", "description": None},
            {"name": "Col_03", "type": "INTEGER", "length": None, "scale": None, "array_dimensional": 0, "is_unsigned": False, "is_zerofill": False, "not_null": False, "pk": False, "unique": False, "constraint": "", "description": None},
            {"name": "Col_04", "type": "NUMERIC", "length": 10, "scale": None, "array_dimensional": 0, "is_unsigned": False, "is_zerofill": False, "not_null": False, "pk": False, "unique": False, "constraint": "", "description": None},
            {"name": "Col_05", "type": "NUMERIC", "length": 20, "scale": 3, "array_dimensional": 0, "is_unsigned": False, "is_zerofill": False, "not_null": False, "pk": False, "unique": False, "constraint": "", "description": None},
            {"name": "Col_06", "type": "VARCHAR", "length": 100, "scale": None, "array_dimensional": 0, "is_unsigned": False, "is_zerofill": False, "not_null": False, "pk": False, "unique": False, "constraint": "", "description": None},
            {"name": "Col_07", "type": "CHARACTER VARYING", "length": None, "scale": None, "array_dimensional": 1, "is_unsigned": False, "is_zerofill": False, "not_null": False, "pk": False, "unique": False, "constraint": "", "description": None},
        ],
        "bq_field": [
            '{"name": "Col_01", "type": "STRING", "mode": "REQUIRED"}',
            '{"name": "Col_02", "type": "STRING", "mode": "REQUIRED"}',
            '{"name": "Col_03", "type": "INTEGER", "mode": "NULLABLE"}',
            '{"name": "Col_04", "type": "INTEGER", "mode": "NULLABLE"}',
            '{"name": "Col_05", "type": "NUMERIC", "mode": "NULLABLE"}',
            '{"name": "Col_06", "type": "STRING", "mode": "NULLABLE"}',
            '{"name": "Col_07", "type": "STRING", "mode": "REPEATED"}',
        ],
        "bq_standard_data_type": [
            "STRING",
            "STRING",
            "INT64",
            "INT64",
            "NUMERIC",
            "STRING",
            "STRING",
        ],
    },

    "datatype_oracle":
    {
        "ddl":
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
        "database": DdlParse.DATABASE.oracle,
        "table": {"schema": None, "name": "Sample_Table", "temp": False},
        "columns": [
            {"name": "Col_01", "type": "DATE", "length": None, "scale": None, "array_dimensional": 0, "is_unsigned": False, "is_zerofill": False, "not_null": False, "pk": False, "unique": False, "constraint": "", "description": None},
            {"name": "Col_02", "type": "NUMBER", "length": 1, "scale": None, "array_dimensional": 0, "is_unsigned": False, "is_zerofill": False, "not_null": False, "pk": False, "unique": False, "constraint": "", "description": None},
            {"name": "Col_03", "type": "NUMBER", "length": 1, "scale": 2, "array_dimensional": 0, "is_unsigned": False, "is_zerofill": False, "not_null": False, "pk": False, "unique": False, "constraint": "", "description": None},
            {"name": "Col_04", "type": "NUMBER", "length": None, "scale": None, "array_dimensional": 0, "is_unsigned": False, "is_zerofill": False, "not_null": False, "pk": False, "unique": False, "constraint": "", "description": None},
            {"name": "Col_05", "type": "VARCHAR2", "length": 3, "scale": None, "array_dimensional": 0, "is_unsigned": False, "is_zerofill": False, "not_null": False, "pk": False, "unique": False, "constraint": "", "description": None},
            {"name": "Col_06", "type": "VARCHAR2", "length": 4, "scale": None, "array_dimensional": 0, "is_unsigned": False, "is_zerofill": False, "not_null": False, "pk": False, "unique": False, "constraint": "", "description": None},
            {"name": "Col_07", "type": "CLOB", "length": None, "scale": None, "array_dimensional": 0, "is_unsigned": False, "is_zerofill": False, "not_null": False, "pk": False, "unique": False, "constraint": "", "description": None},
            {"name": "Col_08", "type": "NCLOB", "length": None, "scale": None, "array_dimensional": 0, "is_unsigned": False, "is_zerofill": False, "not_null": False, "pk": False, "unique": False, "constraint": "", "description": None},
        ],
        "bq_field": [
            '{"name": "Col_01", "type": "DATETIME", "mode": "NULLABLE"}',
            '{"name": "Col_02", "type": "INTEGER", "mode": "NULLABLE"}',
            '{"name": "Col_03", "type": "FLOAT", "mode": "NULLABLE"}',
            '{"name": "Col_04", "type": "NUMERIC", "mode": "NULLABLE"}',
            '{"name": "Col_05", "type": "STRING", "mode": "NULLABLE"}',
            '{"name": "Col_06", "type": "STRING", "mode": "NULLABLE"}',
            '{"name": "Col_07", "type": "STRING", "mode": "NULLABLE"}',
            '{"name": "Col_08", "type": "STRING", "mode": "NULLABLE"}',
        ],
        "bq_standard_data_type": [
            "DATETIME",
            "INT64",
            "FLOAT64",
            "NUMERIC",
            "STRING",
            "STRING",
            "STRING",
            "STRING",
        ],
    },

    "datatype_postgres":
    {
        "ddl":
            """
            CREATE TABLE Sample_Table (
              Col_01 character varying(100) PRIMARY KEY,
              Col_02 json NOT NULL,
              Col_03 integer[], -- one dimensional array
              Col_04 integer[][], -- two dimensional array
              Col_05 integer[][][], -- multiple dimensional array
              Col_06 character varying[], -- character varying array
              Col_07 uuid NOT NULL,
              Col_08 numeric,
              Col_09 decimal
            );
            """,
        "database": DdlParse.DATABASE.postgresql,
        "table": {"schema": None, "name": "Sample_Table", "temp": False},
        "columns": [
            {"name": "Col_01", "type": "CHARACTER VARYING", "length": 100, "scale": None, "array_dimensional": 0, "is_unsigned": False, "is_zerofill": False, "not_null": True, "pk": True, "unique": False, "constraint": "PRIMARY KEY", "description": None},
            {"name": "Col_02", "type": "JSON", "length": None, "scale": None, "array_dimensional": 0, "is_unsigned": False, "is_zerofill": False, "not_null": True, "pk": False, "unique": False, "constraint": "NOT NULL", "description": None},
            {"name": "Col_03", "type": "INTEGER", "length": None, "scale": None, "array_dimensional": 1, "is_unsigned": False, "is_zerofill": False, "not_null": False, "pk": False, "unique": False, "constraint": "", "description": None},
            {"name": "Col_04", "type": "INTEGER", "length": None, "scale": None, "array_dimensional": 2, "is_unsigned": False, "is_zerofill": False, "not_null": False, "pk": False, "unique": False, "constraint": "", "description": None},
            {"name": "Col_05", "type": "INTEGER", "length": None, "scale": None, "array_dimensional": 3, "is_unsigned": False, "is_zerofill": False, "not_null": False, "pk": False, "unique": False, "constraint": "", "description": None},
            {"name": "Col_06", "type": "CHARACTER VARYING", "length": None, "scale": None, "array_dimensional": 1, "is_unsigned": False, "is_zerofill": False, "not_null": False, "pk": False, "unique": False, "constraint": "", "description": None},
            {"name": "Col_07", "type": "UUID", "length": None, "scale": None, "array_dimensional": 0, "is_unsigned": False, "is_zerofill": False, "not_null": True, "pk": False, "unique": False, "constraint": "NOT NULL", "description": None},
            {"name": "Col_08", "type": "NUMERIC", "length": None, "scale": None, "array_dimensional": 0, "is_unsigned": False, "is_zerofill": False, "not_null": False, "pk": False, "unique": False, "constraint": "", "description": None},
            {"name": "Col_09", "type": "DECIMAL", "length": None, "scale": None, "array_dimensional": 0, "is_unsigned": False, "is_zerofill": False, "not_null": False, "pk": False, "unique": False, "constraint": "", "description": None},
        ],
        "bq_field": [
            '{"name": "Col_01", "type": "STRING", "mode": "REQUIRED"}',
            '{"name": "Col_02", "type": "STRING", "mode": "REQUIRED"}',
            '{"name": "Col_03", "type": "INTEGER", "mode": "REPEATED"}',
            '{"name": "Col_04", "type": "RECORD", "mode": "REPEATED", "fields": [{"name": "dimension_1", "type": "INTEGER", "mode": "REPEATED"}]}',
            '{"name": "Col_05", "type": "RECORD", "mode": "REPEATED", "fields": [{"name": "dimension_1", "type": "RECORD", "mode": "REPEATED", "fields": [{"name": "dimension_2", "type": "INTEGER", "mode": "REPEATED"}]}]}',
            '{"name": "Col_06", "type": "STRING", "mode": "REPEATED"}',
            '{"name": "Col_07", "type": "STRING", "mode": "REQUIRED"}',
            '{"name": "Col_08", "type": "NUMERIC", "mode": "NULLABLE"}',
            '{"name": "Col_09", "type": "NUMERIC", "mode": "NULLABLE"}',
        ],
        "bq_standard_data_type": [
            "STRING",
            "STRING",
            "INT64",
            "INT64",
            "INT64",
            "STRING",
            "STRING",
            "NUMERIC",
            "NUMERIC",
        ],
    },

    "datatype_mysql":
    {
        "ddl":
            """
            CREATE TABLE Sample_Table (
              Col_101 TINYINT(11) UNSIGNED ZEROFILL,
              Col_102 SMALLINT(12) UNSIGNED ZEROFILL,
              Col_103 MEDIUMINT(13) UNSIGNED ZEROFILL,
              Col_104 INT(14) UNSIGNED ZEROFILL,
              Col_105 INTEGER(15) UNSIGNED ZEROFILL,
              Col_106 BIGINT(16) UNSIGNED ZEROFILL,
              Col_107 DECIMAL(17, 1) UNSIGNED ZEROFILL,
              Col_108 DEC(18, 2) UNSIGNED ZEROFILL,
              Col_109 NUMERIC(19, 3) UNSIGNED ZEROFILL,
              Col_110 FIXED(20, 4) UNSIGNED ZEROFILL,
              Col_111 FLOAT(21, 5) UNSIGNED ZEROFILL,
              Col_112 DOUBLE(22, 6) UNSIGNED ZEROFILL,
              Col_113 DOUBLE PRECISION(23, 7) UNSIGNED ZEROFILL,
              Col_114 REAL(24, 8) UNSIGNED ZEROFILL,
              Col_115 FLOAT(25) UNSIGNED ZEROFILL,

              Col_201 TINYINT(11) UNSIGNED,
              Col_202 SMALLINT(12) UNSIGNED,
              Col_203 MEDIUMINT(13) UNSIGNED,
              Col_204 INT(14) UNSIGNED,
              Col_205 INTEGER(15) UNSIGNED,
              Col_206 BIGINT(16) UNSIGNED,
              Col_207 DECIMAL(17, 1) UNSIGNED,
              Col_208 DEC(18, 2) UNSIGNED,
              Col_209 NUMERIC(19, 3) UNSIGNED,
              Col_210 FIXED(20, 4) UNSIGNED,
              Col_211 FLOAT(21, 5) UNSIGNED,
              Col_212 DOUBLE(22, 6) UNSIGNED,
              Col_213 DOUBLE PRECISION(23, 7) UNSIGNED,
              Col_214 REAL(24, 8) UNSIGNED,
              Col_215 FLOAT(25) UNSIGNED,

              Col_301 TINYINT(11) ZEROFILL,
              Col_302 SMALLINT(12) ZEROFILL,
              Col_303 MEDIUMINT(13) ZEROFILL,
              Col_304 INT(14) ZEROFILL,
              Col_305 INTEGER(15) ZEROFILL,
              Col_306 BIGINT(16) ZEROFILL,
              Col_307 DECIMAL(17, 1) ZEROFILL,
              Col_308 DEC(18, 2) ZEROFILL,
              Col_309 NUMERIC(19, 3) ZEROFILL,
              Col_310 FIXED(20, 4) ZEROFILL,
              Col_311 FLOAT(21, 5) ZEROFILL,
              Col_312 DOUBLE(22, 6) ZEROFILL,
              Col_313 DOUBLE PRECISION(23, 7) ZEROFILL,
              Col_314 REAL(24, 8) ZEROFILL,
              Col_315 FLOAT(25) ZEROFILL,

              Col_401 TINYINT(11) UNSIGNED ZEROFILL NOT NULL,
              Col_402 SMALLINT(12) UNSIGNED ZEROFILL NOT NULL,
              Col_403 MEDIUMINT(13) UNSIGNED ZEROFILL NOT NULL,
              Col_404 INT(14) UNSIGNED ZEROFILL NOT NULL,
              Col_405 INTEGER(15) UNSIGNED ZEROFILL NOT NULL,
              Col_406 BIGINT(16) UNSIGNED ZEROFILL NOT NULL,
              Col_407 DECIMAL(17, 1) UNSIGNED ZEROFILL NOT NULL,
              Col_408 DEC(18, 2) UNSIGNED ZEROFILL NOT NULL,
              Col_409 NUMERIC(19, 3) UNSIGNED ZEROFILL NOT NULL,
              Col_410 FIXED(20, 4) UNSIGNED ZEROFILL NOT NULL,
              Col_411 FLOAT(21, 5) UNSIGNED ZEROFILL NOT NULL,
              Col_412 DOUBLE(22, 6) UNSIGNED ZEROFILL NOT NULL,
              Col_413 DOUBLE PRECISION(23, 7) UNSIGNED ZEROFILL NOT NULL,
              Col_414 REAL(24, 8) UNSIGNED ZEROFILL NOT NULL,
              Col_415 FLOAT(25) UNSIGNED ZEROFILL NOT NULL
            );
            """,
        "database": DdlParse.DATABASE.mysql,
        "table": {"schema": None, "name": "Sample_Table", "temp": False},
        "columns": [
            {"name": "Col_101", "type": "TINYINT", "length": 11, "scale": None, "array_dimensional": 0, "is_unsigned": True, "is_zerofill": True, "not_null": False, "pk": False, "unique": False, "constraint": "", "description": None},
            {"name": "Col_102", "type": "SMALLINT", "length": 12, "scale": None, "array_dimensional": 0, "is_unsigned": True, "is_zerofill": True, "not_null": False, "pk": False, "unique": False, "constraint": "", "description": None},
            {"name": "Col_103", "type": "MEDIUMINT", "length": 13, "scale": None, "array_dimensional": 0, "is_unsigned": True, "is_zerofill": True, "not_null": False, "pk": False, "unique": False, "constraint": "", "description": None},
            {"name": "Col_104", "type": "INT", "length": 14, "scale": None, "array_dimensional": 0, "is_unsigned": True, "is_zerofill": True, "not_null": False, "pk": False, "unique": False, "constraint": "", "description": None},
            {"name": "Col_105", "type": "INTEGER", "length": 15, "scale": None, "array_dimensional": 0, "is_unsigned": True, "is_zerofill": True, "not_null": False, "pk": False, "unique": False, "constraint": "", "description": None},
            {"name": "Col_106", "type": "BIGINT", "length": 16, "scale": None, "array_dimensional": 0, "is_unsigned": True, "is_zerofill": True, "not_null": False, "pk": False, "unique": False, "constraint": "", "description": None},
            {"name": "Col_107", "type": "DECIMAL", "length": 17, "scale": 1, "array_dimensional": 0, "is_unsigned": True, "is_zerofill": True, "not_null": False, "pk": False, "unique": False, "constraint": "", "description": None},
            {"name": "Col_108", "type": "DEC", "length": 18, "scale": 2, "array_dimensional": 0, "is_unsigned": True, "is_zerofill": True, "not_null": False, "pk": False, "unique": False, "constraint": "", "description": None},
            {"name": "Col_109", "type": "NUMERIC", "length": 19, "scale": 3, "array_dimensional": 0, "is_unsigned": True, "is_zerofill": True, "not_null": False, "pk": False, "unique": False, "constraint": "", "description": None},
            {"name": "Col_110", "type": "FIXED", "length": 20, "scale": 4, "array_dimensional": 0, "is_unsigned": True, "is_zerofill": True, "not_null": False, "pk": False, "unique": False, "constraint": "", "description": None},
            {"name": "Col_111", "type": "FLOAT", "length": 21, "scale": 5, "array_dimensional": 0, "is_unsigned": True, "is_zerofill": True, "not_null": False, "pk": False, "unique": False, "constraint": "", "description": None},
            {"name": "Col_112", "type": "DOUBLE", "length": 22, "scale": 6, "array_dimensional": 0, "is_unsigned": True, "is_zerofill": True, "not_null": False, "pk": False, "unique": False, "constraint": "", "description": None},
            {"name": "Col_113", "type": "DOUBLE PRECISION", "length": 23, "scale": 7, "array_dimensional": 0, "is_unsigned": True, "is_zerofill": True, "not_null": False, "pk": False, "unique": False, "constraint": "", "description": None},
            {"name": "Col_114", "type": "REAL", "length": 24, "scale": 8, "array_dimensional": 0, "is_unsigned": True, "is_zerofill": True, "not_null": False, "pk": False, "unique": False, "constraint": "", "description": None},
            {"name": "Col_115", "type": "FLOAT", "length": 25, "scale": None, "array_dimensional": 0, "is_unsigned": True, "is_zerofill": True, "not_null": False, "pk": False, "unique": False, "constraint": "", "description": None},

            {"name": "Col_201", "type": "TINYINT", "length": 11, "scale": None, "array_dimensional": 0, "is_unsigned": True, "is_zerofill": False, "not_null": False, "pk": False, "unique": False, "constraint": "", "description": None},
            {"name": "Col_202", "type": "SMALLINT", "length": 12, "scale": None, "array_dimensional": 0, "is_unsigned": True, "is_zerofill": False, "not_null": False, "pk": False, "unique": False, "constraint": "", "description": None},
            {"name": "Col_203", "type": "MEDIUMINT", "length": 13, "scale": None, "array_dimensional": 0, "is_unsigned": True, "is_zerofill": False, "not_null": False, "pk": False, "unique": False, "constraint": "", "description": None},
            {"name": "Col_204", "type": "INT", "length": 14, "scale": None, "array_dimensional": 0, "is_unsigned": True, "is_zerofill": False, "not_null": False, "pk": False, "unique": False, "constraint": "", "description": None},
            {"name": "Col_205", "type": "INTEGER", "length": 15, "scale": None, "array_dimensional": 0, "is_unsigned": True, "is_zerofill": False, "not_null": False, "pk": False, "unique": False, "constraint": "", "description": None},
            {"name": "Col_206", "type": "BIGINT", "length": 16, "scale": None, "array_dimensional": 0, "is_unsigned": True, "is_zerofill": False, "not_null": False, "pk": False, "unique": False, "constraint": "", "description": None},
            {"name": "Col_207", "type": "DECIMAL", "length": 17, "scale": 1, "array_dimensional": 0, "is_unsigned": True, "is_zerofill": False, "not_null": False, "pk": False, "unique": False, "constraint": "", "description": None},
            {"name": "Col_208", "type": "DEC", "length": 18, "scale": 2, "array_dimensional": 0, "is_unsigned": True, "is_zerofill": False, "not_null": False, "pk": False, "unique": False, "constraint": "", "description": None},
            {"name": "Col_209", "type": "NUMERIC", "length": 19, "scale": 3, "array_dimensional": 0, "is_unsigned": True, "is_zerofill": False, "not_null": False, "pk": False, "unique": False, "constraint": "", "description": None},
            {"name": "Col_210", "type": "FIXED", "length": 20, "scale": 4, "array_dimensional": 0, "is_unsigned": True, "is_zerofill": False, "not_null": False, "pk": False, "unique": False, "constraint": "", "description": None},
            {"name": "Col_211", "type": "FLOAT", "length": 21, "scale": 5, "array_dimensional": 0, "is_unsigned": True, "is_zerofill": False, "not_null": False, "pk": False, "unique": False, "constraint": "", "description": None},
            {"name": "Col_212", "type": "DOUBLE", "length": 22, "scale": 6, "array_dimensional": 0, "is_unsigned": True, "is_zerofill": False, "not_null": False, "pk": False, "unique": False, "constraint": "", "description": None},
            {"name": "Col_213", "type": "DOUBLE PRECISION", "length": 23, "scale": 7, "array_dimensional": 0, "is_unsigned": True, "is_zerofill": False, "not_null": False, "pk": False, "unique": False, "constraint": "", "description": None},
            {"name": "Col_214", "type": "REAL", "length": 24, "scale": 8, "array_dimensional": 0, "is_unsigned": True, "is_zerofill": False, "not_null": False, "pk": False, "unique": False, "constraint": "", "description": None},
            {"name": "Col_215", "type": "FLOAT", "length": 25, "scale": None, "array_dimensional": 0, "is_unsigned": True, "is_zerofill": False, "not_null": False, "pk": False, "unique": False, "constraint": "", "description": None},

            {"name": "Col_301", "type": "TINYINT", "length": 11, "scale": None, "array_dimensional": 0, "is_unsigned": False, "is_zerofill": True, "not_null": False, "pk": False, "unique": False, "constraint": "", "description": None},
            {"name": "Col_302", "type": "SMALLINT", "length": 12, "scale": None, "array_dimensional": 0, "is_unsigned": False, "is_zerofill": True, "not_null": False, "pk": False, "unique": False, "constraint": "", "description": None},
            {"name": "Col_303", "type": "MEDIUMINT", "length": 13, "scale": None, "array_dimensional": 0, "is_unsigned": False, "is_zerofill": True, "not_null": False, "pk": False, "unique": False, "constraint": "", "description": None},
            {"name": "Col_304", "type": "INT", "length": 14, "scale": None, "array_dimensional": 0, "is_unsigned": False, "is_zerofill": True, "not_null": False, "pk": False, "unique": False, "constraint": "", "description": None},
            {"name": "Col_305", "type": "INTEGER", "length": 15, "scale": None, "array_dimensional": 0, "is_unsigned": False, "is_zerofill": True, "not_null": False, "pk": False, "unique": False, "constraint": "", "description": None},
            {"name": "Col_306", "type": "BIGINT", "length": 16, "scale": None, "array_dimensional": 0, "is_unsigned": False, "is_zerofill": True, "not_null": False, "pk": False, "unique": False, "constraint": "", "description": None},
            {"name": "Col_307", "type": "DECIMAL", "length": 17, "scale": 1, "array_dimensional": 0, "is_unsigned": False, "is_zerofill": True, "not_null": False, "pk": False, "unique": False, "constraint": "", "description": None},
            {"name": "Col_308", "type": "DEC", "length": 18, "scale": 2, "array_dimensional": 0, "is_unsigned": False, "is_zerofill": True, "not_null": False, "pk": False, "unique": False, "constraint": "", "description": None},
            {"name": "Col_309", "type": "NUMERIC", "length": 19, "scale": 3, "array_dimensional": 0, "is_unsigned": False, "is_zerofill": True, "not_null": False, "pk": False, "unique": False, "constraint": "", "description": None},
            {"name": "Col_310", "type": "FIXED", "length": 20, "scale": 4, "array_dimensional": 0, "is_unsigned": False, "is_zerofill": True, "not_null": False, "pk": False, "unique": False, "constraint": "", "description": None},
            {"name": "Col_311", "type": "FLOAT", "length": 21, "scale": 5, "array_dimensional": 0, "is_unsigned": False, "is_zerofill": True, "not_null": False, "pk": False, "unique": False, "constraint": "", "description": None},
            {"name": "Col_312", "type": "DOUBLE", "length": 22, "scale": 6, "array_dimensional": 0, "is_unsigned": False, "is_zerofill": True, "not_null": False, "pk": False, "unique": False, "constraint": "", "description": None},
            {"name": "Col_313", "type": "DOUBLE PRECISION", "length": 23, "scale": 7, "array_dimensional": 0, "is_unsigned": False, "is_zerofill": True, "not_null": False, "pk": False, "unique": False, "constraint": "", "description": None},
            {"name": "Col_314", "type": "REAL", "length": 24, "scale": 8, "array_dimensional": 0, "is_unsigned": False, "is_zerofill": True, "not_null": False, "pk": False, "unique": False, "constraint": "", "description": None},
            {"name": "Col_315", "type": "FLOAT", "length": 25, "scale": None, "array_dimensional": 0, "is_unsigned": False, "is_zerofill": True, "not_null": False, "pk": False, "unique": False, "constraint": "", "description": None},

            {"name": "Col_401", "type": "TINYINT", "length": 11, "scale": None, "array_dimensional": 0, "is_unsigned": True, "is_zerofill": True, "not_null": True, "pk": False, "unique": False, "constraint": "NOT NULL", "description": None},
            {"name": "Col_402", "type": "SMALLINT", "length": 12, "scale": None, "array_dimensional": 0, "is_unsigned": True, "is_zerofill": True, "not_null": True, "pk": False, "unique": False, "constraint": "NOT NULL", "description": None},
            {"name": "Col_403", "type": "MEDIUMINT", "length": 13, "scale": None, "array_dimensional": 0, "is_unsigned": True, "is_zerofill": True, "not_null": True, "pk": False, "unique": False, "constraint": "NOT NULL", "description": None},
            {"name": "Col_404", "type": "INT", "length": 14, "scale": None, "array_dimensional": 0, "is_unsigned": True, "is_zerofill": True, "not_null": True, "pk": False, "unique": False, "constraint": "NOT NULL", "description": None},
            {"name": "Col_405", "type": "INTEGER", "length": 15, "scale": None, "array_dimensional": 0, "is_unsigned": True, "is_zerofill": True, "not_null": True, "pk": False, "unique": False, "constraint": "NOT NULL", "description": None},
            {"name": "Col_406", "type": "BIGINT", "length": 16, "scale": None, "array_dimensional": 0, "is_unsigned": True, "is_zerofill": True, "not_null": True, "pk": False, "unique": False, "constraint": "NOT NULL", "description": None},
            {"name": "Col_407", "type": "DECIMAL", "length": 17, "scale": 1, "array_dimensional": 0, "is_unsigned": True, "is_zerofill": True, "not_null": True, "pk": False, "unique": False, "constraint": "NOT NULL", "description": None},
            {"name": "Col_408", "type": "DEC", "length": 18, "scale": 2, "array_dimensional": 0, "is_unsigned": True, "is_zerofill": True, "not_null": True, "pk": False, "unique": False, "constraint": "NOT NULL", "description": None},
            {"name": "Col_409", "type": "NUMERIC", "length": 19, "scale": 3, "array_dimensional": 0, "is_unsigned": True, "is_zerofill": True, "not_null": True, "pk": False, "unique": False, "constraint": "NOT NULL", "description": None},
            {"name": "Col_410", "type": "FIXED", "length": 20, "scale": 4, "array_dimensional": 0, "is_unsigned": True, "is_zerofill": True, "not_null": True, "pk": False, "unique": False, "constraint": "NOT NULL", "description": None},
            {"name": "Col_411", "type": "FLOAT", "length": 21, "scale": 5, "array_dimensional": 0, "is_unsigned": True, "is_zerofill": True, "not_null": True, "pk": False, "unique": False, "constraint": "NOT NULL", "description": None},
            {"name": "Col_412", "type": "DOUBLE", "length": 22, "scale": 6, "array_dimensional": 0, "is_unsigned": True, "is_zerofill": True, "not_null": True, "pk": False, "unique": False, "constraint": "NOT NULL", "description": None},
            {"name": "Col_413", "type": "DOUBLE PRECISION", "length": 23, "scale": 7, "array_dimensional": 0, "is_unsigned": True, "is_zerofill": True, "not_null": True, "pk": False, "unique": False, "constraint": "NOT NULL", "description": None},
            {"name": "Col_414", "type": "REAL", "length": 24, "scale": 8, "array_dimensional": 0, "is_unsigned": True, "is_zerofill": True, "not_null": True, "pk": False, "unique": False, "constraint": "NOT NULL", "description": None},
            {"name": "Col_415", "type": "FLOAT", "length": 25, "scale": None, "array_dimensional": 0, "is_unsigned": True, "is_zerofill": True, "not_null": True, "pk": False, "unique": False, "constraint": "NOT NULL", "description": None},
        ],
        "bq_field": [
            '{"name": "Col_101", "type": "INTEGER", "mode": "NULLABLE"}',
            '{"name": "Col_102", "type": "INTEGER", "mode": "NULLABLE"}',
            '{"name": "Col_103", "type": "INTEGER", "mode": "NULLABLE"}',
            '{"name": "Col_104", "type": "INTEGER", "mode": "NULLABLE"}',
            '{"name": "Col_105", "type": "INTEGER", "mode": "NULLABLE"}',
            '{"name": "Col_106", "type": "INTEGER", "mode": "NULLABLE"}',
            '{"name": "Col_107", "type": "FLOAT", "mode": "NULLABLE"}',
            '{"name": "Col_108", "type": "FLOAT", "mode": "NULLABLE"}',
            '{"name": "Col_109", "type": "NUMERIC", "mode": "NULLABLE"}',
            '{"name": "Col_110", "type": "NUMERIC", "mode": "NULLABLE"}',
            '{"name": "Col_111", "type": "FLOAT", "mode": "NULLABLE"}',
            '{"name": "Col_112", "type": "FLOAT", "mode": "NULLABLE"}',
            '{"name": "Col_113", "type": "FLOAT", "mode": "NULLABLE"}',
            '{"name": "Col_114", "type": "FLOAT", "mode": "NULLABLE"}',
            '{"name": "Col_115", "type": "FLOAT", "mode": "NULLABLE"}',

            '{"name": "Col_201", "type": "INTEGER", "mode": "NULLABLE"}',
            '{"name": "Col_202", "type": "INTEGER", "mode": "NULLABLE"}',
            '{"name": "Col_203", "type": "INTEGER", "mode": "NULLABLE"}',
            '{"name": "Col_204", "type": "INTEGER", "mode": "NULLABLE"}',
            '{"name": "Col_205", "type": "INTEGER", "mode": "NULLABLE"}',
            '{"name": "Col_206", "type": "INTEGER", "mode": "NULLABLE"}',
            '{"name": "Col_207", "type": "FLOAT", "mode": "NULLABLE"}',
            '{"name": "Col_208", "type": "FLOAT", "mode": "NULLABLE"}',
            '{"name": "Col_209", "type": "NUMERIC", "mode": "NULLABLE"}',
            '{"name": "Col_210", "type": "NUMERIC", "mode": "NULLABLE"}',
            '{"name": "Col_211", "type": "FLOAT", "mode": "NULLABLE"}',
            '{"name": "Col_212", "type": "FLOAT", "mode": "NULLABLE"}',
            '{"name": "Col_213", "type": "FLOAT", "mode": "NULLABLE"}',
            '{"name": "Col_214", "type": "FLOAT", "mode": "NULLABLE"}',
            '{"name": "Col_215", "type": "FLOAT", "mode": "NULLABLE"}',

            '{"name": "Col_301", "type": "INTEGER", "mode": "NULLABLE"}',
            '{"name": "Col_302", "type": "INTEGER", "mode": "NULLABLE"}',
            '{"name": "Col_303", "type": "INTEGER", "mode": "NULLABLE"}',
            '{"name": "Col_304", "type": "INTEGER", "mode": "NULLABLE"}',
            '{"name": "Col_305", "type": "INTEGER", "mode": "NULLABLE"}',
            '{"name": "Col_306", "type": "INTEGER", "mode": "NULLABLE"}',
            '{"name": "Col_307", "type": "FLOAT", "mode": "NULLABLE"}',
            '{"name": "Col_308", "type": "FLOAT", "mode": "NULLABLE"}',
            '{"name": "Col_309", "type": "NUMERIC", "mode": "NULLABLE"}',
            '{"name": "Col_310", "type": "NUMERIC", "mode": "NULLABLE"}',
            '{"name": "Col_311", "type": "FLOAT", "mode": "NULLABLE"}',
            '{"name": "Col_312", "type": "FLOAT", "mode": "NULLABLE"}',
            '{"name": "Col_313", "type": "FLOAT", "mode": "NULLABLE"}',
            '{"name": "Col_314", "type": "FLOAT", "mode": "NULLABLE"}',
            '{"name": "Col_315", "type": "FLOAT", "mode": "NULLABLE"}',

            '{"name": "Col_401", "type": "INTEGER", "mode": "REQUIRED"}',
            '{"name": "Col_402", "type": "INTEGER", "mode": "REQUIRED"}',
            '{"name": "Col_403", "type": "INTEGER", "mode": "REQUIRED"}',
            '{"name": "Col_404", "type": "INTEGER", "mode": "REQUIRED"}',
            '{"name": "Col_405", "type": "INTEGER", "mode": "REQUIRED"}',
            '{"name": "Col_406", "type": "INTEGER", "mode": "REQUIRED"}',
            '{"name": "Col_407", "type": "FLOAT", "mode": "REQUIRED"}',
            '{"name": "Col_408", "type": "FLOAT", "mode": "REQUIRED"}',
            '{"name": "Col_409", "type": "NUMERIC", "mode": "REQUIRED"}',
            '{"name": "Col_410", "type": "NUMERIC", "mode": "REQUIRED"}',
            '{"name": "Col_411", "type": "FLOAT", "mode": "REQUIRED"}',
            '{"name": "Col_412", "type": "FLOAT", "mode": "REQUIRED"}',
            '{"name": "Col_413", "type": "FLOAT", "mode": "REQUIRED"}',
            '{"name": "Col_414", "type": "FLOAT", "mode": "REQUIRED"}',
            '{"name": "Col_415", "type": "FLOAT", "mode": "REQUIRED"}',
        ],
        "bq_standard_data_type": [
            "INT64",
            "INT64",
            "INT64",
            "INT64",
            "INT64",
            "INT64",
            "FLOAT64",
            "FLOAT64",
            "NUMERIC",
            "NUMERIC",
            "FLOAT64",
            "FLOAT64",
            "FLOAT64",
            "FLOAT64",
            "FLOAT64",

            "INT64",
            "INT64",
            "INT64",
            "INT64",
            "INT64",
            "INT64",
            "FLOAT64",
            "FLOAT64",
            "NUMERIC",
            "NUMERIC",
            "FLOAT64",
            "FLOAT64",
            "FLOAT64",
            "FLOAT64",
            "FLOAT64",

            "INT64",
            "INT64",
            "INT64",
            "INT64",
            "INT64",
            "INT64",
            "FLOAT64",
            "FLOAT64",
            "NUMERIC",
            "NUMERIC",
            "FLOAT64",
            "FLOAT64",
            "FLOAT64",
            "FLOAT64",
            "FLOAT64",

            "INT64",
            "INT64",
            "INT64",
            "INT64",
            "INT64",
            "INT64",
            "FLOAT64",
            "FLOAT64",
            "NUMERIC",
            "NUMERIC",
            "FLOAT64",
            "FLOAT64",
            "FLOAT64",
            "FLOAT64",
            "FLOAT64",
        ],
    },

    "name_backquote":
    {
        "ddl":
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
        "database": None,
        "table": {"schema": None, "name": "Sample_Table", "temp": False},
        "columns": [
            {"name": "Col_01", "type": "VARCHAR", "length": 100, "scale": None, "array_dimensional": 0, "is_unsigned": False, "is_zerofill": False, "not_null": True, "pk": True, "unique": False, "constraint": "PRIMARY KEY", "description": None},
            {"name": "Col_02", "type": "CHAR", "length": 200, "scale": None, "array_dimensional": 0, "is_unsigned": False, "is_zerofill": False, "not_null": True, "pk": True, "unique": False, "constraint": "PRIMARY KEY", "description": None},
            {"name": "Col_03", "type": "INTEGER", "length": None, "scale": None, "array_dimensional": 0, "is_unsigned": False, "is_zerofill": False, "not_null": False, "pk": False, "unique": True, "constraint": "UNIQUE", "description": None},
            {"name": "Col_04", "type": "DOUBLE", "length": None, "scale": None, "array_dimensional": 0, "is_unsigned": False, "is_zerofill": False, "not_null": False, "pk": False, "unique": True, "constraint": "UNIQUE", "description": None},
            {"name": "Col_05", "type": "DATETIME", "length": None, "scale": None, "array_dimensional": 0, "is_unsigned": False, "is_zerofill": False, "not_null": False, "pk": False, "unique": False, "constraint": "", "description": None},
        ],
        "bq_field": [
            '{"name": "Col_01", "type": "STRING", "mode": "REQUIRED"}',
            '{"name": "Col_02", "type": "STRING", "mode": "REQUIRED"}',
            '{"name": "Col_03", "type": "INTEGER", "mode": "NULLABLE"}',
            '{"name": "Col_04", "type": "FLOAT", "mode": "NULLABLE"}',
            '{"name": "Col_05", "type": "DATETIME", "mode": "NULLABLE"}',
        ],
        "bq_standard_data_type": [
            "STRING",
            "STRING",
            "INT64",
            "FLOAT64",
            "DATETIME",
        ],
    },

    "name_doublequote":
    {
        "ddl":
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
        "database": None,
        "table": {"schema": None, "name": "Sample_Table", "temp": False},
        "columns": [
            {"name": "Col_01", "type": "VARCHAR", "length": 100, "scale": None, "array_dimensional": 0, "is_unsigned": False, "is_zerofill": False, "not_null": True, "pk": True, "unique": False, "constraint": "PRIMARY KEY", "description": None},
            {"name": "Col_02", "type": "CHAR", "length": 200, "scale": None, "array_dimensional": 0, "is_unsigned": False, "is_zerofill": False, "not_null": True, "pk": True, "unique": False, "constraint": "PRIMARY KEY", "description": None},
            {"name": "Col_03", "type": "INTEGER", "length": None, "scale": None, "array_dimensional": 0, "is_unsigned": False, "is_zerofill": False, "not_null": False, "pk": False, "unique": True, "constraint": "UNIQUE", "description": None},
            {"name": "Col_04", "type": "DOUBLE", "length": None, "scale": None, "array_dimensional": 0, "is_unsigned": False, "is_zerofill": False, "not_null": False, "pk": False, "unique": True, "constraint": "UNIQUE", "description": None},
            {"name": "Col_05", "type": "DATETIME", "length": None, "scale": None, "array_dimensional": 0, "is_unsigned": False, "is_zerofill": False, "not_null": False, "pk": False, "unique": False, "constraint": "", "description": None},
        ],
        "bq_field": [
            '{"name": "Col_01", "type": "STRING", "mode": "REQUIRED"}',
            '{"name": "Col_02", "type": "STRING", "mode": "REQUIRED"}',
            '{"name": "Col_03", "type": "INTEGER", "mode": "NULLABLE"}',
            '{"name": "Col_04", "type": "FLOAT", "mode": "NULLABLE"}',
            '{"name": "Col_05", "type": "DATETIME", "mode": "NULLABLE"}',
        ],
        "bq_standard_data_type": [
            "STRING",
            "STRING",
            "INT64",
            "FLOAT64",
            "DATETIME",
        ],
    },

    "temp_table":
    {
        "ddl":
            """
            CREATE TEMP TABLE Sample_Table (
              Col_01 varchar(100)
            );
            """,
        "database": None,
        "table": {"schema": None, "name": "Sample_Table", "temp": True},
        "columns": [
            {"name": "Col_01", "type": "VARCHAR", "length": 100, "scale": None, "array_dimensional": 0, "is_unsigned": False, "is_zerofill": False, "not_null": False, "pk": False, "unique": False, "constraint": "", "description": None},
        ],
        "bq_field": [
            '{"name": "Col_01", "type": "STRING", "mode": "NULLABLE"}',
        ],
        "bq_standard_data_type": [
            "STRING",
        ],
    },

    "table_with_schema":
    {
        "ddl":
            """
            CREATE TABLE "My_Schema"."Sample_Table" (
              Col_01 varchar(100)
            );
            """,
        "database": None,
        "table": {"schema": "My_Schema", "name": "Sample_Table", "temp": False},
        "columns": [
            {"name": "Col_01", "type": "VARCHAR", "length": 100, "scale": None, "array_dimensional": 0, "is_unsigned": False, "is_zerofill": False, "not_null": False, "pk": False, "unique": False, "constraint": "", "description": None},
        ],
        "bq_field": [
            '{"name": "Col_01", "type": "STRING", "mode": "NULLABLE"}',
        ],
        "bq_standard_data_type": [
            "STRING",
        ],
    },

    "column_comment": {
        "ddl":
            """
            CREATE TABLE Sample_Table (
              Col_01 character varying(100) PRIMARY KEY COMMENT 'Single Quote',
              Col_02 json NOT NULL COMMENT "Double Quote",
              Col_03 integer[] COMMENT 'in "Quote"', -- one dimensional array
              Col_04 integer[][] COMMENT "in 'Quote'", -- two dimensional array
              Col_05 integer[][][] COMMENT 'コメント is full-width(Japanese) character', -- multiple dimensional array
              Col_06 character NOT NULL DEFAULT 'a\\'bc' COMMENT 'Comma, strings, ,', -- Comma, strings, ,
              Col_07 character varying[] -- COMMENT 'Comment out' -- character varying array
            );
            """,
        "database": None,
        "table": {"schema": None, "name": "Sample_Table", "temp": False},
        "columns": [
            {"name": "Col_01", "type": "CHARACTER VARYING", "length": 100, "scale": None, "array_dimensional": 0, "is_unsigned": False, "is_zerofill": False, "not_null": True, "pk": True, "unique": False, "constraint": "PRIMARY KEY", "description": "Single Quote"},
            {"name": "Col_02", "type": "JSON", "length": None, "scale": None, "array_dimensional": 0, "is_unsigned": False, "is_zerofill": False, "not_null": True, "pk": False, "unique": False, "constraint": "NOT NULL", "description": "Double Quote"},
            {"name": "Col_03", "type": "INTEGER", "length": None, "scale": None, "array_dimensional": 1, "is_unsigned": False, "is_zerofill": False, "not_null": False, "pk": False, "unique": False, "constraint": "", "description": "in \"Quote\""},
            {"name": "Col_04", "type": "INTEGER", "length": None, "scale": None, "array_dimensional": 2, "is_unsigned": False, "is_zerofill": False, "not_null": False, "pk": False, "unique": False, "constraint": "", "description": "in 'Quote'"},
            {"name": "Col_05", "type": "INTEGER", "length": None, "scale": None, "array_dimensional": 3, "is_unsigned": False, "is_zerofill": False, "not_null": False, "pk": False, "unique": False, "constraint": "", "description": "コメント is full-width(Japanese) character"},
            {"name": "Col_06", "type": "CHARACTER", "length": None, "scale": None, "array_dimensional": 0, "is_unsigned": False, "is_zerofill": False, "not_null": True, "pk": False, "unique": False, "constraint": "NOT NULL", "description": "Comma, strings, ,"},
            {"name": "Col_07", "type": "CHARACTER VARYING", "length": None, "scale": None, "array_dimensional": 1, "is_unsigned": False, "is_zerofill": False, "not_null": False, "pk": False, "unique": False, "constraint": "", "description": None},
        ],
        "bq_field": [
            '{"name": "Col_01", "type": "STRING", "mode": "REQUIRED", "description": "Single Quote"}',
            '{"name": "Col_02", "type": "STRING", "mode": "REQUIRED", "description": "Double Quote"}',
            '{"name": "Col_03", "type": "INTEGER", "mode": "REPEATED", "description": "in \\"Quote\\""}',
            '{"name": "Col_04", "type": "RECORD", "mode": "REPEATED", "description": "in \'Quote\'", "fields": [{"name": "dimension_1", "type": "INTEGER", "mode": "REPEATED"}]}',
            '{"name": "Col_05", "type": "RECORD", "mode": "REPEATED", "description": "コメント is full-width(Japanese) character", "fields": [{"name": "dimension_1", "type": "RECORD", "mode": "REPEATED", "fields": [{"name": "dimension_2", "type": "INTEGER", "mode": "REPEATED"}]}]}',
            '{"name": "Col_06", "type": "STRING", "mode": "REQUIRED", "description": "Comma, strings, ,"}',
            '{"name": "Col_07", "type": "STRING", "mode": "REPEATED"}',
        ],
        "bq_standard_data_type": [
            "STRING",
            "STRING",
            "INT64",
            "INT64",
            "INT64",
            "STRING",
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
              Col_06 character COMMENT 'Comma, strings, ,', -- Comma, strings, ,
              Col_07 character varying[] -- COMMENT 'Comment out' -- character varying array
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
                  Col_06 STRING OPTIONS (description = "Comma, strings, ,"),
                  Col_07 ARRAY<STRING>
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
                  col_06 STRING OPTIONS (description = "Comma, strings, ,"),
                  col_07 ARRAY<STRING>
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
                  COL_06 STRING OPTIONS (description = "Comma, strings, ,"),
                  COL_07 ARRAY<STRING>
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
    ("datatype_mysql", DDL_SET_PATTERN.method),
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
        assert col.is_unsigned == data_col["is_unsigned"]
        assert col.is_zerofill == data_col["is_zerofill"]
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

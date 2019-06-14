# -*- coding: utf-8 -*-
#
# Copyright (C) 2018 Shinichi Takii, shinichi.takii@gmail.com
#
# This module is part of python-ddlparse and is released under
# the BSD License: https://opensource.org/licenses/BSD-3-Clause

"""Parse DDL statements"""

import re, textwrap, json
from collections import OrderedDict
from enum import IntEnum

from pyparsing import CaselessKeyword, Forward, Word, Regex, alphanums, \
    delimitedList, Suppress, Optional, Group, OneOrMore


class DdlParseBase():

    NAME_CASE = IntEnum("NAME_CASE", "original lower upper")
    DATABASE = IntEnum("DATABASE", "mysql, postgresql, oracle, redshift")

    def __init__(self, source_database=None):
        self._source_database = source_database

    @property
    def source_database(self):
        """
        Source database option

        :param source_database: enum DdlParse.DATABASE
        """
        return self._source_database

    @source_database.setter
    def source_database(self, source_database):
        self._source_database = source_database


class DdlParseTableColumnBase(DdlParseBase):

    def __init__(self, source_database=None):
        super().__init__(source_database)
        self._name = ""

    @property
    def name(self):
        """name"""
        return self._name

    @name.setter
    def name(self, name):
        self._name = name

    def get_name(self, name_case=DdlParseBase.NAME_CASE.original):
        """
        Get Name converted case

        :param name_case: name case type
            * DdlParse.NAME_CASE.original : Return to no convert
            * DdlParse.NAME_CASE.lower : Return to lower
            * DdlParse.NAME_CASE.upper : Return to upper

        :return: name
        """
        if name_case == self.NAME_CASE.lower:
            return self._name.lower()
        elif name_case == self.NAME_CASE.upper:
            return self._name.upper()
        else:
            return self._name


class DdlParseColumn(DdlParseTableColumnBase):
    """Column define info"""

    def __init__(self, name, data_type_array, array_brackets=None, constraint=None, source_database=None):
        """
        :param data_type_array[]: Column data type ['data type name'] or ['data type name', '(length)'] or ['data type name', '(precision, scale)']
        :param array_brackets: Column array brackets string '[]' or '[][]...'
        :param constraint: Column constraint string
        :param source_database: enum DdlParse.DATABASE
        """

        super().__init__(source_database)
        self._name = name
        self._set_data_type(data_type_array)
        self.constraint = constraint
        self._array_dimensional = 0 if array_brackets is None else array_brackets.count('[]')

    @property
    def data_type(self):
        return self._data_type

    @property
    def length(self):
        return self._length

    @property
    def precision(self):
        return self._length

    @property
    def scale(self):
        return self._scale

    def _set_data_type(self, data_type_array):
        self._data_type = data_type_array[0].upper()
        self._length = None
        self._scale = None

        if len(data_type_array) < 2:
            return

        matches = re.findall(r"(\d+)\s*,*\s*(\d*)", data_type_array[-1])
        if len(matches) > 0:
            self._length = int(matches[0][0])
            self._scale = None if len(matches[0]) < 2 or matches[0][1] == "" or int(matches[0][1]) == 0 else int(matches[0][1])

        if re.search(r"^\D+", data_type_array[1]):
            self._data_type += " {}".format(data_type_array[1])


    @property
    def constraint(self):
        """Constraint string"""
        constraint_arr = []
        if self._not_null:
            constraint_arr.append("PRIMARY KEY" if self._pk else "NOT NULL")
        if self._unique:
            constraint_arr.append("UNIQUE")

        return " ".join(constraint_arr)

    @constraint.setter
    def constraint(self, constraint):
        self._constraint = None if constraint is None else constraint.upper()

        self._not_null = False if self._constraint is None or not re.search(r"(NOT NULL|PRIMARY KEY)", self._constraint) else True
        self._pk = False if self._constraint is None or not re.search("PRIMARY KEY", self._constraint) else True
        self._unique = False if self._constraint is None or not re.search("UNIQUE", self._constraint) else True

        self._comment = None
        if constraint is not None:
            matches = re.findall(r"(?:\bCOMMENT\b\s+)(['\"])(.+)\1", constraint, re.IGNORECASE)
            if len(matches) > 0:
                self._comment = matches[0][1]

    @property
    def comment(self):
        return self._comment

    @comment.setter
    def comment(self, comment):
        self._comment = comment

    @property
    def description(self):
        return self.comment

    @description.setter
    def description(self, description):
        self.comment = description

    @property
    def array_dimensional(self):
        """array dimensional number"""
        return self._array_dimensional

    @property
    def not_null(self):
        return self._not_null

    @not_null.setter
    def not_null(self, flag):
        self._not_null = flag

    @property
    def primary_key(self):
        return self._pk

    @primary_key.setter
    def primary_key(self, flag):
        self._pk = flag

    @property
    def unique(self):
        return self._unique

    @unique.setter
    def unique(self, flag):
        self._unique = flag

    @property
    def bigquery_data_type(self):
        """Get BigQuery Legacy SQL data type"""

        # BigQuery data type = {source_database: [data type, ...], ...}
        BQ_DATA_TYPE_DIC = OrderedDict()
        BQ_DATA_TYPE_DIC["STRING"] = {None: [re.compile(r"(CHAR|TEXT|CLOB|JSON|UUID)")]}
        BQ_DATA_TYPE_DIC["INTEGER"] = {None: [re.compile(r"INT|SERIAL|YEAR")]}
        BQ_DATA_TYPE_DIC["FLOAT"] = {None: [re.compile(r"(FLOAT|DOUBLE)"), "REAL", "MONEY"]}
        BQ_DATA_TYPE_DIC["DATETIME"] = {
            None: ["DATETIME", "TIMESTAMP", "TIMESTAMP WITHOUT TIME ZONE"],
            self.DATABASE.oracle: ["DATE"]
        }
        BQ_DATA_TYPE_DIC["TIMESTAMP"] = {None: ["TIMESTAMPTZ", "TIMESTAMP WITH TIME ZONE"]}
        BQ_DATA_TYPE_DIC["DATE"] = {None: ["DATE"]}
        BQ_DATA_TYPE_DIC["TIME"] = {None: ["TIME"]}
        BQ_DATA_TYPE_DIC["BOOLEAN"] = {None: [re.compile(r"BOOL")]}

        for bq_type, conditions in BQ_DATA_TYPE_DIC.items():
            for source_db, source_datatypes in conditions.items():
                for source_datatype in source_datatypes:

                    if isinstance(source_datatype, str):
                        if self._data_type == source_datatype \
                            and (  self._source_database == source_db
                                or (self._source_database is not None and source_db is None)):
                            return bq_type

                    elif re.search(source_datatype, self._data_type) \
                        and (  self._source_database == source_db
                            or (self._source_database is not None and source_db is None)):
                        return bq_type

        if self._data_type in ["NUMERIC", "NUMBER", "DECIMAL"]:
            if self._scale is not None:
                return "FLOAT"

            if self._data_type == "NUMBER" \
                and self._source_database == self.DATABASE.oracle \
                and self._length is None:
                return "FLOAT"

            return "INTEGER"

        raise ValueError("Unknown data type : '{}'".format(self._data_type))

    @property
    def bigquery_legacy_data_type(self):
        """Get BigQuery Legacy SQL data type"""

        return self.bigquery_data_type

    @property
    def bigquery_standard_data_type(self):
        """Get BigQuery Standard SQL data type"""

        legacy_data_type = self.bigquery_data_type

        if legacy_data_type == "INTEGER":
            return "INT64"
        elif legacy_data_type == "FLOAT":
            return "FLOAT64"
        elif legacy_data_type == "BOOLEAN":
            return "BOOL"

        return legacy_data_type

    @property
    def bigquery_mode(self):
        """Get BigQuery constraint"""

        if self.array_dimensional > 0:
            return "REPEATED"
        elif self.not_null:
            return "REQUIRED"
        else:
            return "NULLABLE"

    def to_bigquery_field(self, name_case=DdlParseBase.NAME_CASE.original):
        """Generate BigQuery JSON field define"""

        col_name = self.get_name(name_case)
        mode = self.bigquery_mode

        if self.array_dimensional <= 1:
            # no or one dimensional array data type
            type = self.bigquery_legacy_data_type

        else:
            # multiple dimensional array data type
            type = "RECORD"

            fields = OrderedDict()
            fields_cur = fields

            for i in range(1, self.array_dimensional):
                is_last = True if i == self.array_dimensional - 1 else False

                fields_cur['fields'] = [OrderedDict()]
                fields_cur = fields_cur['fields'][0]

                fields_cur['name'] = "dimension_{}".format(i)
                fields_cur['type'] = self.bigquery_legacy_data_type if is_last else "RECORD"
                fields_cur['mode'] = self.bigquery_mode if is_last else "REPEATED"

        col = OrderedDict()
        col['name'] = col_name
        col['type'] = type
        col['mode'] = mode
        if self.description is not None:
            col['description'] = self.description
        if self.array_dimensional > 1:
            col['fields'] = fields['fields']

        return json.dumps(col, ensure_ascii=False)


class DdlParseColumnDict(OrderedDict, DdlParseBase):
    """
    Columns dictionary collection

    * Orderd dictionary
    * Dict with case insensitive keys
      (SQL is case insensitive)
    """

    def __init__(self, source_database=None):
        super().__init__()
        self.source_database = source_database

    def __getitem__(self, key):
        return super().__getitem__(key.lower())

    def __setitem__(self, key, value):
        super().__setitem__(key.lower(), value)

    def append(self, column_name, data_type_array=None, array_brackets=None, constraint=None, source_database=None):
        if source_database is None:
            source_database = self.source_database

        column = DdlParseColumn(column_name, data_type_array, array_brackets, constraint, source_database)
        self.__setitem__(column_name, column)
        return column

    def to_bigquery_fields(self, name_case=DdlParseBase.NAME_CASE.original):
        """
        Generate BigQuery JSON fields define

        :param name_case: name case type
            * DdlParse.NAME_CASE.original : Return to no convert
            * DdlParse.NAME_CASE.lower : Return to lower
            * DdlParse.NAME_CASE.upper : Return to upper

        :return: BigQuery JSON fields define
        """

        bq_fields = []

        for col in self.values():
            bq_fields.append(col.to_bigquery_field(name_case))

        return "[{}]".format(",".join(bq_fields))


class DdlParseTable(DdlParseTableColumnBase):
    """Table define info"""

    def __init__(self, source_database=None):
        super().__init__(source_database)
        self._schema = None
        self._columns = DdlParseColumnDict(source_database)

    @property
    def source_database(self):
        """
        Source database option

        :param source_database: enum DdlParse.DATABASE
        """
        return super().source_database

    @source_database.setter
    def source_database(self, source_database):
        super(self.__class__, self.__class__).source_database.__set__(self, source_database)
        self._columns.source_database = source_database

    @property
    def is_temp(self):
        """Temporary Table Flag"""
        return self._is_temp

    @is_temp.setter
    def is_temp(self, flag):
        self._is_temp = flag

    @property
    def schema(self):
        """Schema name"""
        return self._schema

    @schema.setter
    def schema(self, schema):
        self._schema = schema

    @property
    def columns(self):
        """DdlParseColumn dictionary collection"""
        return self._columns

    def to_bigquery_fields(self, name_case=DdlParseBase.NAME_CASE.original):
        """
        Generate BigQuery JSON fields define

        :param name_case: name case type
            * DdlParse.NAME_CASE.original : Return to no convert
            * DdlParse.NAME_CASE.lower : Return to lower
            * DdlParse.NAME_CASE.upper : Return to upper

        :return: BigQuery JSON fields define
        """

        return self._columns.to_bigquery_fields(name_case)

    def to_bigquery_ddl(self, name_case=DdlParseBase.NAME_CASE.original):
        """
        Generate BigQuery CREATE TABLE statements

        :param name_case: name case type
            * DdlParse.NAME_CASE.original : Return to no convert
            * DdlParse.NAME_CASE.lower : Return to lower
            * DdlParse.NAME_CASE.upper : Return to upper

        :return: BigQuery CREATE TABLE statements
        """

        if self.schema is None:
            dataset = "dataset"
        elif name_case == self.NAME_CASE.lower:
            dataset = self.schema.lower()
        elif name_case == self.NAME_CASE.upper:
            dataset = self.schema.upper()
        else:
            dataset = self.schema

        cols_defs = []
        for col in self.columns.values():
            col_name = col.get_name(name_case)

            if col.array_dimensional < 1:
                # no array data type
                type = col.bigquery_standard_data_type
                not_null = " NOT NULL" if col.not_null else ""

            else:
                # one or multiple dimensional array data type
                type_front = "ARRAY<"
                type_back = ">"
                for i in range(1, col.array_dimensional):
                    type_front += "STRUCT<dimension_{} ARRAY<".format(i)
                    type_back += ">>"

                type = "{}{}{}".format(type_front, col.bigquery_standard_data_type, type_back)
                not_null = ""

            # cols_defs.append("{name} {type}{not_null}".format(
            cols_defs.append("{name} {type}{not_null}{description}".format(
                name=col_name,
                type=type,
                not_null=not_null,
                description=' OPTIONS (description = "{}")'.format(col.description.replace('"', '\\"')) if col.description is not None else "",
            ))

        return textwrap.dedent(
            """\
            #standardSQL
            CREATE TABLE `project.{dataset}.{table}`
            (
              {colmns_define}
            )""").format(
            dataset=dataset,
            table=self.get_name(name_case),
            colmns_define=",\n  ".join(cols_defs),
        )


class DdlParse(DdlParseBase):
    """DDL parser"""

    _LPAR, _RPAR, _COMMA, _SEMICOLON, _DOT, _DOUBLEQUOTE, _BACKQUOTE, _SPACE = map(Suppress, "(),;.\"` ")
    _CREATE, _TABLE, _TEMP, _CONSTRAINT, _NOT_NULL, _PRIMARY_KEY, _UNIQUE, _UNIQUE_KEY, _FOREIGN_KEY, _REFERENCES, _KEY, _CHAR_SEMANTICS, _BYTE_SEMANTICS = \
        map(CaselessKeyword, "CREATE, TABLE, TEMP, CONSTRAINT, NOT NULL, PRIMARY KEY, UNIQUE, UNIQUE KEY, FOREIGN KEY, REFERENCES, KEY, CHAR, BYTE".replace(", ", ",").split(","))
    _SUPPRESS_QUOTE = _BACKQUOTE | _DOUBLEQUOTE

    _COMMENT = Suppress("--" + Regex(r".+"))

    _CREATE_TABLE_STATEMENT = Suppress(_CREATE) + Optional(_TEMP)("temp") + Suppress(_TABLE) + Optional(Suppress(CaselessKeyword("IF NOT EXISTS"))) \
        + Optional(_SUPPRESS_QUOTE) + Optional(Word(alphanums+"_")("schema") + Optional(_SUPPRESS_QUOTE) + _DOT + Optional(_SUPPRESS_QUOTE)) + Word(alphanums+"_<>")("table") + Optional(_SUPPRESS_QUOTE) \
        + _LPAR \
        + delimitedList(
            OneOrMore(
                _COMMENT
                |
                # Ignore Index
                Suppress(_KEY + Word(alphanums+"_'`() "))
                |
                Group(
                    Optional(Suppress(_CONSTRAINT) + Optional(_SUPPRESS_QUOTE) + Word(alphanums+"_")("name") + Optional(_SUPPRESS_QUOTE))
                    + (
                        (
                            (_PRIMARY_KEY ^ _UNIQUE ^ _UNIQUE_KEY ^ _NOT_NULL)("type")
                            + Optional(_SUPPRESS_QUOTE) + Optional(Word(alphanums+"_"))("name") + Optional(_SUPPRESS_QUOTE)
                            + _LPAR + Group(delimitedList(Optional(_SUPPRESS_QUOTE) + Word(alphanums+"_") + Optional(_SUPPRESS_QUOTE)))("constraint_columns") + _RPAR
                        )
                        |
                        (
                            (_FOREIGN_KEY)("type")
                            + _LPAR + Group(delimitedList(Optional(_SUPPRESS_QUOTE) + Word(alphanums+"_") + Optional(_SUPPRESS_QUOTE)))("constraint_columns") + _RPAR
                            + Optional(Suppress(_REFERENCES)
                                + Optional(_SUPPRESS_QUOTE) + Word(alphanums+"_")("references_table") + Optional(_SUPPRESS_QUOTE)
                                + _LPAR + Group(delimitedList(Optional(_SUPPRESS_QUOTE) + Word(alphanums+"_") + Optional(_SUPPRESS_QUOTE)))("references_columns") + _RPAR
                            )
                        )
                    )
                )("constraint")
                |
                Group(
                    Optional(_SUPPRESS_QUOTE) + Word(alphanums+"_")("name") + Optional(_SUPPRESS_QUOTE)
                    + Group(
                          Word(alphanums+"_")
                        + Optional(CaselessKeyword("WITHOUT TIME ZONE") ^ CaselessKeyword("WITH TIME ZONE") ^ CaselessKeyword("PRECISION") ^ CaselessKeyword("VARYING"))
                        + Optional(_LPAR + Regex(r"\d+\s*,*\s*\d*") + Optional(Suppress(_CHAR_SEMANTICS | _BYTE_SEMANTICS)) + _RPAR)
                        )("type")
                    + Optional(Word(r"\[\]"))("array_brackets")
                    + Optional(Regex(r"(?!--)(\b(COMMENT|DEFAULT)\b\s+[^,]+|([A-Za-z0-9_'\": -]|[^\x01-\x7E])*)", re.IGNORECASE))("constraint")
                )("column")
                |
                _COMMENT
            )
        )("columns")

    _DDL_PARSE_EXPR = Forward()
    _DDL_PARSE_EXPR << OneOrMore(_COMMENT | _CREATE_TABLE_STATEMENT)


    def __init__(self, ddl=None, source_database=None):
        super().__init__(source_database)
        self._ddl = ddl
        self._table = DdlParseTable(source_database)

    @property
    def source_database(self):
        """
        Source database option

        :param source_database: enum DdlParse.DATABASE
        """
        return super().source_database

    @source_database.setter
    def source_database(self, source_database):
        super(self.__class__, self.__class__).source_database.__set__(self, source_database)
        self._table.source_database = source_database

    @property
    def ddl(self):
        """DDL script"""
        return self._ddl

    @ddl.setter
    def ddl(self, ddl):
        self._ddl = ddl

    def parse(self, ddl=None, source_database=None):
        """
        Parse DDL script.

        :param ddl: DDL script
        :return: DdlParseTable, Parsed table define info.
        """

        if ddl is not None:
            self._ddl = ddl

        if source_database is not None:
            self.source_database = source_database

        if self._ddl is None:
            raise ValueError("DDL is not specified")

        ret = self._DDL_PARSE_EXPR.parseString(self._ddl)
        # print(ret.dump())

        if "schema" in ret:
            self._table.schema = ret["schema"]

        self._table.name = ret["table"]
        self._table.is_temp = True if "temp" in ret else False

        for ret_col in ret["columns"]:

            if ret_col.getName() == "column":
                # add column
                col = self._table.columns.append(
                    column_name=ret_col["name"],
                    data_type_array=ret_col["type"],
                    array_brackets=ret_col['array_brackets'] if "array_brackets" in ret_col else None,
                    constraint=ret_col['constraint'] if "constraint" in ret_col else None)

            elif ret_col.getName() == "constraint":
                # set column constraint
                for col_name in ret_col["constraint_columns"]:
                    col = self._table.columns[col_name]

                    if ret_col["type"] == "PRIMARY KEY":
                        col.not_null = True
                        col.primary_key = True
                    elif ret_col["type"] in ["UNIQUE", "UNIQUE KEY"]:
                        col.unique = True
                    elif ret_col["type"] == "NOT NULL":
                        col.not_null = True

        return self._table

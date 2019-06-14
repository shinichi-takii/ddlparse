# Changelog

## [1.3.0] - 2019-06-15
### Added
- Add supports the parse of `COMMENT` statements.
- Add `DdlParseColumn.comment` property.
- Add `DdlParseColumn.description` property for the `DdlParseColumn.comment` property alias.
- Add supports column comments in BigQuery DDL.

## [1.2.3] - 2019-02-17
- Fix parse error for MySQL DDL with 'FOREIGN KEY'.
- Fix not completely parsed with block comments.

## [1.2.2] - 2019-02-02
- Fix FutureWarning of Python 3.7.
- Add supports PostgreSQL data type.
    - `UUID`
- Fix parse `DEFAULT` value.
    - Add parse regex of `DEFAULT` value.

## [1.2.1] - 2019-01-27
- Add supports for Python 3.7.
    - Pass Python 3.7 test.
- Add supports PostgreSQL data type.
    - `CHARACTER VARYING`
    - `JSON`
    - Array type
- Fix parse `DEFAULT` value.
    - Add decimal point to `DEFAULT` parse character.

## [1.2.0] - 2019-01-02
- Add `DdlParseTable.to_bigquery_ddl` function.
    - BigQuery DDL (CREATE TABLE) statement generate function.
- Add `DdlParseColumn.bigquery_legacy_data_type` property.
    - Get BigQuery Legacy SQL data property.
    - Alias of `DdlParseColumn.bigquery_data_type` property.
- Add `DdlParseColumn.bigquery_standard_data_type` property.
    - Get BigQuery Standard SQL data property.

## [1.1.3] - 2018-08-02
- Add support inline comment.
- Add support constraint name with quotes.
- Add support Oracle Length Semantics for Character Datatypes.

## [1.1.2] - 2018-03-25
- Add support Oracle data type.
    - `CLOB`, `NCLOB`
    - `NUMBER` with no length & scale specification
- Miner fix.

## [1.1.1] - 2018-03-25
- Fix Postgres/Redshift parse of "::" syntax in field attribute.

## [1.1.0] - 2018-01-14
- Add `source_database` option.
- Add `to_bigquery_fields` method to Columns dicttionary(`DdlParseColumnDict` class).
- Fix BigQuery convert of Oracle data type.
    - Oracle 'DATE' -> BigQuery 'DATETIME'
    - Oracle 'NUMBER' -> BigQuery 'INTEGER' or 'FLOAT'

## [1.0.2] - 2018-01-09
- Miner enhancement.
    - `ddlparse.py` : Exclude unused module.
    - `example.py` : Modified comment.
    - `README.md` : Miner fix.

## [1.0.1] - 2018-01-07
- Miner enhancement.

## [1.0.0]
- Initial released.

[1.3.0]: https://github.com/shinichi-takii/ddlparse/compare/v1.2.3...v1.3.0
[1.2.3]: https://github.com/shinichi-takii/ddlparse/compare/v1.2.2...v1.2.3
[1.2.2]: https://github.com/shinichi-takii/ddlparse/compare/v1.2.1...v1.2.2
[1.2.1]: https://github.com/shinichi-takii/ddlparse/compare/v1.2.0...v1.2.1
[1.2.0]: https://github.com/shinichi-takii/ddlparse/compare/v1.1.3...v1.2.0
[1.1.3]: https://github.com/shinichi-takii/ddlparse/compare/v1.1.2...v1.1.3
[1.1.2]: https://github.com/shinichi-takii/ddlparse/compare/v1.1.1...v1.1.2
[1.1.1]: https://github.com/shinichi-takii/ddlparse/compare/v1.1.0...v1.1.1
[1.1.0]: https://github.com/shinichi-takii/ddlparse/compare/1.0.2...v1.1.0
[1.0.2]: https://github.com/shinichi-takii/ddlparse/compare/1.0.1...1.0.2
[1.0.1]: https://github.com/shinichi-takii/ddlparse/compare/1.0.0...1.0.1
[1.0.0]: https://github.com/shinichi-takii/ddlparse/releases/tag/1.0.0

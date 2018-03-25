# Changelog

## 1.1.1
- Fix Postgres/Redshift parse of "::" syntax in field attribute.

## 1.1.0
- Add `source_database` option.
- Add `to_bigquery_fields` method to Columns dicttionary(`DdlParseColumnDict` class).
- Fix BigQuery convert of Oracle data type.
  - Oracle 'DATE' -> BigQuery 'DATETIME'
  - Oracle 'NUMBER' -> BigQuery 'INTEGER' or 'FLOAT'

## 1.0.2
- Miner enhancement.
    - `ddlparse.py` : Exclude unused module.
    - `example.py` : Modified comment.
    - `README.md` : Miner fix.

## 1.0.1
- Miner enhancement.

## 1.0.0
- Initial released.

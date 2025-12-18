# Naming Conventions

This document outlines the naming conventions used for schemas, tables, views, columns, and other objects in the data warehouse.

---

## Table of Contents

1. [General Principles](#general-principles)
2. [Table Naming Conventions](#table-naming-conventions)
   - [Bronze Rules](#bronze-rules)
   - [Silver Rules](#silver-rules)
   - [Gold Rules](#gold-rules)
3. [Column Naming Conventions](#column-naming-conventions)
   - [Surrogate Keys](#surrogate-keys)
   - [Technical Columns](#technical-columns)
4. [Stored Procedures](#stored-procedures)

---

## General Principles

- **Naming Style**: Use `snake_case`, with lowercase letters and underscores (`_`) to separate words.
- **Language**: Use English for all object names.
- **Reserved Words**: Do not use SQL reserved keywords as object names.
- **Clarity**: Names must be descriptive, meaningful, and self-explanatory.
- **Consistency**: Follow the same naming pattern across all layers.

---

## Table Naming Conventions

### Bronze Rules
All bronze tables must start with the source system name,and all the name must match the source name without renaming.
`<sourcesytem>_<entity>`
 * `<sourcesytem>`: name of the source system(e.g crm, erp).
 * `<entity>`: exact name of the table in the source system.
 * Example: crm_customer_info → Customer information from the CRM system.

### Silver Rules
All silver tables must start with the source system name,and all the name must match the source name without renaming.
`<sourcesytem>_<entity>`
 * `<sourcesytem>`: name of the source system(e.g crm, erp).
 * `<entity>`: exact name of the table in the source system.
 * Example: crm_customer_info → Customer information from the CRM system.

### Gold Rules
All names must use meaningful, business-aligned names for tables starting with the category prefix
`<category>_<entity>`
* `<category>`: Describes the role of the table, such as dim (dimension) or fact (fact table).
* `<entity>` : Descriptive name of the table, aligned with the business domain (e.g.,  customers, products, sales).
* Examples
  * dim_customers → Dimension table for customers
  * fact_sales → Fact table containing sales transactions

#### Glossary of category patterns
| Pattern  | Meaning         | Example(s)                               |
|---------|-----------------|-------------------------------------------|
| dim_    | Dimension table | `dim_customer`, `dim_product`             |
| fact_   | Fact table      | `fact_sales`                              |
| report_ | Report table    | `report_sales_monthly`                    |

#### Column Naming Conventions
#### Surrogaate keys
All primary keys in dimension tables must use the suffix_key.
`<tablename>_<key>`
* `<table_name>`: Refers to the name of the table or entity the key belongs to
* `<_key>`: A suffix indicating that this column is a surrogate key.
* Example: customer_key → surrogate key in dim_customers table.

#### Technical columns
All technical columns must start with the prefix dh_. followed by a descriptive name indicating the column's purpose.
`dwh_<column_name>`
* `dwh`: Prefix exclusively for system-generated metadata.
* `<column_name>`: Descriptive name indicating the column's purpose.
* Example: dwh_load_date → systen-generated column used to store the date when the record was loaded.

Technical Columns
All technical columns must start with the prefix dwh_, followed by a descriptive name indicating the column's purpose.
`dwh_<column_name>`
* dwh: Prefix exclusively for system-generated metadata.
* <column_name>: Descriptive name indicating the column's purpose.
* Example: dwh_load_date → System-generated column used to store the date when the record was loaded.

Stored Procedure
All stored procedures used for loading data must follow the naming pattern:
`load_<layer>.`
  * `<layer>`: Represents the layer being loaded, such as bronze,silver.
  * Example
    * load_bronze → Stored procedure for loading data into the Bronze layer.
    * load_silver → Stored procedure for loading data into the Silver layer.

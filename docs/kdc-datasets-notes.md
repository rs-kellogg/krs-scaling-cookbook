# Kellogg Data Cloud

:::{admonition} Data Workflow
:class: note

```{figure} ./images/data-workflow-kdc.png
---
width: 900px
name: data-workflow-kdc
---
```
:::

## Accessing Datasets

:::{admonition} [KDC](https://www.kellogg.northwestern.edu/academics-research/research-support/computing/kellogg-data-cloud.aspx)
:class: note

<!-- ```{figure} ./images/KDC-1.png
---
width: 900px
name: KDC-1
---
``` -->

```{figure} ./images/KDC-2.png
---
width: 900px
name: KDC-2
---
```

Use your netid to log in on the [Northwestern AWS Access Portal](https://nu-sso.awsapps.com/start/#/?tab=accounts)
:::


:::{admonition} Numerator Dataset
:class: note
For this workshop, we will be exploring consumer panel data in the [Numerator](https://www.numerator.com/) dataset

```{figure} ./images/numerator-website.png
---
width: 900px
name: numerator-website
---
```
:::


:::{admonition} Numerator dataset on KDC and KLC
```{figure} ./images/numerator-s3.png
---
width: 900px
name: numerator-s3
---
```
:::

## Writing SQL Queries

<!-- - KDC intro and access (videos)
- SQL introduction (videos, pointers)
- Efficient queries (partitions, select columns, joins)
- Leveraging partitions for efficiency -->

:::{admonition} Basic structure of a SQL statement
```sql
SELECT <column expr>        -- required
FROM <table expr>           -- required
JOIN <another table>        -- optional
WHERE <boolean condition>   -- optional
GROUP BY <columns>          -- optional
ORDER BY <columns>          -- optional
;
```
[Northwestern Research Computing Guide to SQL](https://sites.northwestern.edu/researchcomputing/resource-guides/resource-guide-sql/)
:::

:::{admonition} Athena Query Console
:class: note
```{figure} ./images/numerator-athena-1.png
---
width: 900px
name: numerator-athena-1
---
```
:::


:::{admonition} SQL numerator example: counting rows
```sql
SELECT COUNT(*) 
FROM standard_nmr_feed_fact_table
; -- As of 2024-06-19: 5444236053
```
Response shows there are approximately 5.4 billion rows in this table
:::

:::{important} 
The structure of your SQL query can dramatically impact efficiency. For large datasets a well structured query can dramatically improve speed. Sometimes it can make the difference between success and failure.
:::

:::{admonition} Which query is better?
*Goal: select all users and their postal codes*

```sql
SELECT *
FROM standard_nmr_feed_person_table
;
```

```sql
SELECT user_id, postal_code
FROM standard_nmr_feed_person_table
;
```
:::

:::{admonition} How much data will be scanned?
*Goal: select all items from the "cheese" category*

```sql
SELECT *
FROM standard_nmr_feed_item_table
WHERE majorcat_id = 'isc_gro_dai_cheese'
;
```
:::


:::{admonition} Which query is better?
*Goal: select all users who have bought items from the "cheese" category*

```sql
SELECT DISTINCT FACT.user_id
FROM standard_nmr_feed_fact_table AS FACT
INNER JOIN
    (SELECT *
    FROM standard_nmr_feed_item_table
    WHERE majorcat_id = 'isc_gro_dai_cheese') AS ITEM
ON ITEM.item_id = FACT.item_id
;
```

```sql
SELECT FACT.*
FROM standard_nmr_feed_fact_table AS FACT
INNER JOIN
    (SELECT *
    FROM standard_nmr_feed_item_table) AS ITEM
ON ITEM.item_id = FACT.item_id
WHERE ITEM.majorcat_id = 'isc_gro_dai_cheese'
;
```
:::

:::{admonition} What is wrong with this query?
*Goal: select all purchase facts*

```sql
SELECT *
FROM standard_nmr_feed_fact_table
;
```
:::

## Automation with Scripting

- Athena CLI
- Python SDK
- Command line arguments
- Testing and logging
- Parallelizing Athena queries


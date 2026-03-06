# NHS Prescription Data Schema

## Source

NHS BSA Open Data Portal: https://opendata.nhsbsa.net/dataset/english-prescribing-data-epd

Each monthly file (EPD_YYYYMM.csv) contains ~18 million rows of prescription data.

## PRESCRIPTIONS table

After running `ingest.py finalize`, the main table has these columns:

- `PRACTICE` (VARCHAR) - GP practice code
- `BNF_CODE` (VARCHAR) - British National Formulary drug code
- `BNF_NAME` (VARCHAR) - Drug name/description
- `ITEMS` (DECIMAL) - Number of prescription items
- `NIC` (DECIMAL) - Net ingredient cost
- `ACT_COST` (DECIMAL) - Actual cost
- `QUANTITY` (DECIMAL) - Quantity prescribed
- `PERIOD` (DECIMAL) - Reporting period as YYYYMM (e.g. 202506)
- `POSTCODE` (VARCHAR) - Practice postcode

## Challenge queries

Top 3 most prescribed chemicals in East Central London (EC postcodes):

```sql
SELECT BNF_NAME, SUM(ITEMS) AS total
FROM PRESCRIPTIONS
WHERE POSTCODE LIKE 'EC%'
GROUP BY BNF_NAME
ORDER BY total DESC
LIMIT 3
```

Year with most prescriptions of the top chemical:

```sql
SELECT FLOOR(PERIOD / 100) AS yr, SUM(ITEMS) AS total
FROM PRESCRIPTIONS
WHERE POSTCODE LIKE 'EC%' AND BNF_NAME = '<top chemical>'
GROUP BY FLOOR(PERIOD / 100)
ORDER BY total DESC
LIMIT 1
```

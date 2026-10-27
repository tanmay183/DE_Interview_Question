# 200 PySpark Coding Interview Questions (Data Engineer, ~2 Years Experience)

Commonly asked patterns in product and service based companies (TCS, Infosys, Accenture, Cognizant, Capgemini, Amazon, Walmart, Flipkart, Uber, etc.).

## Setup (used in every question)

```python
from pyspark.sql import SparkSession, functions as F, Window
from pyspark.sql.types import *

spark = SparkSession.builder.appName("interview").getOrCreate()

# Every "Input" table below is created like this:
# df = spark.createDataFrame([(1, "Amit", "IT", 50000), ...], ["emp_id", "name", "dept", "salary"])
```

## Sections

| Section | Questions |
|---|---|
| 1. DataFrame basics and transformations | Q1 - Q25 |
| 2. Aggregations and GroupBy | Q26 - Q50 |
| 3. Joins | Q51 - Q75 |
| 4. Window functions | Q76 - Q110 |
| 5. Deduplication, CDC, data quality, dates | Q111 - Q135 |
| 6. Strings, arrays, maps, JSON | Q136 - Q160 |
| 7. Pivot, gaps and islands, puzzles | Q161 - Q185 |
| 8. Performance, I/O and ETL | Q186 - Q200 |

---

# Section 1: DataFrame Basics and Transformations

### Q1. Select and rename columns
**Problem:** Select `name` and `salary`, and show salary as `monthly_salary`.

**Input: `employees`**
| emp_id | name | dept | salary |
|---|---|---|---|
| 1 | Amit | IT | 50000 |
| 2 | Riya | HR | 40000 |

**Solution**
```python
# select() picks columns, alias() renames the column in the output
df.select("name", F.col("salary").alias("monthly_salary")).show()
```

**Output**
| name | monthly_salary |
|---|---|
| Amit | 50000 |
| Riya | 40000 |

---

### Q2. Filter with multiple conditions
**Problem:** Get IT employees earning more than 45000.

**Input: `employees`**
| emp_id | name | dept | salary |
|---|---|---|---|
| 1 | Amit | IT | 50000 |
| 2 | Riya | HR | 40000 |
| 3 | John | IT | 60000 |
| 4 | Sara | Sales | 45000 |

**Solution**
```python
# Each condition must be in brackets; & = AND, | = OR, ~ = NOT
df.filter((F.col("dept") == "IT") & (F.col("salary") > 45000)).show()
```

**Output**
| emp_id | name | dept | salary |
|---|---|---|---|
| 1 | Amit | IT | 50000 |
| 3 | John | IT | 60000 |

---

### Q3. Add a derived column
**Problem:** Add `annual_salary` = salary * 12.

**Input: `employees`**
| emp_id | name | salary |
|---|---|---|
| 1 | Amit | 50000 |
| 2 | Riya | 40000 |

**Solution**
```python
# withColumn adds a new column (or replaces one with the same name)
df.withColumn("annual_salary", F.col("salary") * 12).show()
```

**Output**
| emp_id | name | salary | annual_salary |
|---|---|---|---|
| 1 | Amit | 50000 | 600000 |
| 2 | Riya | 40000 | 480000 |

---

### Q4. Rename a column
**Problem:** Rename `salary` to `monthly_salary`.

**Input: `employees`**
| emp_id | name | salary |
|---|---|---|
| 1 | Amit | 50000 |

**Solution**
```python
# withColumnRenamed(old, new); use toDF(*names) to rename all columns at once
df.withColumnRenamed("salary", "monthly_salary").show()
```

**Output**
| emp_id | name | monthly_salary |
|---|---|---|
| 1 | Amit | 50000 |

---

### Q5. Drop a column
**Problem:** Remove the `dept` column.

**Input: `employees`**
| emp_id | name | dept |
|---|---|---|
| 1 | Amit | IT |
| 2 | Riya | HR |

**Solution**
```python
# drop() ignores columns that do not exist (no error)
df.drop("dept").show()
```

**Output**
| emp_id | name |
|---|---|
| 1 | Amit |
| 2 | Riya |

---

### Q6. Distinct values of a column
**Problem:** List the unique departments.

**Input: `employees`**
| name | dept |
|---|---|
| Amit | IT |
| Riya | HR |
| John | IT |
| Sara | Sales |

**Solution**
```python
# select the column first, then distinct() (triggers a shuffle)
df.select("dept").distinct().show()
```

**Output**
| dept |
|---|
| IT |
| HR |
| Sales |

---

### Q7. Remove duplicates on selected columns
**Problem:** Keep one row for every (customer, product) pair.

**Input: `orders`**
| order_id | customer | product |
|---|---|---|
| 1 | A | Pen |
| 2 | A | Pen |
| 3 | B | Book |

**Solution**
```python
# dropDuplicates(subset) keeps ONE arbitrary row per subset
# (use row_number() if you need a specific row, see Q94)
df.dropDuplicates(["customer", "product"]).show()
```

**Output**
| order_id | customer | product |
|---|---|---|
| 1 | A | Pen |
| 3 | B | Book |

---

### Q8. Sort by multiple columns
**Problem:** Sort by dept ascending, then salary descending.

**Input: `employees`**
| name | dept | salary |
|---|---|---|
| Amit | IT | 50000 |
| Riya | HR | 40000 |
| John | IT | 60000 |
| Sara | Sales | 45000 |

**Solution**
```python
# orderBy accepts column expressions with asc()/desc()
df.orderBy(F.col("dept").asc(), F.col("salary").desc()).show()
```

**Output**
| name | dept | salary |
|---|---|---|
| Riya | HR | 40000 |
| John | IT | 60000 |
| Amit | IT | 50000 |
| Sara | Sales | 45000 |

---

### Q9. Top N rows
**Problem:** Get the 2 highest paid employees.

**Input: `employees`**
| name | salary |
|---|---|
| Amit | 50000 |
| Riya | 40000 |
| John | 60000 |
| Sara | 45000 |

**Solution**
```python
# sort descending then limit
df.orderBy(F.col("salary").desc()).limit(2).show()
```

**Output**
| name | salary |
|---|---|
| John | 60000 |
| Amit | 50000 |

---

### Q10. Conditional column (CASE WHEN)
**Problem:** Grade: salary >= 55000 High, >= 45000 Medium, else Low.

**Input: `employees`**
| name | salary |
|---|---|
| Amit | 50000 |
| Riya | 40000 |
| John | 60000 |
| Sara | 45000 |

**Solution**
```python
# when().when().otherwise() = CASE WHEN ... ELSE ... END (evaluated top to bottom)
df.withColumn("grade",
    F.when(F.col("salary") >= 55000, "High")
     .when(F.col("salary") >= 45000, "Medium")
     .otherwise("Low")).show()
```

**Output**
| name | salary | grade |
|---|---|---|
| Amit | 50000 | Medium |
| Riya | 40000 | Low |
| John | 60000 | High |
| Sara | 45000 | Medium |

---

### Q11. Replace nulls with a default
**Problem:** Replace null `city` with "Unknown".

**Input: `customers`**
| id | name | city |
|---|---|---|
| 1 | Amit | Pune |
| 2 | Riya | null |
| 3 | John | null |

**Solution**
```python
# fillna with a dict lets you set different defaults per column
df.fillna({"city": "Unknown"}).show()
```

**Output**
| id | name | city |
|---|---|---|
| 1 | Amit | Pune |
| 2 | Riya | Unknown |
| 3 | John | Unknown |

---

### Q12. Drop rows with nulls
**Problem:** Remove rows where `name` is null.

**Input: `employees`**
| id | name | salary |
|---|---|---|
| 1 | Amit | 50000 |
| 2 | null | 40000 |
| 3 | John | null |

**Solution**
```python
# subset restricts the null check to given columns
# dropna(how="any") with no subset would also drop row 3
df.dropna(subset=["name"]).show()
```

**Output**
| id | name | salary |
|---|---|---|
| 1 | Amit | 50000 |
| 3 | John | null |

---

### Q13. First non-null value with coalesce
**Problem:** Pick `mobile`, else `alt_mobile`, else "NA".

**Input: `contacts`**
| id | mobile | alt_mobile |
|---|---|---|
| 1 | null | 9811 |
| 2 | 9722 | null |
| 3 | null | null |

**Solution**
```python
# coalesce returns the first non-null argument
df.select("id", F.coalesce("mobile", "alt_mobile", F.lit("NA")).alias("contact")).show()
```

**Output**
| id | contact |
|---|---|
| 1 | 9811 |
| 2 | 9722 |
| 3 | NA |

---

### Q14. Cast data types
**Problem:** Convert `amount` (string) to double and `order_date` (string) to date.

**Input: `orders`**
| id | amount | order_date |
|---|---|---|
| 1 | "100.5" | "2024-01-15" |
| 2 | "200" | "2024-02-20" |

**Solution**
```python
# cast() with a type name; to_date() for strings with a known format
df2 = (df.withColumn("amount", F.col("amount").cast("double"))
         .withColumn("order_date", F.to_date("order_date", "yyyy-MM-dd")))
df2.printSchema()
```

**Output** (schema)
| column | type |
|---|---|
| id | long |
| amount | double |
| order_date | date |

---

### Q15. IN and NOT IN
**Problem:** Employees in IT or HR, then employees not in IT or HR.

**Input: `employees`**
| name | dept |
|---|---|
| Amit | IT |
| Riya | HR |
| John | IT |
| Sara | Sales |

**Solution**
```python
df.filter(F.col("dept").isin("IT", "HR")).show()     # IN
df.filter(~F.col("dept").isin("IT", "HR")).show()    # NOT IN
```

**Output** (IN)
| name | dept |
|---|---|
| Amit | IT |
| Riya | HR |
| John | IT |

**Output** (NOT IN)
| name | dept |
|---|---|
| Sara | Sales |

---

### Q16. BETWEEN and LIKE
**Problem:** Salary between 40000 and 50000 (inclusive) and name starting with 'A'.

**Input: `employees`**
| name | salary |
|---|---|
| Amit | 50000 |
| Riya | 40000 |
| Arun | 70000 |

**Solution**
```python
# between is inclusive on both ends; like uses % wildcard
df.filter(F.col("salary").between(40000, 50000) & F.col("name").like("A%")).show()
```

**Output**
| name | salary |
|---|---|
| Amit | 50000 |

---

### Q17. Count nulls in every column
**Problem:** Show the number of nulls per column.

**Input: `customers`**
| id | name | city |
|---|---|---|
| 1 | Amit | Pune |
| 2 | null | null |
| 3 | John | null |

**Solution**
```python
# count() ignores nulls, so count(when(isNull)) counts only the null rows
df.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in df.columns]).show()
```

**Output**
| id | name | city |
|---|---|---|
| 0 | 1 | 2 |

---

### Q18. Add a constant column
**Problem:** Add a column `country` with value "India".

**Input: `employees`**
| name |
|---|
| Amit |
| Riya |

**Solution**
```python
# lit() wraps a Python literal as a Column
df.withColumn("country", F.lit("India")).show()
```

**Output**
| name | country |
|---|---|
| Amit | India |
| Riya | India |

---

### Q19. Concatenate columns
**Problem:** Create `full_name` from first and last names with a space.

**Input: `people`**
| first | last |
|---|---|
| Amit | Sharma |
| Riya | Singh |

**Solution**
```python
# concat_ws(separator, *cols) skips nulls, concat() returns null if any is null
df.withColumn("full_name", F.concat_ws(" ", "first", "last")).show()
```

**Output**
| first | last | full_name |
|---|---|---|
| Amit | Sharma | Amit Sharma |
| Riya | Singh | Riya Singh |

---

### Q20. Split a column into multiple columns
**Problem:** Split `full_name` into `first_name` and `last_name`.

**Input: `people`**
| full_name |
|---|
| Amit Sharma |
| Riya Singh |

**Solution**
```python
# split() returns an array; getItem(i) picks element i
parts = F.split("full_name", " ")
df.select(parts.getItem(0).alias("first_name"), parts.getItem(1).alias("last_name")).show()
```

**Output**
| first_name | last_name |
|---|---|
| Amit | Sharma |
| Riya | Singh |

---

### Q21. Trim, case and length functions
**Problem:** Clean the name (trim), uppercase it and get its length.

**Input: `people`**
| name |
|---|
| "  Amit " |
| "riya  " |

**Solution**
```python
df.select(
    F.trim("name").alias("trimmed"),
    F.upper(F.trim("name")).alias("upper_name"),
    F.length(F.trim("name")).alias("len")
).show()
```

**Output**
| trimmed | upper_name | len |
|---|---|---|
| Amit | AMIT | 4 |
| riya | RIYA | 4 |

---

### Q22. Extract email domain
**Problem:** Get the domain of each email.

**Input: `users`**
| email |
|---|
| amit@gmail.com |
| riya@yahoo.com |

**Solution**
```python
# substring_index(col, delimiter, -1) = text after the last '@'
df.withColumn("domain", F.substring_index("email", "@", -1)).show()
```

**Output**
| email | domain |
|---|---|
| amit@gmail.com | gmail.com |
| riya@yahoo.com | yahoo.com |

---

### Q23. Row count and distinct count
**Problem:** Find the total rows and the distinct number of departments.

**Input: `employees`**
| name | dept |
|---|---|
| Amit | IT |
| Riya | HR |
| John | IT |
| Sara | Sales |

**Solution**
```python
df.agg(F.count("*").alias("total_rows"),
       F.countDistinct("dept").alias("distinct_depts")).show()
```

**Output**
| total_rows | distinct_depts |
|---|---|
| 4 | 3 |

---

### Q24. Union two DataFrames and remove duplicates
**Problem:** Combine two DataFrames whose columns are in a different order.

**Input: `df1`**
| id | name |
|---|---|
| 1 | Amit |
| 2 | Riya |

**Input: `df2`**
| name | id |
|---|---|
| Riya | 2 |
| John | 3 |

**Solution**
```python
# unionByName matches columns by NAME (union() matches by position, a classic trap)
# union keeps duplicates like UNION ALL, so call distinct() for UNION
df1.unionByName(df2).distinct().show()
```

**Output**
| id | name |
|---|---|
| 1 | Amit |
| 2 | Riya |
| 3 | John |

---

### Q25. Read a CSV with an explicit schema
**Problem:** Read the CSV file below using a defined schema instead of inferSchema.

**Input: `/data/emp.csv`**
```
emp_id,name,salary
1,Amit,50000
2,Riya,40000
```

**Solution**
```python
# Explicit schema is faster (no extra pass over data) and safer than inferSchema
schema = StructType([
    StructField("emp_id", IntegerType(), True),
    StructField("name",   StringType(),  True),
    StructField("salary", DoubleType(),  True),
])
df = spark.read.option("header", True).schema(schema).csv("/data/emp.csv")
df.show()
```

**Output**
| emp_id | name | salary |
|---|---|---|
| 1 | Amit | 50000.0 |
| 2 | Riya | 40000.0 |

---
# Section 2: Aggregations and GroupBy

### Q26. Total salary per department
**Problem:** Find the total salary of each department.

**Input: `employees`**
| emp_id | name | dept | salary |
|---|---|---|---|
| 1 | Amit | IT | 50000 |
| 2 | Riya | HR | 40000 |
| 3 | John | IT | 60000 |
| 4 | Sara | Sales | 45000 |

**Solution**
```python
# groupBy + agg is the standard pattern; always alias aggregate columns
df.groupBy("dept").agg(F.sum("salary").alias("total_salary")).show()
```

**Output**
| dept | total_salary |
|---|---|
| IT | 110000 |
| HR | 40000 |
| Sales | 45000 |

---

### Q27. Employee count per department
**Problem:** Count employees in each department.

**Input: `employees`** (same as Q26)

**Solution**
```python
# count("*") counts all rows; count("col") ignores nulls in that column
df.groupBy("dept").agg(F.count("*").alias("emp_count")).show()
```

**Output**
| dept | emp_count |
|---|---|
| IT | 2 |
| HR | 1 |
| Sales | 1 |

---

### Q28. Average salary per department (rounded)
**Problem:** Find the average salary per department rounded to 2 decimals.

**Input: `employees`**
| name | dept | salary |
|---|---|---|
| Amit | IT | 50000 |
| John | IT | 60000 |
| Riya | HR | 40000 |

**Solution**
```python
df.groupBy("dept").agg(F.round(F.avg("salary"), 2).alias("avg_salary")).show()
```

**Output**
| dept | avg_salary |
|---|---|
| IT | 55000.0 |
| HR | 40000.0 |

---

### Q29. Multiple aggregations together
**Problem:** For each department show min, max, avg salary and headcount.

**Input: `employees`**
| name | dept | salary |
|---|---|---|
| Amit | IT | 50000 |
| John | IT | 60000 |
| Riya | HR | 40000 |
| Sara | Sales | 45000 |

**Solution**
```python
# Many aggregates can be computed in one agg() call (single pass over data)
df.groupBy("dept").agg(
    F.min("salary").alias("min_sal"),
    F.max("salary").alias("max_sal"),
    F.avg("salary").alias("avg_sal"),
    F.count("*").alias("cnt")
).show()
```

**Output**
| dept | min_sal | max_sal | avg_sal | cnt |
|---|---|---|---|---|
| IT | 50000 | 60000 | 55000.0 | 2 |
| HR | 40000 | 40000 | 40000.0 | 1 |
| Sales | 45000 | 45000 | 45000.0 | 1 |

---

### Q30. HAVING: departments with more than 1 employee
**Problem:** Show departments that have more than one employee.

**Input: `employees`**
| name | dept |
|---|---|
| Amit | IT |
| John | IT |
| Riya | HR |

**Solution**
```python
# HAVING in SQL = filter() after groupBy().agg()
(df.groupBy("dept").agg(F.count("*").alias("cnt"))
   .filter(F.col("cnt") > 1).show())
```

**Output**
| dept | cnt |
|---|---|
| IT | 2 |

---

### Q31. Department with the highest total salary
**Problem:** Find the department with the highest total salary.

**Input: `employees`**
| name | dept | salary |
|---|---|---|
| Amit | IT | 50000 |
| John | IT | 60000 |
| Riya | HR | 40000 |
| Sara | Sales | 45000 |

**Solution**
```python
# aggregate -> sort desc -> limit 1
(df.groupBy("dept").agg(F.sum("salary").alias("total"))
   .orderBy(F.col("total").desc()).limit(1).show())
```

**Output**
| dept | total |
|---|---|
| IT | 110000 |

---

### Q32. Distinct count per group
**Problem:** Count distinct products bought by each customer.

**Input: `orders`**
| customer | product |
|---|---|
| A | Pen |
| A | Pen |
| A | Book |
| B | Pen |

**Solution**
```python
# countDistinct counts unique non-null values inside each group
df.groupBy("customer").agg(F.countDistinct("product").alias("distinct_products")).show()
```

**Output**
| customer | distinct_products |
|---|---|
| A | 2 |
| B | 1 |

---

### Q33. Collect values of a group into a list
**Problem:** Create a list of employee names per department.

**Input: `employees`**
| name | dept |
|---|---|
| Amit | IT |
| John | IT |
| Riya | HR |

**Solution**
```python
# collect_list keeps duplicates, collect_set removes them. Order is not guaranteed,
# so wrap in sort_array() if the order matters
df.groupBy("dept").agg(F.sort_array(F.collect_list("name")).alias("names")).show()
```

**Output**
| dept | names |
|---|---|
| IT | [Amit, John] |
| HR | [Riya] |

---

### Q34. Conditional aggregation
**Problem:** For each customer, find the delivered amount and the count of cancelled orders.

**Input: `orders`**
| order_id | customer | status | amount |
|---|---|---|---|
| 1 | A | delivered | 100 |
| 2 | A | cancelled | 50 |
| 3 | B | delivered | 200 |
| 4 | B | delivered | 150 |

**Solution**
```python
# SUM(CASE WHEN ...) pattern, avoids filtering and multiple passes
df.groupBy("customer").agg(
    F.sum(F.when(F.col("status") == "delivered", F.col("amount")).otherwise(0)).alias("delivered_amt"),
    F.sum(F.when(F.col("status") == "cancelled", 1).otherwise(0)).alias("cancelled_cnt")
).show()
```

**Output**
| customer | delivered_amt | cancelled_cnt |
|---|---|---|
| A | 100 | 1 |
| B | 350 | 0 |

---

### Q35. GroupBy on multiple columns
**Problem:** Total quantity sold per region and product.

**Input: `sales`**
| region | product | qty |
|---|---|---|
| East | Pen | 10 |
| East | Pen | 5 |
| East | Book | 7 |
| West | Pen | 3 |

**Solution**
```python
df.groupBy("region", "product").agg(F.sum("qty").alias("total_qty")).show()
```

**Output**
| region | product | total_qty |
|---|---|---|
| East | Pen | 15 |
| East | Book | 7 |
| West | Pen | 3 |

---

### Q36. Monthly sales
**Problem:** Total sales per year-month.

**Input: `sales`**
| order_date | amount |
|---|---|
| 2024-01-05 | 100 |
| 2024-01-20 | 200 |
| 2024-02-10 | 150 |

**Solution**
```python
# date_format converts a date to a string pattern, which can be used as a group key
(df.withColumn("month", F.date_format("order_date", "yyyy-MM"))
   .groupBy("month").agg(F.sum("amount").alias("total_sales"))
   .orderBy("month").show())
```

**Output**
| month | total_sales |
|---|---|
| 2024-01 | 300 |
| 2024-02 | 150 |

---

### Q37. Percentage contribution of each group
**Problem:** Show each department's share of the total salary bill.

**Input: `employees`**
| dept | salary |
|---|---|
| IT | 50000 |
| IT | 60000 |
| HR | 40000 |
| Sales | 45000 |

**Solution**
```python
# Aggregate first, then divide by the grand total using an empty window
# (fine for small aggregated data; for big data use agg().crossJoin(total_df))
w = Window.partitionBy()
(df.groupBy("dept").agg(F.sum("salary").alias("dept_total"))
   .withColumn("pct", F.round(F.col("dept_total") * 100 / F.sum("dept_total").over(w), 2))
   .show())
```

**Output**
| dept | dept_total | pct |
|---|---|---|
| IT | 110000 | 56.41 |
| HR | 40000 | 20.51 |
| Sales | 45000 | 23.08 |

---

### Q38. Employees earning more than their department average
**Problem:** List employees whose salary is greater than their department average.

**Input: `employees`**
| name | dept | salary |
|---|---|---|
| Amit | IT | 50000 |
| John | IT | 60000 |
| Riya | HR | 40000 |
| Sara | Sales | 45000 |

**Solution**
```python
# Compute the average per dept, join back, then filter
avg_df = df.groupBy("dept").agg(F.avg("salary").alias("avg_sal"))
(df.join(avg_df, "dept")
   .filter(F.col("salary") > F.col("avg_sal"))
   .select("name", "dept", "salary").show())
```

**Output**
| name | dept | salary |
|---|---|---|
| John | IT | 60000 |

---

### Q39. Find duplicate values
**Problem:** Find emails that appear more than once.

**Input: `users`**
| id | email |
|---|---|
| 1 | a@x.com |
| 2 | b@x.com |
| 3 | a@x.com |

**Solution**
```python
(df.groupBy("email").agg(F.count("*").alias("cnt"))
   .filter("cnt > 1").show())
```

**Output**
| email | cnt |
|---|---|
| a@x.com | 2 |

---

### Q40. Customers with average order value above 100
**Problem:** Find customers whose average order amount is greater than 100.

**Input: `orders`**
| customer | amount |
|---|---|
| A | 100 |
| A | 200 |
| B | 50 |
| C | 400 |

**Solution**
```python
(df.groupBy("customer").agg(F.avg("amount").alias("avg_amt"))
   .filter(F.col("avg_amt") > 100).show())
```

**Output**
| customer | avg_amt |
|---|---|
| A | 150.0 |
| C | 400.0 |

---

### Q41. Subtotals and grand total with rollup
**Problem:** Total qty per region and product, with region subtotals and a grand total.

**Input: `sales`**
| region | product | qty |
|---|---|---|
| East | Pen | 15 |
| East | Book | 7 |
| West | Pen | 3 |

**Solution**
```python
# rollup(a, b) = (a,b), (a), () subtotals. cube() gives all combinations.
# NULL in the output marks the subtotal level
df.rollup("region", "product").agg(F.sum("qty").alias("total_qty")).orderBy("region", "product").show()
```

**Output**
| region | product | total_qty |
|---|---|---|
| null | null | 25 |
| East | null | 22 |
| East | Book | 7 |
| East | Pen | 15 |
| West | null | 3 |
| West | Pen | 3 |

---

### Q42. Count of non-null values vs total rows per group
**Problem:** For each dept show the number of rows and the number of non-null bonus values.

**Input: `employees`**
| dept | bonus |
|---|---|
| IT | 100 |
| IT | null |
| HR | null |

**Solution**
```python
# count("*") counts rows, count(col) counts only non-null values
df.groupBy("dept").agg(F.count("*").alias("total_rows"),
                       F.count("bonus").alias("bonus_count")).show()
```

**Output**
| dept | total_rows | bonus_count |
|---|---|---|
| IT | 2 | 1 |
| HR | 1 | 0 |

---

### Q43. Latest status per user without a window
**Problem:** Get the status with the highest timestamp for each user.

**Input: `events`**
| user | ts | status |
|---|---|---|
| u1 | 1 | A |
| u1 | 2 | B |
| u2 | 1 | C |

**Solution**
```python
# max(struct(ts, status)) compares fields left to right, so it picks the row
# with the max ts and carries along its status. Avoids a window shuffle.
(df.groupBy("user")
   .agg(F.max(F.struct("ts", "status")).alias("latest"))
   .select("user", "latest.ts", "latest.status").show())
```

**Output**
| user | ts | status |
|---|---|---|
| u1 | 2 | B |
| u2 | 1 | C |

---

### Q44. Median salary (approximate and exact)
**Problem:** Find the median salary.

**Input: `employees`**
| name | salary |
|---|---|
| Riya | 40000 |
| Sara | 45000 |
| Amit | 50000 |
| John | 60000 |

**Solution**
```python
# percentile_approx returns an actual value from the data (fast, scalable)
# percentile() does exact calculation with interpolation (slower)
df.agg(
    F.percentile_approx("salary", 0.5).alias("approx_median"),
    F.expr("percentile(salary, 0.5)").alias("exact_median")
).show()
```

**Output**
| approx_median | exact_median |
|---|---|
| 45000 | 47500.0 |

---

### Q45. Most frequent value (mode)
**Problem:** Find the city that appears most often.

**Input: `customers`**
| name | city |
|---|---|
| Amit | Pune |
| Riya | Pune |
| John | Mumbai |

**Solution**
```python
(df.groupBy("city").count()
   .orderBy(F.col("count").desc()).limit(1).show())
```

**Output**
| city | count |
|---|---|
| Pune | 2 |

---

### Q46. Weighted average
**Problem:** Compute the weighted average price per item using quantity as weight.

**Input: `purchases`**
| item | qty | price |
|---|---|---|
| Pen | 10 | 5 |
| Pen | 30 | 7 |

**Solution**
```python
# weighted avg = sum(qty * price) / sum(qty)
(df.groupBy("item")
   .agg((F.sum(F.col("qty") * F.col("price")) / F.sum("qty")).alias("weighted_avg_price"))
   .show())
```

**Output**
| item | weighted_avg_price |
|---|---|
| Pen | 6.5 |

---

### Q47. Group by salary bands
**Problem:** Count employees in bands: Low (<45000), Mid (45000-55000), High (>55000).

**Input: `employees`**
| name | salary |
|---|---|
| Amit | 50000 |
| Riya | 40000 |
| John | 60000 |
| Sara | 45000 |

**Solution**
```python
# Create the derived column first, then group by it
(df.withColumn("band",
        F.when(F.col("salary") < 45000, "Low")
         .when(F.col("salary") <= 55000, "Mid")
         .otherwise("High"))
   .groupBy("band").count().show())
```

**Output**
| band | count |
|---|---|
| Low | 1 |
| Mid | 2 |
| High | 1 |

---

### Q48. Year-wise revenue
**Problem:** Total revenue per year.

**Input: `orders`**
| order_date | amount |
|---|---|
| 2023-05-01 | 100 |
| 2023-08-01 | 200 |
| 2024-01-01 | 300 |

**Solution**
```python
df.groupBy(F.year("order_date").alias("year")).agg(F.sum("amount").alias("revenue")).orderBy("year").show()
```

**Output**
| year | revenue |
|---|---|
| 2023 | 300 |
| 2024 | 300 |

---

### Q49. Aggregation using a dictionary
**Problem:** Sum salary and count emp_id per department using dict style agg.

**Input: `employees`**
| emp_id | dept | salary |
|---|---|---|
| 1 | IT | 50000 |
| 2 | IT | 60000 |
| 3 | HR | 40000 |

**Solution**
```python
# Dict style is short but output columns are auto-named like sum(salary),
# so prefer the F.xxx().alias() style in production code
df.groupBy("dept").agg({"salary": "sum", "emp_id": "count"}).show()
```

**Output**
| dept | sum(salary) | count(emp_id) |
|---|---|---|
| IT | 110000 | 2 |
| HR | 40000 | 1 |

---

### Q50. Departments where ALL employees earn more than a threshold
**Problem:** Find departments in which every employee earns more than 42000.

**Input: `employees`**
| name | dept | salary |
|---|---|---|
| Amit | IT | 50000 |
| John | IT | 60000 |
| Riya | HR | 40000 |
| Sara | Sales | 45000 |

**Solution**
```python
# "All rows satisfy X" is the same as "min(column) satisfies X"
(df.groupBy("dept").agg(F.min("salary").alias("min_sal"))
   .filter(F.col("min_sal") > 42000).select("dept").show())
```

**Output**
| dept |
|---|
| IT |
| Sales |

---

# Section 3: Joins

### Q51. Inner join
**Problem:** Show employee names with their department names.

**Input: `employees`**
| emp_id | name | dept_id |
|---|---|---|
| 1 | Amit | 10 |
| 2 | Riya | 20 |
| 3 | John | 30 |

**Input: `departments`**
| dept_id | dept_name |
|---|---|
| 10 | IT |
| 20 | HR |
| 40 | Finance |

**Solution**
```python
# Joining on a column NAME keeps a single dept_id column in the output
emp.join(dept, "dept_id", "inner").select("name", "dept_name").show()
```

**Output**
| name | dept_name |
|---|---|
| Amit | IT |
| Riya | HR |

---

### Q52. Left join
**Problem:** Show all employees, with department name if available.

**Input:** same `employees` and `departments` as Q51.

**Solution**
```python
emp.join(dept, "dept_id", "left").select("name", "dept_name").show()
```

**Output**
| name | dept_name |
|---|---|
| Amit | IT |
| Riya | HR |
| John | null |

---

### Q53. Left anti join: customers with no orders
**Problem:** Find customers who never placed an order.

**Input: `customers`**
| cust_id | name |
|---|---|
| 1 | Amit |
| 2 | Riya |
| 3 | John |

**Input: `orders`**
| order_id | cust_id |
|---|---|
| 101 | 1 |
| 102 | 1 |
| 103 | 2 |

**Solution**
```python
# left_anti returns left rows that have NO match on the right (NOT EXISTS / NOT IN)
customers.join(orders, "cust_id", "left_anti").show()
```

**Output**
| cust_id | name |
|---|---|
| 3 | John |

---

### Q54. Left semi join: customers with orders
**Problem:** Find customers who placed at least one order (without duplicating rows).

**Input:** same as Q53.

**Solution**
```python
# left_semi = EXISTS. Returns only left columns, no duplicates even if many matches
customers.join(orders, "cust_id", "left_semi").show()
```

**Output**
| cust_id | name |
|---|---|
| 1 | Amit |
| 2 | Riya |

---

### Q55. Full outer join
**Problem:** Show all employees and all departments, matched where possible.

**Input:** same `employees` and `departments` as Q51.

**Solution**
```python
# full outer keeps unmatched rows from both sides
emp.join(dept, "dept_id", "full").select("dept_id", "name", "dept_name").orderBy("dept_id").show()
```

**Output**
| dept_id | name | dept_name |
|---|---|---|
| 10 | Amit | IT |
| 20 | Riya | HR |
| 30 | John | null |
| 40 | null | Finance |

---

### Q56. Self join: employee and manager
**Problem:** Show each employee with their manager's name.

**Input: `employees`**
| emp_id | name | manager_id |
|---|---|---|
| 1 | Amit | null |
| 2 | Riya | 1 |
| 3 | John | 1 |
| 4 | Sara | 2 |

**Solution**
```python
# Alias the same DataFrame twice, then join employee.manager_id = manager.emp_id
e = df.alias("e"); m = df.alias("m")
(e.join(m, F.col("e.manager_id") == F.col("m.emp_id"), "left")
  .select(F.col("e.name").alias("employee"), F.col("m.name").alias("manager")).show())
```

**Output**
| employee | manager |
|---|---|
| Amit | null |
| Riya | Amit |
| John | Amit |
| Sara | Riya |

---

### Q57. Cross join (all combinations)
**Problem:** Generate every colour and size combination.

**Input: `colors`**
| color |
|---|
| red |
| blue |

**Input: `sizes`**
| size |
|---|
| S |
| M |

**Solution**
```python
# crossJoin = Cartesian product (rows_a x rows_b), use deliberately
colors.crossJoin(sizes).show()
```

**Output**
| color | size |
|---|---|
| red | S |
| red | M |
| blue | S |
| blue | M |

---

### Q58. Join on differently named columns
**Problem:** Join orders (`cust_id`) with customers (`id`) without keeping a duplicate key column.

**Input: `orders`**
| order_id | cust_id |
|---|---|
| 101 | 1 |
| 102 | 2 |

**Input: `customers`**
| id | name |
|---|---|
| 1 | Amit |
| 2 | Riya |

**Solution**
```python
# Explicit condition keeps BOTH key columns, so drop one afterwards
orders.join(customers, orders.cust_id == customers.id, "inner").drop(customers.id).show()
```

**Output**
| order_id | cust_id | name |
|---|---|---|
| 101 | 1 | Amit |
| 102 | 2 | Riya |

---

### Q59. Broadcast join hint
**Problem:** Join a large orders table with a small countries lookup efficiently.

**Input: `orders`**
| order_id | country_code |
|---|---|
| 1 | IN |
| 2 | US |

**Input: `countries`** (small)
| country_code | country |
|---|---|
| IN | India |
| US | USA |

**Solution**
```python
# broadcast() ships the small table to every executor, so the large table is NOT shuffled.
# Spark does this automatically below spark.sql.autoBroadcastJoinThreshold (10 MB default)
orders.join(F.broadcast(countries), "country_code").show()
```

**Output**
| country_code | order_id | country |
|---|---|---|
| IN | 1 | India |
| US | 2 | USA |

---

### Q60. Records in A but not in B
**Problem:** Find ids present in table A but missing in table B.

**Input: `A`**
| id |
|---|
| 1 |
| 2 |
| 3 |

**Input: `B`**
| id |
|---|
| 2 |
| 3 |
| 4 |

**Solution**
```python
# subtract = EXCEPT DISTINCT; exceptAll keeps duplicates; left_anti is the join alternative
A.subtract(B).show()
```

**Output**
| id |
|---|
| 1 |

---

### Q61. Common records between two tables
**Problem:** Find ids present in both A and B.

**Input:** same `A` and `B` as Q60.

**Solution**
```python
# intersect = INTERSECT DISTINCT (intersectAll keeps duplicates)
A.intersect(B).show()
```

**Output**
| id |
|---|
| 2 |
| 3 |

---

### Q62. Range (non-equi) join
**Problem:** Assign a salary grade to each employee using min/max salary ranges.

**Input: `employees`**
| name | salary |
|---|---|
| Amit | 50000 |
| Riya | 40000 |

**Input: `grades`**
| grade | min_sal | max_sal |
|---|---|---|
| A | 50000 | 100000 |
| B | 30000 | 49999 |

**Solution**
```python
# Join condition can be any boolean expression (non-equi join)
cond = (emp.salary >= grades.min_sal) & (emp.salary <= grades.max_sal)
emp.join(grades, cond).select("name", "salary", "grade").show()
```

**Output**
| name | salary | grade |
|---|---|---|
| Amit | 50000 | A |
| Riya | 40000 | B |

---

### Q63. Customers who bought both Pen and Book
**Problem:** Find customers who purchased both products.

**Input: `purchases`**
| cust | product |
|---|---|
| A | Pen |
| A | Book |
| B | Pen |
| C | Book |
| C | Pen |

**Solution**
```python
# Filter to the products of interest, then require BOTH to be present
(df.filter(F.col("product").isin("Pen", "Book"))
   .groupBy("cust").agg(F.countDistinct("product").alias("n"))
   .filter("n = 2").select("cust").show())
```

**Output**
| cust |
|---|
| A |
| C |

---

### Q64. Employees earning more than their manager
**Problem:** Find employees whose salary is greater than their manager's salary.

**Input: `employees`**
| emp_id | name | salary | manager_id |
|---|---|---|---|
| 1 | Amit | 90000 | null |
| 2 | Riya | 95000 | 1 |
| 3 | John | 60000 | 1 |

**Solution**
```python
e = df.alias("e"); m = df.alias("m")
(e.join(m, F.col("e.manager_id") == F.col("m.emp_id"))
  .filter(F.col("e.salary") > F.col("m.salary"))
  .select(F.col("e.name").alias("employee")).show())
```

**Output**
| employee |
|---|
| Riya |

---

### Q65. Attach group totals to every row
**Problem:** Show each employee along with the total salary of their department.

**Input: `employees`**
| name | dept | salary |
|---|---|---|
| Amit | IT | 50000 |
| John | IT | 60000 |
| Riya | HR | 40000 |

**Solution**
```python
totals = df.groupBy("dept").agg(F.sum("salary").alias("dept_total"))
df.join(totals, "dept").select("name", "dept", "salary", "dept_total").show()
```

**Output**
| name | dept | salary | dept_total |
|---|---|---|---|
| Amit | IT | 50000 | 110000 |
| John | IT | 60000 | 110000 |
| Riya | HR | 40000 | 40000 |

---

### Q66. Null-safe join
**Problem:** Join two tables on a key that can be NULL and match NULL with NULL.

**Input: `t1`**
| k | v1 |
|---|---|
| 1 | a |
| null | b |

**Input: `t2`**
| k | v2 |
|---|---|
| 1 | c |
| null | d |

**Solution**
```python
# Normal == never matches NULL = NULL. eqNullSafe (<=>) treats NULL = NULL as true
t1.join(t2, t1.k.eqNullSafe(t2.k)).select(t1.k, "v1", "v2").show()
```

**Output**
| k | v1 | v2 |
|---|---|---|
| 1 | a | c |
| null | b | d |

---

### Q67. Products never sold
**Problem:** List products that have no sales.

**Input: `products`**
| pid | name |
|---|---|
| 1 | Pen |
| 2 | Book |
| 3 | Bag |

**Input: `sales`**
| sale_id | pid |
|---|---|
| 1 | 1 |
| 2 | 1 |
| 3 | 2 |

**Solution**
```python
products.join(sales, "pid", "left_anti").show()
```

**Output**
| pid | name |
|---|---|
| 3 | Bag |

---

### Q68. Departments without employees
**Problem:** Find departments that have no employees.

**Input:** `employees` and `departments` same as Q51.

**Solution**
```python
dept.join(emp, "dept_id", "left_anti").show()
```

**Output**
| dept_id | dept_name |
|---|---|
| 40 | Finance |

---

### Q69. Compare source and target (reconciliation)
**Problem:** Find rows that are missing or different between source and target.

**Input: `src`**
| id | amt |
|---|---|
| 1 | 100 |
| 2 | 200 |
| 3 | 300 |

**Input: `tgt`**
| id | amt |
|---|---|
| 1 | 100 |
| 2 | 250 |
| 4 | 400 |

**Solution**
```python
# Full outer join on the key, then classify each row
s = src.withColumnRenamed("amt", "src_amt")
t = tgt.withColumnRenamed("amt", "tgt_amt")
(s.join(t, "id", "full")
  .withColumn("status",
      F.when(F.col("tgt_amt").isNull(), "missing_in_target")
       .when(F.col("src_amt").isNull(), "missing_in_source")
       .when(F.col("src_amt") != F.col("tgt_amt"), "mismatch")
       .otherwise("match"))
  .filter("status != 'match'").orderBy("id").show())
```

**Output**
| id | src_amt | tgt_amt | status |
|---|---|---|---|
| 2 | 200 | 250 | mismatch |
| 3 | 300 | null | missing_in_target |
| 4 | null | 400 | missing_in_source |

---

### Q70. Anti join on multiple keys
**Problem:** Find rows of A whose (id, dt) combination does not exist in B.

**Input: `A`**
| id | dt |
|---|---|
| 1 | d1 |
| 1 | d2 |
| 2 | d1 |

**Input: `B`**
| id | dt |
|---|---|
| 1 | d1 |

**Solution**
```python
A.join(B, ["id", "dt"], "left_anti").show()
```

**Output**
| id | dt |
|---|---|
| 1 | d2 |
| 2 | d1 |

---

### Q71. Employees having the same salary
**Problem:** Find pairs of employees with identical salaries.

**Input: `employees`**
| id | name | salary |
|---|---|---|
| 1 | Amit | 50000 |
| 2 | Riya | 50000 |
| 3 | John | 60000 |

**Solution**
```python
# a.id < b.id avoids (X,X) and mirrored (Y,X) pairs
a = df.alias("a"); b = df.alias("b")
(a.join(b, (F.col("a.salary") == F.col("b.salary")) & (F.col("a.id") < F.col("b.id")))
  .select(F.col("a.name").alias("emp1"), F.col("b.name").alias("emp2"), F.col("a.salary")).show())
```

**Output**
| emp1 | emp2 | salary |
|---|---|---|
| Amit | Riya | 50000 |

---

### Q72. Customers who ordered in both 2023 and 2024
**Problem:** Find customers active in both years.

**Input: `orders`**
| cust | year |
|---|---|
| A | 2023 |
| A | 2024 |
| B | 2023 |
| C | 2024 |
| C | 2024 |

**Solution**
```python
(df.groupBy("cust").agg(F.countDistinct("year").alias("yrs"))
   .filter("yrs = 2").select("cust").show())
```

**Output**
| cust |
|---|
| A |

---

### Q73. Avoid ambiguous columns after a join
**Problem:** Both tables have a `name` column. Show employee name and department name clearly.

**Input: `emp`**
| id | name | dept_id |
|---|---|---|
| 1 | Amit | 10 |

**Input: `dept`**
| dept_id | name |
|---|---|
| 10 | IT |

**Solution**
```python
# Alias the DataFrames, reference columns as alias.col, and rename in select
e = emp.alias("e"); d = dept.alias("d")
(e.join(d, F.col("e.dept_id") == F.col("d.dept_id"))
  .select(F.col("e.name").alias("emp_name"), F.col("d.name").alias("dept_name")).show())
```

**Output**
| emp_name | dept_name |
|---|---|
| Amit | IT |

---

### Q74. Three-table join with calculation
**Problem:** Compute total revenue per customer (qty * price).

**Input: `customers`**
| cid | cname |
|---|---|
| 1 | Amit |
| 2 | Riya |

**Input: `orders`**
| oid | cid | pid | qty |
|---|---|---|---|
| 1 | 1 | 1 | 2 |
| 2 | 1 | 2 | 1 |
| 3 | 2 | 1 | 3 |

**Input: `products`**
| pid | price |
|---|---|
| 1 | 100 |
| 2 | 50 |

**Solution**
```python
(orders.join(products, "pid").join(customers, "cid")
   .groupBy("cname").agg(F.sum(F.col("qty") * F.col("price")).alias("revenue"))
   .orderBy(F.col("revenue").desc()).show())
```

**Output**
| cname | revenue |
|---|---|
| Riya | 300 |
| Amit | 250 |

---

### Q75. Total spend including customers with no orders
**Problem:** Show total spend per customer, with 0 for customers who never ordered.

**Input: `customers`**
| cid | name |
|---|---|
| 1 | Amit |
| 2 | Riya |
| 3 | John |

**Input: `orders`**
| oid | cid | amount |
|---|---|---|
| 1 | 1 | 100 |
| 2 | 1 | 50 |
| 3 | 2 | 200 |

**Solution**
```python
# left join keeps customers without orders; coalesce turns the NULL sum into 0
(customers.join(orders, "cid", "left")
   .groupBy("cid", "name").agg(F.coalesce(F.sum("amount"), F.lit(0)).alias("total_spend"))
   .orderBy("cid").show())
```

**Output**
| cid | name | total_spend |
|---|---|---|
| 1 | Amit | 150 |
| 2 | Riya | 200 |
| 3 | John | 0 |

---
# Section 4: Window Functions

> Employee data used in many questions below (`emp`):
>
> | emp_id | name | dept | salary |
> |---|---|---|---|
> | 1 | Amit | IT | 50000 |
> | 2 | Riya | HR | 40000 |
> | 3 | John | IT | 60000 |
> | 4 | Sara | IT | 60000 |
> | 5 | Neha | HR | 45000 |

### Q76. Row number within each department
**Problem:** Number employees in each department from highest to lowest salary (break ties by name).

**Input:** `emp` (see table above)

**Solution**
```python
# partitionBy = GROUP BY for the window, orderBy = order inside each partition
w = Window.partitionBy("dept").orderBy(F.col("salary").desc(), F.col("name"))
df.withColumn("rn", F.row_number().over(w)).show()
```

**Output**
| name | dept | salary | rn |
|---|---|---|---|
| John | IT | 60000 | 1 |
| Sara | IT | 60000 | 2 |
| Amit | IT | 50000 | 3 |
| Neha | HR | 45000 | 1 |
| Riya | HR | 40000 | 2 |

---

### Q77. rank() vs dense_rank()
**Problem:** Show both rank and dense_rank of salary within each department.

**Input:** `emp`

**Solution**
```python
# rank leaves gaps after ties (1,1,3), dense_rank does not (1,1,2)
w = Window.partitionBy("dept").orderBy(F.col("salary").desc())
(df.withColumn("rank", F.rank().over(w))
   .withColumn("dense_rank", F.dense_rank().over(w)).show())
```

**Output**
| name | dept | salary | rank | dense_rank |
|---|---|---|---|---|
| John | IT | 60000 | 1 | 1 |
| Sara | IT | 60000 | 1 | 1 |
| Amit | IT | 50000 | 3 | 2 |
| Neha | HR | 45000 | 1 | 1 |
| Riya | HR | 40000 | 2 | 2 |

---

### Q78. 3rd highest salary overall
**Problem:** Find the employee with the 3rd highest distinct salary.

**Input:** `emp`

**Solution**
```python
# dense_rank over ALL rows (no partition) so ties share a rank; filter rank = N
w = Window.orderBy(F.col("salary").desc())
(df.withColumn("dr", F.dense_rank().over(w)).filter("dr = 3").drop("dr").show())
```

**Output**
| emp_id | name | dept | salary |
|---|---|---|---|
| 5 | Neha | HR | 45000 |

---

### Q79. Highest paid employee(s) in each department (ties included)
**Problem:** Return all top earners per department.

**Input:** `emp`

**Solution**
```python
w = Window.partitionBy("dept").orderBy(F.col("salary").desc())
(df.withColumn("rnk", F.rank().over(w)).filter("rnk = 1").drop("rnk").show())
```

**Output**
| emp_id | name | dept | salary |
|---|---|---|---|
| 3 | John | IT | 60000 |
| 4 | Sara | IT | 60000 |
| 5 | Neha | HR | 45000 |

---

### Q80. Previous row value with lag()
**Problem:** Show the previous day's sales and the difference from it.

**Input: `daily_sales`**
| dt | amount |
|---|---|
| 2024-01-01 | 100 |
| 2024-01-02 | 150 |
| 2024-01-03 | 120 |

**Solution**
```python
# lag(col, n) looks n rows back, NULL if none exists (lag with default: lag("amount", 1, 0))
w = Window.orderBy("dt")
(df.withColumn("prev_amount", F.lag("amount", 1).over(w))
   .withColumn("diff", F.col("amount") - F.col("prev_amount")).show())
```

**Output**
| dt | amount | prev_amount | diff |
|---|---|---|---|
| 2024-01-01 | 100 | null | null |
| 2024-01-02 | 150 | 100 | 50 |
| 2024-01-03 | 120 | 150 | -30 |

---

### Q81. Next row value with lead()
**Problem:** Show the next day's sales for each day.

**Input:** `daily_sales` (same as Q80)

**Solution**
```python
w = Window.orderBy("dt")
df.withColumn("next_amount", F.lead("amount", 1).over(w)).show()
```

**Output**
| dt | amount | next_amount |
|---|---|---|
| 2024-01-01 | 100 | 150 |
| 2024-01-02 | 150 | 120 |
| 2024-01-03 | 120 | null |

---

### Q82. Running total
**Problem:** Compute the cumulative sales by date.

**Input:** `daily_sales` (same as Q80)

**Solution**
```python
# Frame: from the first row up to the current row
w = Window.orderBy("dt").rowsBetween(Window.unboundedPreceding, Window.currentRow)
df.withColumn("running_total", F.sum("amount").over(w)).show()
```

**Output**
| dt | amount | running_total |
|---|---|---|
| 2024-01-01 | 100 | 100 |
| 2024-01-02 | 150 | 250 |
| 2024-01-03 | 120 | 370 |

---

### Q83. Moving average of last 3 rows
**Problem:** Compute the 3-day moving average of sales.

**Input:** `daily_sales` (same as Q80)

**Solution**
```python
# rowsBetween(-2, 0) = two previous rows + current row
w = Window.orderBy("dt").rowsBetween(-2, 0)
df.withColumn("moving_avg", F.round(F.avg("amount").over(w), 2)).show()
```

**Output**
| dt | amount | moving_avg |
|---|---|---|
| 2024-01-01 | 100 | 100.0 |
| 2024-01-02 | 150 | 125.0 |
| 2024-01-03 | 120 | 123.33 |

---

### Q84. Days with sales higher than the previous day
**Problem:** Find the days where sales increased compared to the day before.

**Input:** `daily_sales` (same as Q80)

**Solution**
```python
w = Window.orderBy("dt")
(df.withColumn("prev", F.lag("amount").over(w))
   .filter(F.col("amount") > F.col("prev")).drop("prev").show())
```

**Output**
| dt | amount |
|---|---|
| 2024-01-02 | 150 |

---

### Q85. Month over month growth percentage
**Problem:** Calculate MoM revenue growth %.

**Input: `monthly`**
| month | revenue |
|---|---|
| 2024-01 | 1000 |
| 2024-02 | 1200 |
| 2024-03 | 900 |

**Solution**
```python
w = Window.orderBy("month")
(df.withColumn("prev", F.lag("revenue").over(w))
   .withColumn("growth_pct", F.round((F.col("revenue") - F.col("prev")) * 100 / F.col("prev"), 2))
   .drop("prev").show())
```

**Output**
| month | revenue | growth_pct |
|---|---|---|
| 2024-01 | 1000 | null |
| 2024-02 | 1200 | 20.0 |
| 2024-03 | 900 | -25.0 |

---

### Q86. Lowest and highest paid in the department for every row
**Problem:** Add the name of the lowest and highest paid employee of each department to each row.

**Input: `emp`**
| name | dept | salary |
|---|---|---|
| Amit | IT | 50000 |
| John | IT | 60000 |
| Riya | HR | 40000 |
| Neha | HR | 45000 |

**Solution**
```python
# last_value needs a FULL frame, otherwise it just returns the current row
w = (Window.partitionBy("dept").orderBy("salary")
           .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing))
(df.withColumn("lowest_paid",  F.first("name").over(w))
   .withColumn("highest_paid", F.last("name").over(w)).show())
```

**Output**
| name | dept | salary | lowest_paid | highest_paid |
|---|---|---|---|---|
| Riya | HR | 40000 | Riya | Neha |
| Neha | HR | 45000 | Riya | Neha |
| Amit | IT | 50000 | Amit | John |
| John | IT | 60000 | Amit | John |

---

### Q87. Split employees into buckets with ntile()
**Problem:** Divide employees into 2 salary groups.

**Input:** `emp`

**Solution**
```python
# ntile(n) splits ordered rows into n nearly equal buckets (5 rows -> 3 + 2)
w = Window.orderBy("salary")
df.withColumn("bucket", F.ntile(2).over(w)).select("name", "salary", "bucket").show()
```

**Output**
| name | salary | bucket |
|---|---|---|
| Riya | 40000 | 1 |
| Neha | 45000 | 1 |
| Amit | 50000 | 1 |
| John | 60000 | 2 |
| Sara | 60000 | 2 |

---

### Q88. percent_rank() and cume_dist()
**Problem:** Show percent_rank and cume_dist of salaries.

**Input:** `emp`

**Solution**
```python
# percent_rank = (rank - 1) / (n - 1); cume_dist = rows <= current / n
w = Window.orderBy("salary")
(df.withColumn("pct_rank",  F.round(F.percent_rank().over(w), 2))
   .withColumn("cume_dist", F.round(F.cume_dist().over(w), 2))
   .select("name", "salary", "pct_rank", "cume_dist").show())
```

**Output**
| name | salary | pct_rank | cume_dist |
|---|---|---|---|
| Riya | 40000 | 0.0 | 0.2 |
| Neha | 45000 | 0.25 | 0.4 |
| Amit | 50000 | 0.5 | 0.6 |
| John | 60000 | 0.75 | 1.0 |
| Sara | 60000 | 0.75 | 1.0 |

---

### Q89. Salary difference from the department maximum
**Problem:** Show how far each salary is from the highest salary in its department.

**Input:** `emp`

**Solution**
```python
# Aggregate functions over an un-ordered partition give the group value on every row
w = Window.partitionBy("dept")
(df.withColumn("diff_from_max", F.max("salary").over(w) - F.col("salary"))
   .select("name", "dept", "salary", "diff_from_max").show())
```

**Output**
| name | dept | salary | diff_from_max |
|---|---|---|---|
| Amit | IT | 50000 | 10000 |
| John | IT | 60000 | 0 |
| Sara | IT | 60000 | 0 |
| Riya | HR | 40000 | 5000 |
| Neha | HR | 45000 | 0 |

---

### Q90. Cumulative order count per customer
**Problem:** Number each customer's orders in time order (1st, 2nd, 3rd...).

**Input: `orders`**
| cust | order_date |
|---|---|
| A | 2024-01-01 |
| A | 2024-01-05 |
| B | 2024-01-02 |
| A | 2024-01-09 |

**Solution**
```python
w = Window.partitionBy("cust").orderBy("order_date")
df.withColumn("order_no", F.row_number().over(w)).show()
```

**Output**
| cust | order_date | order_no |
|---|---|---|
| A | 2024-01-01 | 1 |
| A | 2024-01-05 | 2 |
| A | 2024-01-09 | 3 |
| B | 2024-01-02 | 1 |

---

### Q91. Running total per customer
**Problem:** Cumulative spend for each customer by date.

**Input: `orders`**
| cust | order_date | amount |
|---|---|---|
| A | 2024-01-01 | 100 |
| A | 2024-01-05 | 50 |
| B | 2024-01-02 | 200 |
| A | 2024-01-09 | 25 |

**Solution**
```python
w = (Window.partitionBy("cust").orderBy("order_date")
           .rowsBetween(Window.unboundedPreceding, Window.currentRow))
df.withColumn("running_spend", F.sum("amount").over(w)).show()
```

**Output**
| cust | order_date | amount | running_spend |
|---|---|---|---|
| A | 2024-01-01 | 100 | 100 |
| A | 2024-01-05 | 50 | 150 |
| A | 2024-01-09 | 25 | 175 |
| B | 2024-01-02 | 200 | 200 |

---

### Q92. Employees earning above department average (window version)
**Problem:** List employees with salary greater than their department's average.

**Input:** `emp`

**Solution**
```python
# No join needed: compute the avg as a window column and filter on it
w = Window.partitionBy("dept")
(df.withColumn("dept_avg", F.avg("salary").over(w))
   .filter(F.col("salary") > F.col("dept_avg"))
   .select("name", "dept", "salary").show())
```

**Output**
| name | dept | salary |
|---|---|---|
| John | IT | 60000 |
| Sara | IT | 60000 |
| Neha | HR | 45000 |

---

### Q93. Second highest salary in each department
**Problem:** Find the 2nd highest distinct salary per department.

**Input:** `emp`

**Solution**
```python
w = Window.partitionBy("dept").orderBy(F.col("salary").desc())
(df.withColumn("dr", F.dense_rank().over(w)).filter("dr = 2")
   .select("name", "dept", "salary").show())
```

**Output**
| name | dept | salary |
|---|---|---|
| Amit | IT | 50000 |
| Riya | HR | 40000 |

---

### Q94. Remove duplicates keeping the latest row
**Problem:** For each id keep only the most recently created row.

**Input: `customers`**
| id | email | created |
|---|---|---|
| 1 | a@x.com | 2024-01-01 |
| 1 | a@x.com | 2024-02-01 |
| 2 | b@x.com | 2024-01-05 |

**Solution**
```python
# The most asked dedup pattern: row_number() desc by timestamp, keep rn = 1
w = Window.partitionBy("id").orderBy(F.col("created").desc())
(df.withColumn("rn", F.row_number().over(w)).filter("rn = 1").drop("rn").show())
```

**Output**
| id | email | created |
|---|---|---|
| 1 | a@x.com | 2024-02-01 |
| 2 | b@x.com | 2024-01-05 |

---

### Q95. First order of every customer
**Problem:** Get the earliest order for each customer.

**Input: `orders`**
| oid | cust | dt |
|---|---|---|
| 1 | A | 2024-01-03 |
| 2 | A | 2024-01-01 |
| 3 | B | 2024-01-05 |

**Solution**
```python
w = Window.partitionBy("cust").orderBy("dt")
(df.withColumn("rn", F.row_number().over(w)).filter("rn = 1").drop("rn").show())
```

**Output**
| oid | cust | dt |
|---|---|---|
| 2 | A | 2024-01-01 |
| 3 | B | 2024-01-05 |

---

### Q96. Orders where amount increased compared to the previous order
**Problem:** Find each customer's orders whose amount is higher than their previous order.

**Input: `orders`**
| cust | dt | amount |
|---|---|---|
| A | 1 | 100 |
| A | 2 | 150 |
| A | 3 | 120 |
| B | 1 | 50 |
| B | 2 | 60 |

**Solution**
```python
w = Window.partitionBy("cust").orderBy("dt")
(df.withColumn("prev", F.lag("amount").over(w))
   .filter(F.col("amount") > F.col("prev")).drop("prev").show())
```

**Output**
| cust | dt | amount |
|---|---|---|
| A | 2 | 150 |
| B | 2 | 60 |

---

### Q97. Rolling 7-day sum (calendar based)
**Problem:** For each date, sum the amount of the last 7 days (including today).

**Input: `sales`**
| dt | amount |
|---|---|
| 2024-01-01 | 10 |
| 2024-01-02 | 20 |
| 2024-01-05 | 30 |
| 2024-01-09 | 40 |

**Solution**
```python
# rangeBetween works on the VALUE of the ordering column, so convert the date to
# seconds. rowsBetween would count rows, which is wrong when dates are missing
sec = F.col("dt").cast("timestamp").cast("long")
w = Window.orderBy(sec).rangeBetween(-6 * 86400, 0)
df.withColumn("rolling_7d", F.sum("amount").over(w)).show()
```

**Output**
| dt | amount | rolling_7d |
|---|---|---|
| 2024-01-01 | 10 | 10 |
| 2024-01-02 | 20 | 30 |
| 2024-01-05 | 30 | 60 |
| 2024-01-09 | 40 | 70 |

---

### Q98. Overall rank and department rank together
**Problem:** Show the dense rank of salary overall and within the department.

**Input:** `emp`

**Solution**
```python
w_all  = Window.orderBy(F.col("salary").desc())
w_dept = Window.partitionBy("dept").orderBy(F.col("salary").desc())
(df.withColumn("overall_rank", F.dense_rank().over(w_all))
   .withColumn("dept_rank",    F.dense_rank().over(w_dept))
   .select("name", "dept", "salary", "overall_rank", "dept_rank").show())
```

**Output**
| name | dept | salary | overall_rank | dept_rank |
|---|---|---|---|---|
| John | IT | 60000 | 1 | 1 |
| Sara | IT | 60000 | 1 | 1 |
| Amit | IT | 50000 | 2 | 2 |
| Neha | HR | 45000 | 3 | 1 |
| Riya | HR | 40000 | 4 | 2 |

---

### Q99. Cumulative percentage (Pareto)
**Problem:** Show the cumulative % of total sales, products ordered by sales descending.

**Input: `product_sales`**
| product | sales |
|---|---|
| A | 50 |
| B | 30 |
| C | 20 |

**Solution**
```python
w_cum = Window.orderBy(F.col("sales").desc()).rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_tot = Window.partitionBy()
(df.withColumn("cum_pct", F.round(F.sum("sales").over(w_cum) * 100 / F.sum("sales").over(w_tot), 1)).show())
```

**Output**
| product | sales | cum_pct |
|---|---|---|
| A | 50 | 50.0 |
| B | 30 | 80.0 |
| C | 20 | 100.0 |

---

### Q100. Top 2 products per category by revenue
**Problem:** Return the top 2 products in each category.

**Input: `product_revenue`**
| category | product | revenue |
|---|---|---|
| Elec | Phone | 500 |
| Elec | TV | 300 |
| Elec | Cable | 50 |
| Groc | Rice | 200 |
| Groc | Oil | 250 |

**Solution**
```python
# Top-N per group: rank inside each group, then filter rank <= N
w = Window.partitionBy("category").orderBy(F.col("revenue").desc())
(df.withColumn("rn", F.row_number().over(w)).filter("rn <= 2").show())
```

**Output**
| category | product | revenue | rn |
|---|---|---|---|
| Elec | Phone | 500 | 1 |
| Elec | TV | 300 | 2 |
| Groc | Oil | 250 | 1 |
| Groc | Rice | 200 | 2 |

---

### Q101. Compare each salary with the overall average
**Problem:** Show the difference between each salary and the company-wide average.

**Input:** `emp` (average = 51000)

**Solution**
```python
w = Window.partitionBy()   # one window over the whole table
(df.withColumn("diff_from_avg", F.col("salary") - F.avg("salary").over(w))
   .select("name", "salary", "diff_from_avg").show())
```

**Output**
| name | salary | diff_from_avg |
|---|---|---|
| Amit | 50000 | -1000.0 |
| Riya | 40000 | -11000.0 |
| John | 60000 | 9000.0 |
| Sara | 60000 | 9000.0 |
| Neha | 45000 | -6000.0 |

---

### Q102. Employees earning more than the previous employee
**Problem:** Ordered by emp_id, find employees whose salary is higher than the previous one's.

**Input:** `emp`

**Solution**
```python
w = Window.orderBy("emp_id")
(df.withColumn("prev_sal", F.lag("salary").over(w))
   .filter(F.col("salary") > F.col("prev_sal")).select("emp_id", "name", "salary").show())
```

**Output**
| emp_id | name | salary |
|---|---|---|
| 3 | John | 60000 |

---

### Q103. Running maximum
**Problem:** Show the highest temperature seen so far.

**Input: `temps`**
| day | temp |
|---|---|
| 1 | 30 |
| 2 | 28 |
| 3 | 35 |
| 4 | 33 |

**Solution**
```python
w = Window.orderBy("day").rowsBetween(Window.unboundedPreceding, Window.currentRow)
df.withColumn("max_so_far", F.max("temp").over(w)).show()
```

**Output**
| day | temp | max_so_far |
|---|---|---|
| 1 | 30 | 30 |
| 2 | 28 | 30 |
| 3 | 35 | 35 |
| 4 | 33 | 35 |

---

### Q104. Department headcount on every row
**Problem:** Add the number of employees in the same department to each row.

**Input:** `emp`

**Solution**
```python
df.withColumn("dept_size", F.count("*").over(Window.partitionBy("dept"))).show()
```

**Output**
| name | dept | dept_size |
|---|---|---|
| Amit | IT | 3 |
| John | IT | 3 |
| Sara | IT | 3 |
| Riya | HR | 2 |
| Neha | HR | 2 |

---

### Q105. Days between consecutive orders
**Problem:** Find the gap in days between a customer's consecutive orders.

**Input: `orders`**
| cust | order_date |
|---|---|
| A | 2024-01-01 |
| A | 2024-01-05 |
| A | 2024-01-12 |

**Solution**
```python
w = Window.partitionBy("cust").orderBy("order_date")
(df.withColumn("prev_date", F.lag("order_date").over(w))
   .withColumn("gap_days", F.datediff("order_date", "prev_date")).show())
```

**Output**
| cust | order_date | prev_date | gap_days |
|---|---|---|---|
| A | 2024-01-01 | null | null |
| A | 2024-01-05 | 2024-01-01 | 4 |
| A | 2024-01-12 | 2024-01-05 | 7 |

---

### Q106. Days since the customer's first order
**Problem:** For each order, show how many days have passed since the customer's first order.

**Input:** `orders` (same as Q105)

**Solution**
```python
w = Window.partitionBy("cust")
(df.withColumn("first_order", F.min("order_date").over(w))
   .withColumn("days_since_first", F.datediff("order_date", "first_order")).show())
```

**Output**
| cust | order_date | first_order | days_since_first |
|---|---|---|---|
| A | 2024-01-01 | 2024-01-01 | 0 |
| A | 2024-01-05 | 2024-01-01 | 4 |
| A | 2024-01-12 | 2024-01-01 | 11 |

---

### Q107. Store ranking per month
**Problem:** Rank stores by sales within each month.

**Input: `store_sales`**
| month | store | sales |
|---|---|---|
| Jan | S1 | 100 |
| Jan | S2 | 150 |
| Feb | S1 | 200 |
| Feb | S2 | 50 |

**Solution**
```python
w = Window.partitionBy("month").orderBy(F.col("sales").desc())
df.withColumn("rank", F.rank().over(w)).show()
```

**Output**
| month | store | sales | rank |
|---|---|---|---|
| Feb | S1 | 200 | 1 |
| Feb | S2 | 50 | 2 |
| Jan | S2 | 150 | 1 |
| Jan | S1 | 100 | 2 |

---

### Q108. Last 3 transactions per user
**Problem:** Keep only the 3 most recent transactions of every user.

**Input: `txns`**
| user | txn_id |
|---|---|
| u1 | 1 |
| u1 | 2 |
| u1 | 3 |
| u1 | 4 |
| u2 | 5 |

**Solution**
```python
w = Window.partitionBy("user").orderBy(F.col("txn_id").desc())
(df.withColumn("rn", F.row_number().over(w)).filter("rn <= 3").drop("rn").show())
```

**Output**
| user | txn_id |
|---|---|
| u1 | 4 |
| u1 | 3 |
| u1 | 2 |
| u2 | 5 |

---

### Q109. lag() with a default value
**Problem:** Show the previous day's amount, using 0 when there is no previous day.

**Input:** `daily_sales` (same as Q80)

**Solution**
```python
# third argument of lag() is the default when no previous row exists
w = Window.orderBy("dt")
df.withColumn("prev_amount", F.lag("amount", 1, 0).over(w)).show()
```

**Output**
| dt | amount | prev_amount |
|---|---|---|
| 2024-01-01 | 100 | 0 |
| 2024-01-02 | 150 | 100 |
| 2024-01-03 | 120 | 150 |

---

### Q110. Running total that resets every month
**Problem:** Compute the cumulative amount that restarts at the beginning of each month.

**Input: `sales`**
| dt | amount |
|---|---|
| 2024-01-10 | 10 |
| 2024-01-20 | 20 |
| 2024-02-05 | 5 |
| 2024-02-15 | 15 |

**Solution**
```python
# Partition by the month so the frame restarts for every month
w = (Window.partitionBy(F.date_format("dt", "yyyy-MM")).orderBy("dt")
           .rowsBetween(Window.unboundedPreceding, Window.currentRow))
df.withColumn("month_running", F.sum("amount").over(w)).show()
```

**Output**
| dt | amount | month_running |
|---|---|---|
| 2024-01-10 | 10 | 10 |
| 2024-01-20 | 20 | 30 |
| 2024-02-05 | 5 | 5 |
| 2024-02-15 | 15 | 20 |

---
# Section 5: Deduplication, CDC, Data Quality and Dates

### Q111. Deduplicate full records keeping the latest version
**Problem:** Keep only the latest record for every customer id.

**Input: `customers`**
| id | name | updated_at |
|---|---|---|
| 1 | Amit | 2024-01-01 |
| 1 | Amit K | 2024-03-01 |
| 2 | Riya | 2024-02-01 |

**Solution**
```python
w = Window.partitionBy("id").orderBy(F.col("updated_at").desc())
(df.withColumn("rn", F.row_number().over(w)).filter("rn = 1").drop("rn").show())
```

**Output**
| id | name | updated_at |
|---|---|---|
| 1 | Amit K | 2024-03-01 |
| 2 | Riya | 2024-02-01 |

---

### Q112. Detect fully duplicated rows
**Problem:** Find rows that appear more than once (all columns identical).

**Input: `data`**
| id | name |
|---|---|
| 1 | A |
| 1 | A |
| 2 | B |

**Solution**
```python
# Group by ALL columns and keep groups with count > 1
(df.groupBy(df.columns).count().filter("count > 1").show())
```

**Output**
| id | name | count |
|---|---|---|
| 1 | A | 2 |

---

### Q113. Upsert: apply updates onto existing data
**Problem:** Merge `updates` into `target` (update matching ids, insert new ids).

**Input: `target`**
| id | name | amt |
|---|---|---|
| 1 | A | 100 |
| 2 | B | 200 |

**Input: `updates`**
| id | name | amt |
|---|---|---|
| 2 | B | 250 |
| 3 | C | 300 |

**Solution**
```python
# Plain parquet/CSV has no MERGE. Emulate: union with a priority flag, keep best per id.
# (On Delta tables use DeltaTable.merge instead)
t = target.withColumn("prio", F.lit(1))
u = updates.withColumn("prio", F.lit(2))
w = Window.partitionBy("id").orderBy(F.col("prio").desc())
(t.unionByName(u).withColumn("rn", F.row_number().over(w))
  .filter("rn = 1").drop("rn", "prio").orderBy("id").show())
```

**Output**
| id | name | amt |
|---|---|---|
| 1 | A | 100 |
| 2 | B | 250 |
| 3 | C | 300 |

---

### Q114. Incremental load using a watermark
**Problem:** Load only rows updated after the last processed timestamp 2024-01-05.

**Input: `source`**
| id | updated_at |
|---|---|
| 1 | 2024-01-01 |
| 2 | 2024-01-05 |
| 3 | 2024-01-10 |

**Solution**
```python
# Store the max(updated_at) of the previous run, then read only newer rows
last_watermark = "2024-01-05"
new_rows = df.filter(F.col("updated_at") > F.lit(last_watermark).cast("date"))
new_rows.show()
# after loading: new watermark = new_rows.agg(F.max("updated_at")).collect()[0][0]
```

**Output**
| id | updated_at |
|---|---|
| 3 | 2024-01-10 |

---

### Q115. Detect new and changed rows between two snapshots
**Problem:** Compare yesterday's and today's snapshots and label new and changed rows.

**Input: `yesterday`**
| id | name | city |
|---|---|---|
| 1 | A | Pune |
| 2 | B | Delhi |

**Input: `today`**
| id | name | city |
|---|---|---|
| 1 | A | Pune |
| 2 | B | Mumbai |
| 3 | C | Goa |

**Solution**
```python
# Hash all non-key columns so comparing many columns is a single comparison
def with_hash(d):
    return d.withColumn("h", F.md5(F.concat_ws("|", "name", "city")))

y = with_hash(yesterday).select("id", F.col("h").alias("y_hash"))
t = with_hash(today)
(t.join(y, "id", "left")
  .withColumn("change_type",
      F.when(F.col("y_hash").isNull(), "NEW")
       .when(F.col("y_hash") != F.col("h"), "CHANGED"))
  .filter("change_type is not null")
  .select("id", "name", "city", "change_type").orderBy("id").show())
```

**Output**
| id | name | city | change_type |
|---|---|---|---|
| 2 | B | Mumbai | CHANGED |
| 3 | C | Goa | NEW |

---

### Q116. Apply CDC records (Insert / Update / Delete)
**Problem:** Build the final table from a change log with operation flags I, U, D.

**Input: `changes`**
| id | name | op | ts |
|---|---|---|---|
| 1 | A | I | 1 |
| 1 | A2 | U | 2 |
| 2 | B | I | 1 |
| 2 | B | D | 2 |
| 3 | C | I | 1 |

**Solution**
```python
# Take the latest change per id, then drop the ones whose latest op is Delete
w = Window.partitionBy("id").orderBy(F.col("ts").desc())
(df.withColumn("rn", F.row_number().over(w))
   .filter("rn = 1").filter(F.col("op") != "D")
   .select("id", "name").orderBy("id").show())
```

**Output**
| id | name |
|---|---|
| 1 | A2 |
| 3 | C |

---

### Q117. Null percentage of every column
**Problem:** Calculate the percentage of nulls in each column.

**Input: `data`**
| id | name | city |
|---|---|---|
| 1 | A | Pune |
| 2 | null | null |
| 3 | C | null |
| 4 | D | Goa |

**Solution**
```python
total = df.count()
df.select([(F.sum(F.col(c).isNull().cast("int")) * 100 / total).alias(c) for c in df.columns]).show()
```

**Output**
| id | name | city |
|---|---|---|
| 0.0 | 25.0 | 50.0 |

---

### Q118. Validate email format
**Problem:** Find rows with an invalid email address.

**Input: `users`**
| id | email |
|---|---|
| 1 | a@x.com |
| 2 | bad.email |
| 3 | b@y |

**Solution**
```python
# rlike applies a Java regex. Valid = text@text.text
pattern = r"^[\w.+-]+@[\w-]+\.[\w.]+$"
df.filter(~F.col("email").rlike(pattern)).show()
```

**Output**
| id | email |
|---|---|
| 2 | bad.email |
| 3 | b@y |

---

### Q119. Outlier detection using IQR
**Problem:** Find values outside Q1 - 1.5*IQR and Q3 + 1.5*IQR.

**Input: `readings`**
| value |
|---|
| 10 |
| 11 |
| 12 |
| 13 |
| 100 |

**Solution**
```python
# approxQuantile(col, [probabilities], relativeError); 0.0 = exact
q1, q3 = df.approxQuantile("value", [0.25, 0.75], 0.0)
iqr = q3 - q1
low, high = q1 - 1.5 * iqr, q3 + 1.5 * iqr
df.filter((F.col("value") < low) | (F.col("value") > high)).show()
```

**Output**
| value |
|---|
| 100 |

---

### Q120. Referential integrity check (orphan records)
**Problem:** Find orders whose customer does not exist in the customers table.

**Input: `orders`**
| order_id | cust_id |
|---|---|
| 1 | 1 |
| 2 | 2 |
| 3 | 5 |

**Input: `customers`**
| cust_id | name |
|---|---|
| 1 | Amit |
| 2 | Riya |

**Solution**
```python
orders.join(customers, "cust_id", "left_anti").show()
```

**Output**
| cust_id | order_id |
|---|---|
| 5 | 3 |

---

### Q121. Standardize phone numbers
**Problem:** Keep only the digits of the phone number.

**Input: `contacts`**
| phone |
|---|
| 98765-43210 |
| (98765) 43210 |

**Solution**
```python
# [^0-9] means "anything that is not a digit"
df.withColumn("phone_clean", F.regexp_replace("phone", r"[^0-9]", "")).show()
```

**Output**
| phone | phone_clean |
|---|---|
| 98765-43210 | 9876543210 |
| (98765) 43210 | 9876543210 |

---

### Q122. Forward fill nulls with the last known value
**Problem:** Replace null prices with the previous non-null price.

**Input: `prices`**
| day | price |
|---|---|
| 1 | 10 |
| 2 | null |
| 3 | null |
| 4 | 12 |

**Solution**
```python
# last(..., ignorenulls=True) over everything up to the current row
w = Window.orderBy("day").rowsBetween(Window.unboundedPreceding, Window.currentRow)
df.withColumn("price_filled", F.last("price", ignorenulls=True).over(w)).show()
```

**Output**
| day | price | price_filled |
|---|---|---|
| 1 | 10 | 10 |
| 2 | null | 10 |
| 3 | null | 10 |
| 4 | 12 | 12 |

---

### Q123. Replace nulls with the group average
**Problem:** Fill missing salaries with the average salary of the same department.

**Input: `employees`**
| dept | salary |
|---|---|
| IT | 50000 |
| IT | 60000 |
| IT | null |
| HR | 40000 |
| HR | null |

**Solution**
```python
# avg() ignores nulls, so the window average is computed from known values only
w = Window.partitionBy("dept")
df.withColumn("salary", F.coalesce("salary", F.avg("salary").over(w))).show()
```

**Output**
| dept | salary |
|---|---|
| IT | 50000.0 |
| IT | 60000.0 |
| IT | 55000.0 |
| HR | 40000.0 |
| HR | 40000.0 |

---

### Q124. Generate a surrogate key
**Problem:** Add a sequential surrogate key to a dimension table.

**Input: `dim_customer`**
| name |
|---|
| Riya |
| Amit |
| John |

**Solution**
```python
# row_number gives 1,2,3... (single partition, ok for small dims).
# monotonically_increasing_id() is scalable but ids are NOT consecutive
w = Window.orderBy("name")
df.withColumn("sk", F.row_number().over(w)).show()
```

**Output**
| name | sk |
|---|---|
| Amit | 1 |
| John | 2 |
| Riya | 3 |

---

### Q125. Build SCD Type 2 date ranges
**Problem:** From change history, derive `end_date` and `is_current` for each version.

**Input: `history`**
| id | city | eff_date |
|---|---|---|
| 1 | Pune | 2023-01-01 |
| 1 | Mumbai | 2023-06-01 |
| 1 | Delhi | 2024-01-01 |

**Solution**
```python
# end_date = start of the NEXT version (lead); open-ended row gets 9999-12-31
w = Window.partitionBy("id").orderBy("eff_date")
(df.withColumn("end_date", F.coalesce(F.lead("eff_date").over(w), F.lit("9999-12-31").cast("date")))
   .withColumn("is_current", F.col("end_date") == F.lit("9999-12-31").cast("date")).show())
```

**Output**
| id | city | eff_date | end_date | is_current |
|---|---|---|---|---|
| 1 | Pune | 2023-01-01 | 2023-06-01 | false |
| 1 | Mumbai | 2023-06-01 | 2024-01-01 | false |
| 1 | Delhi | 2024-01-01 | 9999-12-31 | true |

---

### Q126. Add and subtract days
**Problem:** Show the date 7 days later and 7 days earlier.

**Input: `orders`**
| order_date |
|---|
| 2024-01-15 |

**Solution**
```python
df.select("order_date",
          F.date_add("order_date", 7).alias("plus_7"),
          F.date_sub("order_date", 7).alias("minus_7")).show()
```

**Output**
| order_date | plus_7 | minus_7 |
|---|---|---|
| 2024-01-15 | 2024-01-22 | 2024-01-08 |

---

### Q127. Difference between two dates
**Problem:** Find the number of days and months between start and end dates.

**Input: `projects`**
| start_date | end_date |
|---|---|
| 2024-01-01 | 2024-03-15 |

**Solution**
```python
# datediff(end, start) returns days. months_between returns a decimal number of months
df.select(F.datediff("end_date", "start_date").alias("days"),
          F.round(F.months_between("end_date", "start_date"), 2).alias("months")).show()
```

**Output**
| days | months |
|---|---|
| 74 | 2.45 |

---

### Q128. Extract year, month, day and weekday
**Problem:** Break a date into its parts.

**Input: `orders`**
| order_date |
|---|
| 2024-03-15 |

**Solution**
```python
# dayofweek: Sunday = 1 ... Saturday = 7
df.select(F.year("order_date").alias("year"),
          F.month("order_date").alias("month"),
          F.dayofmonth("order_date").alias("day"),
          F.dayofweek("order_date").alias("dow"),
          F.date_format("order_date", "EEEE").alias("day_name")).show()
```

**Output**
| year | month | day | dow | day_name |
|---|---|---|---|---|
| 2024 | 3 | 15 | 6 | Friday |

---

### Q129. Parse dates that are not in the default format
**Problem:** Convert `15/03/2024` to a date and `2024-03-16 10:30:00` to a timestamp.

**Input: `raw`**
| d1 | d2 |
|---|---|
| 15/03/2024 | 2024-03-16 10:30:00 |

**Solution**
```python
# Give the pattern that MATCHES the input text; wrong pattern gives NULL
df.select(F.to_date("d1", "dd/MM/yyyy").alias("date_col"),
          F.to_timestamp("d2", "yyyy-MM-dd HH:mm:ss").alias("ts_col")).show()
```

**Output**
| date_col | ts_col |
|---|---|
| 2024-03-15 | 2024-03-16 10:30:00 |

---

### Q130. First and last day of the month
**Problem:** Get the first and last date of the month for each date.

**Input: `orders`**
| order_date |
|---|
| 2024-03-15 |

**Solution**
```python
df.select(F.trunc("order_date", "month").alias("first_day"),
          F.last_day("order_date").alias("last_day")).show()
```

**Output**
| first_day | last_day |
|---|---|
| 2024-03-01 | 2024-03-31 |

---

### Q131. Employees with more than 5 years of tenure
**Problem:** As of 2024-12-31, find employees with tenure above 5 years.

**Input: `employees`**
| name | join_date |
|---|---|
| Amit | 2015-06-01 |
| Riya | 2021-01-10 |
| John | 2018-03-15 |

**Solution**
```python
ref = F.lit("2024-12-31").cast("date")   # use F.current_date() in real jobs
(df.withColumn("tenure_years", F.round(F.months_between(ref, "join_date") / 12, 2))
   .filter("tenure_years > 5").show())
```

**Output**
| name | join_date | tenure_years |
|---|---|---|
| Amit | 2015-06-01 | 9.58 |
| John | 2018-03-15 | 6.8 |

---

### Q132. Orders in the last 30 days
**Problem:** With reference date 2024-02-15, return orders from the last 30 days.

**Input: `orders`**
| id | dt |
|---|---|
| 1 | 2024-01-10 |
| 2 | 2024-01-25 |
| 3 | 2024-02-10 |

**Solution**
```python
ref = F.lit("2024-02-15").cast("date")
df.filter(F.col("dt") >= F.date_sub(ref, 30)).show()
```

**Output**
| id | dt |
|---|---|
| 2 | 2024-01-25 |
| 3 | 2024-02-10 |

---

### Q133. Month-wise order count with month name
**Problem:** Count orders per month and show the month name.

**Input: `orders`**
| dt |
|---|
| 2024-01-05 |
| 2024-01-20 |
| 2024-02-10 |

**Solution**
```python
(df.groupBy(F.month("dt").alias("m"), F.date_format("dt", "MMM").alias("month_name"))
   .count().orderBy("m").drop("m").show())
```

**Output**
| month_name | count |
|---|---|
| Jan | 2 |
| Feb | 1 |

---

### Q134. Generate a list of dates between two dates
**Problem:** Create one row per day from 2024-01-01 to 2024-01-04.

**Input:** (none, only start and end dates)

**Solution**
```python
# sequence() builds an array of dates, explode() turns it into rows
(spark.range(1)
   .select(F.explode(F.sequence(F.lit("2024-01-01").cast("date"),
                                F.lit("2024-01-04").cast("date"),
                                F.expr("interval 1 day"))).alias("dt")).show())
```

**Output**
| dt |
|---|
| 2024-01-01 |
| 2024-01-02 |
| 2024-01-03 |
| 2024-01-04 |

---

### Q135. Fill missing dates with zero sales
**Problem:** Show every date between the first and last sale, using 0 for days with no sales.

**Input: `sales`**
| dt | amt |
|---|---|
| 2024-01-01 | 100 |
| 2024-01-03 | 50 |

**Solution**
```python
# 1) build a calendar  2) left join the facts  3) fill nulls with 0
rng = df.agg(F.min("dt").alias("s"), F.max("dt").alias("e"))
cal = rng.select(F.explode(F.sequence("s", "e", F.expr("interval 1 day"))).alias("dt"))
(cal.join(df, "dt", "left").fillna({"amt": 0}).orderBy("dt").show())
```

**Output**
| dt | amt |
|---|---|
| 2024-01-01 | 100 |
| 2024-01-02 | 0 |
| 2024-01-03 | 50 |

---

# Section 6: Strings, Arrays, Maps and JSON

### Q136. Explode an array column
**Problem:** Convert each skill in the array into its own row.

**Input: `people`**
| id | skills |
|---|---|
| 1 | [Python, SQL] |
| 2 | [Spark] |

**Solution**
```python
# explode: one output row per array element
df.select("id", F.explode("skills").alias("skill")).show()
```

**Output**
| id | skill |
|---|---|
| 1 | Python |
| 1 | SQL |
| 2 | Spark |

---

### Q137. explode vs explode_outer
**Problem:** Keep rows whose array is empty or null.

**Input: `people`**
| id | skills |
|---|---|
| 1 | [Python] |
| 2 | null |

**Solution**
```python
# explode DROPS rows with null/empty arrays, explode_outer keeps them with NULL
df.select("id", F.explode_outer("skills").alias("skill")).show()
```

**Output**
| id | skill |
|---|---|
| 1 | Python |
| 2 | null |

---

### Q138. Split a comma separated string into rows
**Problem:** Convert the string column `tags` into one row per tag.

**Input: `posts`**
| id | tags |
|---|---|
| 1 | a,b,c |
| 2 | d |

**Solution**
```python
df.select("id", F.explode(F.split("tags", ",")).alias("tag")).show()
```

**Output**
| id | tag |
|---|---|
| 1 | a |
| 1 | b |
| 1 | c |
| 2 | d |

---

### Q139. posexplode: elements with their position
**Problem:** Explode the array and keep the index of every element.

**Input: `people`**
| id | skills |
|---|---|
| 1 | [Python, SQL] |

**Solution**
```python
# posexplode returns (pos, col). Position starts at 0
df.select("id", F.posexplode("skills").alias("pos", "skill")).show()
```

**Output**
| id | pos | skill |
|---|---|---|
| 1 | 0 | Python |
| 1 | 1 | SQL |

---

### Q140. Filter rows using an array column
**Problem:** Find people who know Spark.

**Input: `people`**
| id | skills |
|---|---|
| 1 | [Python, SQL] |
| 2 | [Spark, SQL] |

**Solution**
```python
df.filter(F.array_contains("skills", "Spark")).show()
```

**Output**
| id | skills |
|---|---|
| 2 | [Spark, SQL] |

---

### Q141. Array utility functions
**Problem:** Get the size, distinct values and sorted version of an array.

**Input: `nums`**
| id | arr |
|---|---|
| 1 | [3, 1, 3, 2] |

**Solution**
```python
df.select(F.size("arr").alias("size"),
          F.array_distinct("arr").alias("distinct"),
          F.sort_array("arr").alias("sorted")).show()
```

**Output**
| size | distinct | sorted |
|---|---|---|
| 4 | [3, 1, 2] | [1, 2, 3, 3] |

---

### Q142. Aggregate rows into one comma separated string
**Problem:** List employee names per department as a single string.

**Input: `employees`**
| name | dept |
|---|---|
| Amit | IT |
| John | IT |
| Riya | HR |

**Solution**
```python
# collect_list -> sort for determinism -> array_join to make a string
(df.groupBy("dept")
   .agg(F.array_join(F.sort_array(F.collect_list("name")), ",").alias("names")).show())
```

**Output**
| dept | names |
|---|---|
| IT | Amit,John |
| HR | Riya |

---

### Q143. Explode a map column
**Problem:** Convert a map of properties into key/value rows.

**Input: `items`**
| id | props |
|---|---|
| 1 | {a -> 1, b -> 2} |

**Solution**
```python
# exploding a map gives two columns: key and value
df.select("id", F.explode("props").alias("key", "value")).show()
```

**Output**
| id | key | value |
|---|---|---|
| 1 | a | 1 |
| 1 | b | 2 |

---

### Q144. Access fields of a struct column
**Problem:** Read city and pin from a nested address struct.

**Input: `customers`** (schema: `id INT, address STRUCT<city: STRING, pin: STRING>`)
| id | address |
|---|---|
| 1 | {Pune, 411001} |

**Solution**
```python
# dot notation, or select("address.*") to flatten all struct fields
df.select("id", F.col("address.city").alias("city"), F.col("address.pin").alias("pin")).show()
```

**Output**
| id | city | pin |
|---|---|---|
| 1 | Pune | 411001 |

---

### Q145. Parse a JSON string column
**Problem:** Convert a JSON string column into proper columns.

**Input: `raw`**
| json_str |
|---|
| {"id":1,"name":"Amit"} |

**Solution**
```python
# from_json needs a schema (DDL string works). select("j.*") expands the struct
df.select(F.from_json("json_str", "id INT, name STRING").alias("j")).select("j.*").show()
```

**Output**
| id | name |
|---|---|
| 1 | Amit |

---

### Q146. Convert columns to a JSON string
**Problem:** Create a JSON string from id and name.

**Input: `people`**
| id | name |
|---|---|
| 1 | Amit |

**Solution**
```python
df.select(F.to_json(F.struct("id", "name")).alias("json_str")).show(truncate=False)
```

**Output**
| json_str |
|---|
| {"id":1,"name":"Amit"} |

---

### Q147. Extract a value from JSON without a schema
**Problem:** Get the user's name from nested JSON text.

**Input: `raw`**
| json_str |
|---|
| {"user":{"name":"Amit","age":30}} |

**Solution**
```python
# get_json_object uses a JSONPath, handy for quick extraction of a few fields
df.select(F.get_json_object("json_str", "$.user.name").alias("name")).show()
```

**Output**
| name |
|---|
| Amit |

---

### Q148. Flatten an array of arrays
**Problem:** Convert `[[1,2],[3]]` into `[1,2,3]`.

**Input: `nested`**
| id | arr |
|---|---|
| 1 | [[1, 2], [3]] |

**Solution**
```python
df.select("id", F.flatten("arr").alias("flat")).show()
```

**Output**
| id | flat |
|---|---|
| 1 | [1, 2, 3] |

---

### Q149. Extract a pattern with regexp_extract
**Problem:** Extract the numeric part of an order code.

**Input: `orders`**
| code |
|---|
| ORD-1234-IN |
| ORD-98-US |

**Solution**
```python
# 3rd argument = capture group number (1 = first set of brackets)
df.withColumn("order_no", F.regexp_extract("code", r"ORD-(\d+)", 1)).show()
```

**Output**
| code | order_no |
|---|---|
| ORD-1234-IN | 1234 |
| ORD-98-US | 98 |

---

### Q150. Remove special characters
**Problem:** Keep only letters in the name.

**Input: `names`**
| name |
|---|
| A@m#it! |

**Solution**
```python
df.withColumn("clean", F.regexp_replace("name", r"[^A-Za-z]", "")).show()
```

**Output**
| name | clean |
|---|---|
| A@m#it! | Amit |

---

### Q151. Word count
**Problem:** Count the frequency of each word.

**Input: `lines`**
| line |
|---|
| spark is fast |
| spark is great |

**Solution**
```python
# split -> explode -> groupBy -> count (the classic word count)
(df.select(F.explode(F.split("line", " ")).alias("word"))
   .groupBy("word").count().orderBy(F.col("count").desc(), "word").show())
```

**Output**
| word | count |
|---|---|
| is | 2 |
| spark | 2 |
| fast | 1 |
| great | 1 |

---

### Q152. Palindrome check
**Problem:** Mark whether each word is a palindrome.

**Input: `words`**
| word |
|---|
| madam |
| spark |

**Solution**
```python
df.withColumn("is_palindrome", F.col("word") == F.reverse("word")).show()
```

**Output**
| word | is_palindrome |
|---|---|
| madam | true |
| spark | false |

---

### Q153. Names starting and ending with a vowel
**Problem:** Find names that start and end with a vowel.

**Input: `names`**
| name |
|---|
| Anita |
| Amit |
| Uma |
| John |

**Solution**
```python
df.filter(F.col("name").rlike(r"^[AEIOUaeiou].*[AEIOUaeiou]$")).show()
```

**Output**
| name |
|---|
| Anita |
| Uma |

---

### Q154. Count vowels in a string
**Problem:** Count the vowels of each word.

**Input: `words`**
| word |
|---|
| banana |
| spark |

**Solution**
```python
# vowels = original length - length after removing vowels
df.withColumn("vowels", F.length("word") - F.length(F.regexp_replace("word", "(?i)[aeiou]", ""))).show()
```

**Output**
| word | vowels |
|---|---|
| banana | 3 |
| spark | 1 |

---

### Q155. Capitalize the first letter of each word
**Problem:** Convert `amit sharma` to `Amit Sharma`.

**Input: `names`**
| name |
|---|
| amit sharma |

**Solution**
```python
df.withColumn("name_proper", F.initcap("name")).show()
```

**Output**
| name | name_proper |
|---|---|
| amit sharma | Amit Sharma |

---

### Q156. Extract digits from a string
**Problem:** Keep only the digits from the text.

**Input: `data`**
| txt |
|---|
| abc123def45 |

**Solution**
```python
df.withColumn("digits", F.regexp_replace("txt", r"\D", "")).show()
```

**Output**
| txt | digits |
|---|---|
| abc123def45 | 12345 |

---

### Q157. Mask an email address
**Problem:** Show only the first 2 characters of the email name, then ****.

**Input: `users`**
| email |
|---|
| amitsharma@gmail.com |
| riya@yahoo.com |

**Solution**
```python
df.withColumn("masked",
    F.concat(F.substring("email", 1, 2), F.lit("****@"), F.substring_index("email", "@", -1))).show(truncate=False)
```

**Output**
| email | masked |
|---|---|
| amitsharma@gmail.com | am****@gmail.com |
| riya@yahoo.com | ri****@yahoo.com |

---

### Q158. concat vs concat_ws with nulls
**Problem:** Build a full name when the middle name is null.

**Input: `people`**
| first | middle | last |
|---|---|---|
| Amit | null | Sharma |

**Solution**
```python
# concat returns NULL if any input is NULL; concat_ws skips NULLs
df.select(F.concat("first", F.lit(" "), "middle", F.lit(" "), "last").alias("concat_result"),
          F.concat_ws(" ", "first", "middle", "last").alias("concat_ws_result")).show()
```

**Output**
| concat_result | concat_ws_result |
|---|---|
| null | Amit Sharma |

---

### Q159. Left pad with zeros
**Problem:** Convert id to a 3 character code padded with zeros.

**Input: `data`**
| id |
|---|
| 7 |
| 42 |

**Solution**
```python
df.withColumn("code", F.lpad(F.col("id").cast("string"), 3, "0")).show()
```

**Output**
| id | code |
|---|---|
| 7 | 007 |
| 42 | 042 |

---

### Q160. Higher order functions on arrays
**Problem:** Double every number in the array and keep only the even ones.

**Input: `nums`**
| id | arr |
|---|---|
| 1 | [1, 2, 3, 4] |

**Solution**
```python
# transform / filter run a lambda on each array element WITHOUT exploding (Spark 3.1+)
df.select(F.transform("arr", lambda x: x * 2).alias("doubled"),
          F.filter("arr", lambda x: x % 2 == 0).alias("evens")).show()
```

**Output**
| doubled | evens |
|---|---|
| [2, 4, 6, 8] | [2, 4] |

---
# Section 7: Pivot, Gaps and Islands, Puzzles

### Q161. Pivot rows into columns
**Problem:** Show monthly sales of each product as separate columns.

**Input: `sales`**
| product | month | amt |
|---|---|---|
| A | Jan | 100 |
| A | Feb | 150 |
| B | Jan | 200 |

**Solution**
```python
# groupBy(row key).pivot(column to spread).agg(value)
df.groupBy("product").pivot("month").agg(F.sum("amt")).show()
```

**Output**
| product | Feb | Jan |
|---|---|---|
| A | 150 | 100 |
| B | null | 200 |

---

### Q162. Pivot with explicit values and zero fill
**Problem:** Same as Q161, with column order Jan, Feb and 0 instead of null.

**Input:** same `sales` as Q161.

**Solution**
```python
# Passing the pivot values list avoids an extra job to discover them and fixes the order
(df.groupBy("product").pivot("month", ["Jan", "Feb"]).agg(F.sum("amt")).fillna(0).show())
```

**Output**
| product | Jan | Feb |
|---|---|---|
| A | 100 | 150 |
| B | 200 | 0 |

---

### Q163. Unpivot columns into rows using stack()
**Problem:** Convert subject columns into (subject, marks) rows.

**Input: `marks`**
| id | math | sci |
|---|---|---|
| 1 | 80 | 90 |
| 2 | 70 | 60 |

**Solution**
```python
# stack(n, 'label1', col1, 'label2', col2, ...) generates n rows per input row
df.select("id", F.expr("stack(2, 'math', math, 'sci', sci) as (subject, marks)")).show()
```

**Output**
| id | subject | marks |
|---|---|---|
| 1 | math | 80 |
| 1 | sci | 90 |
| 2 | math | 70 |
| 2 | sci | 60 |

---

### Q164. Unpivot using the unpivot() API
**Problem:** Same as Q163 with the DataFrame API (Spark 3.4+).

**Input:** same `marks` as Q163.

**Solution**
```python
# unpivot(ids, values, variableColumnName, valueColumnName)
df.unpivot("id", ["math", "sci"], "subject", "marks").show()
```

**Output**
| id | subject | marks |
|---|---|---|
| 1 | math | 80 |
| 1 | sci | 90 |
| 2 | math | 70 |
| 2 | sci | 60 |

---

### Q165. Users with 3 or more consecutive login days
**Problem:** Find users who logged in at least 3 days in a row.

**Input: `logins`**
| user | dt |
|---|---|
| u1 | 2024-01-01 |
| u1 | 2024-01-02 |
| u1 | 2024-01-03 |
| u1 | 2024-01-05 |
| u2 | 2024-01-01 |
| u2 | 2024-01-03 |

**Solution**
```python
# Gaps-and-islands trick: dt minus row_number is CONSTANT for consecutive dates.
w = Window.partitionBy("user").orderBy("dt")
g = (df.dropDuplicates(["user", "dt"])
       .withColumn("grp", F.date_sub("dt", F.row_number().over(w).cast("int"))))
(g.groupBy("user", "grp").agg(F.count("*").alias("streak"))
  .filter("streak >= 3").select("user").distinct().show())
```

**Output**
| user |
|---|
| u1 |

---

### Q166. Find missing numbers in a sequence
**Problem:** Find the missing ids between the min and max id.

**Input: `ids`**
| id |
|---|
| 1 |
| 2 |
| 4 |
| 7 |

**Solution**
```python
# Generate the full range, then anti-join with the existing ids
bounds = df.agg(F.min("id").alias("lo"), F.max("id").alias("hi"))
full = bounds.select(F.explode(F.sequence("lo", "hi")).alias("id"))
full.join(df, "id", "left_anti").orderBy("id").show()
```

**Output**
| id |
|---|
| 3 |
| 5 |
| 6 |

---

### Q167. Sessionization (30 minute inactivity)
**Problem:** Start a new session when the gap between events exceeds 30 minutes.

**Input: `events`**
| user | ts |
|---|---|
| u1 | 2024-01-01 10:00:00 |
| u1 | 2024-01-01 10:10:00 |
| u1 | 2024-01-01 11:00:00 |
| u1 | 2024-01-01 11:20:00 |

**Solution**
```python
# 1) gap vs previous event  2) flag = 1 when gap > 30 min  3) cumulative sum of flags = session id
w = Window.partitionBy("user").orderBy("ts")
(df.withColumn("prev_ts", F.lag("ts").over(w))
   .withColumn("new_session",
       F.when(F.col("prev_ts").isNull() |
              ((F.col("ts").cast("long") - F.col("prev_ts").cast("long")) > 30 * 60), 1).otherwise(0))
   .withColumn("session_id", F.sum("new_session").over(w))
   .select("user", "ts", "session_id").show(truncate=False))
```

**Output**
| user | ts | session_id |
|---|---|---|
| u1 | 2024-01-01 10:00:00 | 1 |
| u1 | 2024-01-01 10:10:00 | 1 |
| u1 | 2024-01-01 11:00:00 | 2 |
| u1 | 2024-01-01 11:20:00 | 2 |

---

### Q168. Seats: 3 or more consecutive free seats
**Problem:** Return seats that belong to a run of at least 3 consecutive free seats (free = 1).

**Input: `cinema`**
| seat_id | free |
|---|---|
| 1 | 1 |
| 2 | 1 |
| 3 | 1 |
| 4 | 0 |
| 5 | 1 |
| 6 | 1 |

**Solution**
```python
# Keep free seats only; seat_id - row_number is constant inside a consecutive run
w = Window.orderBy("seat_id")
free = df.filter("free = 1").withColumn("grp", F.col("seat_id") - F.row_number().over(w))
cnt = Window.partitionBy("grp")
(free.withColumn("run_len", F.count("*").over(cnt))
     .filter("run_len >= 3").select("seat_id").orderBy("seat_id").show())
```

**Output**
| seat_id |
|---|
| 1 |
| 2 |
| 3 |

---

### Q169. Longest login streak per user
**Problem:** Find the longest consecutive login streak of every user.

**Input:** `logins` (same as Q165)

**Solution**
```python
w = Window.partitionBy("user").orderBy("dt")
g = df.withColumn("grp", F.date_sub("dt", F.row_number().over(w).cast("int")))
(g.groupBy("user", "grp").agg(F.count("*").alias("streak"))
  .groupBy("user").agg(F.max("streak").alias("longest_streak")).orderBy("user").show())
```

**Output**
| user | longest_streak |
|---|---|
| u1 | 3 |
| u2 | 1 |

---

### Q170. Cumulative hires per year
**Problem:** Show how many people joined each year and the running total.

**Input: `employees`**
| name | hire_year |
|---|---|
| A | 2020 |
| B | 2020 |
| C | 2021 |
| D | 2022 |

**Solution**
```python
w = Window.orderBy("hire_year").rowsBetween(Window.unboundedPreceding, Window.currentRow)
(df.groupBy("hire_year").agg(F.count("*").alias("hires"))
   .withColumn("cumulative", F.sum("hires").over(w)).orderBy("hire_year").show())
```

**Output**
| hire_year | hires | cumulative |
|---|---|---|
| 2020 | 2 | 2 |
| 2021 | 1 | 3 |
| 2022 | 1 | 4 |

---

### Q171. Swap values of a column
**Problem:** Swap M and F in the gender column.

**Input: `people`**
| id | gender |
|---|---|
| 1 | M |
| 2 | F |

**Solution**
```python
df.withColumn("gender", F.when(F.col("gender") == "M", "F")
                         .when(F.col("gender") == "F", "M")
                         .otherwise(F.col("gender"))).show()
```

**Output**
| id | gender |
|---|---|
| 1 | F |
| 2 | M |

---

### Q172. Start date, end date and length of each login streak
**Problem:** List all consecutive-date islands per user.

**Input:** `logins` (same as Q165)

**Solution**
```python
w = Window.partitionBy("user").orderBy("dt")
g = df.withColumn("grp", F.date_sub("dt", F.row_number().over(w).cast("int")))
(g.groupBy("user", "grp")
  .agg(F.min("dt").alias("start_dt"), F.max("dt").alias("end_dt"), F.count("*").alias("days"))
  .drop("grp").orderBy("user", "start_dt").show())
```

**Output**
| user | start_dt | end_dt | days |
|---|---|---|---|
| u1 | 2024-01-01 | 2024-01-03 | 3 |
| u1 | 2024-01-05 | 2024-01-05 | 1 |
| u2 | 2024-01-01 | 2024-01-01 | 1 |
| u2 | 2024-01-03 | 2024-01-03 | 1 |

---

### Q173. Customers who purchased in at least 2 consecutive months
**Problem:** Find customers with purchases in two or more consecutive months.

**Input: `purchases`**
| cust | month |
|---|---|
| A | 1 |
| A | 2 |
| B | 1 |
| B | 3 |

**Solution**
```python
w = Window.partitionBy("cust").orderBy("month")
g = df.withColumn("grp", F.col("month") - F.row_number().over(w))
(g.groupBy("cust", "grp").agg(F.count("*").alias("n"))
  .filter("n >= 2").select("cust").distinct().show())
```

**Output**
| cust |
|---|
| A |

---

### Q174. Users who came back the day after their first login
**Problem:** Find users with a login exactly one day after their first login.

**Input: `logins`**
| user | dt |
|---|---|
| u1 | 2024-01-01 |
| u1 | 2024-01-02 |
| u2 | 2024-01-01 |
| u3 | 2024-01-02 |

**Solution**
```python
first = df.groupBy("user").agg(F.min("dt").alias("first_dt"))
(df.join(first, "user")
   .filter(F.col("dt") == F.date_add("first_dt", 1))
   .select("user").distinct().show())
```

**Output**
| user |
|---|
| u1 |

---

### Q175. Funnel: distinct users at each step
**Problem:** Count unique users for view, cart and buy events.

**Input: `events`**
| user | event |
|---|---|
| 1 | view |
| 1 | cart |
| 1 | buy |
| 2 | view |
| 2 | cart |
| 3 | view |

**Solution**
```python
(df.groupBy("event").agg(F.countDistinct("user").alias("users"))
   .orderBy(F.col("users").desc()).show())
```

**Output**
| event | users |
|---|---|
| view | 3 |
| cart | 2 |
| buy | 1 |

---

### Q176. Customers who purchased in every month
**Problem:** Find customers who bought in all months present in the data.

**Input: `purchases`**
| cust | month |
|---|---|
| A | Jan |
| A | Feb |
| B | Jan |

**Solution**
```python
total_months = df.select("month").distinct().count()
(df.groupBy("cust").agg(F.countDistinct("month").alias("m"))
   .filter(F.col("m") == total_months).select("cust").show())
```

**Output**
| cust |
|---|
| A |

---

### Q177. Products frequently bought together
**Problem:** Count how many orders contain each pair of products.

**Input: `order_items`**
| order_id | product |
|---|---|
| 1 | Pen |
| 1 | Book |
| 2 | Pen |
| 2 | Book |
| 2 | Bag |

**Solution**
```python
# Self join on order_id; a.product < b.product keeps each pair once
a = df.alias("a"); b = df.alias("b")
(a.join(b, (F.col("a.order_id") == F.col("b.order_id")) & (F.col("a.product") < F.col("b.product")))
  .groupBy(F.col("a.product").alias("p1"), F.col("b.product").alias("p2"))
  .agg(F.count("*").alias("times")).orderBy(F.col("times").desc(), "p1").show())
```

**Output**
| p1 | p2 | times |
|---|---|---|
| Book | Pen | 2 |
| Bag | Book | 1 |
| Bag | Pen | 1 |

---

### Q178. Customer cohorts by first purchase month
**Problem:** Count customers per first-purchase month.

**Input: `orders`**
| cust | dt |
|---|---|
| A | 2024-01-05 |
| A | 2024-02-10 |
| B | 2024-02-01 |

**Solution**
```python
first = df.groupBy("cust").agg(F.min("dt").alias("first_dt"))
(first.groupBy(F.date_format("first_dt", "yyyy-MM").alias("cohort"))
      .agg(F.count("*").alias("customers")).orderBy("cohort").show())
```

**Output**
| cohort | customers |
|---|---|
| 2024-01 | 1 |
| 2024-02 | 1 |

---

### Q179. Running account balance
**Problem:** Compute the balance after each transaction (deposit adds, withdraw subtracts).

**Input: `txns`**
| dt | type | amt |
|---|---|---|
| 1 | deposit | 100 |
| 2 | withdraw | 30 |
| 3 | deposit | 50 |

**Solution**
```python
w = Window.orderBy("dt").rowsBetween(Window.unboundedPreceding, Window.currentRow)
(df.withColumn("signed", F.when(F.col("type") == "deposit", F.col("amt")).otherwise(-F.col("amt")))
   .withColumn("balance", F.sum("signed").over(w)).drop("signed").show())
```

**Output**
| dt | type | amt | balance |
|---|---|---|---|
| 1 | deposit | 100 | 100 |
| 2 | withdraw | 30 | 70 |
| 3 | deposit | 50 | 120 |

---

### Q180. Overlapping meetings
**Problem:** Find pairs of meetings that overlap in time.

**Input: `meetings`**
| id | start_hr | end_hr |
|---|---|---|
| 1 | 9 | 11 |
| 2 | 10 | 12 |
| 3 | 13 | 14 |

**Solution**
```python
# Two intervals overlap when a.start < b.end AND b.start < a.end
a = df.alias("a"); b = df.alias("b")
(a.join(b, (F.col("a.id") < F.col("b.id")) &
           (F.col("a.start_hr") < F.col("b.end_hr")) &
           (F.col("b.start_hr") < F.col("a.end_hr")))
  .select(F.col("a.id").alias("meeting1"), F.col("b.id").alias("meeting2")).show())
```

**Output**
| meeting1 | meeting2 |
|---|---|
| 1 | 2 |

---

### Q181. Employee hierarchy level
**Problem:** Compute the level of each employee in the reporting chain (CEO = 1).

**Input: `employees`**
| emp_id | name | manager_id |
|---|---|---|
| 1 | Amit | null |
| 2 | Riya | 1 |
| 3 | John | 2 |

**Solution**
```python
# Spark SQL has no recursive CTE, so iterate: join children to the previous level
level = df.filter("manager_id is null").select("emp_id", "name").withColumn("level", F.lit(1))
result, lvl = level, 1
while True:
    nxt = (df.alias("e").join(level.alias("p"), F.col("e.manager_id") == F.col("p.emp_id"))
             .select(F.col("e.emp_id"), F.col("e.name"), F.lit(lvl + 1).alias("level")))
    if nxt.rdd.isEmpty(): break
    result = result.unionByName(nxt); level = nxt; lvl += 1
result.orderBy("level").show()
```

**Output**
| emp_id | name | level |
|---|---|---|
| 1 | Amit | 1 |
| 2 | Riya | 2 |
| 3 | John | 3 |

---

### Q182. Top 25 percent customers by spend
**Problem:** Return the customers in the top quartile of spend.

**Input: `spend`**
| cust | amount |
|---|---|
| A | 100 |
| B | 200 |
| C | 300 |
| D | 400 |

**Solution**
```python
w = Window.orderBy(F.col("amount").desc())
(df.withColumn("quartile", F.ntile(4).over(w)).filter("quartile = 1").drop("quartile").show())
```

**Output**
| cust | amount |
|---|---|
| D | 400 |

---

### Q183. Median salary per department
**Problem:** Find the exact median salary in each department.

**Input: `employees`**
| dept | salary |
|---|---|
| IT | 50000 |
| IT | 60000 |
| IT | 70000 |
| HR | 40000 |
| HR | 45000 |

**Solution**
```python
# percentile() as an aggregate expression gives the exact median
df.groupBy("dept").agg(F.expr("percentile(salary, 0.5)").alias("median_salary")).show()
```

**Output**
| dept | median_salary |
|---|---|
| IT | 60000.0 |
| HR | 42500.0 |

---

### Q184. Second order of every customer
**Problem:** Get each customer's 2nd order.

**Input: `orders`**
| cust | dt |
|---|---|
| A | 2024-01-01 |
| A | 2024-01-05 |
| B | 2024-01-02 |

**Solution**
```python
w = Window.partitionBy("cust").orderBy("dt")
(df.withColumn("rn", F.row_number().over(w)).filter("rn = 2").drop("rn").show())
```

**Output**
| cust | dt |
|---|---|
| A | 2024-01-05 |

---

### Q185. Customers with multiple orders on the same day
**Problem:** Find customers who placed more than one order on a single day.

**Input: `orders`**
| order_id | cust | dt |
|---|---|---|
| 1 | A | 2024-01-01 |
| 2 | A | 2024-01-01 |
| 3 | B | 2024-01-01 |
| 4 | A | 2024-01-02 |

**Solution**
```python
(df.groupBy("cust", "dt").agg(F.count("*").alias("orders_that_day"))
   .filter("orders_that_day > 1").show())
```

**Output**
| cust | dt | orders_that_day |
|---|---|---|
| A | 2024-01-01 | 2 |

---

# Section 8: Performance, I/O and ETL

### Q186. repartition vs coalesce
**Problem:** Increase a DataFrame to 8 partitions, then reduce it to 2.

**Input:** a DataFrame with 4 partitions (`df.rdd.getNumPartitions() == 4`).

**Solution**
```python
# repartition(n): full shuffle, can increase or decrease, evenly sized partitions
# coalesce(n):    NO full shuffle, can only decrease, may leave uneven partitions
print(df.repartition(8).rdd.getNumPartitions())   # 8
print(df.coalesce(2).rdd.getNumPartitions())      # 2
# Use coalesce before writing to reduce small files; repartition(col) to co-locate keys
```

**Output**
| operation | partitions |
|---|---|
| original | 4 |
| repartition(8) | 8 |
| coalesce(2) | 2 |

---

### Q187. cache() and persist()
**Problem:** Reuse an expensive DataFrame for two different aggregations without recomputing it.

**Input: `orders`** (large table, read and filtered once)

**Solution**
```python
from pyspark import StorageLevel
base = spark.read.parquet("/data/orders").filter("status = 'delivered'")
base.persist(StorageLevel.MEMORY_AND_DISK)   # cache() = MEMORY_AND_DISK for DataFrames
base.count()                                 # an ACTION is needed to materialize the cache
by_cust = base.groupBy("cust").count()
by_day  = base.groupBy("dt").count()
# ... use both ...
base.unpersist()                             # always release memory when done
```

**Output**
| step | behaviour |
|---|---|
| first action | computes and stores the data |
| later actions | read from cache, no re-read of parquet |

---

### Q188. Map codes to names with a lookup dictionary
**Problem:** Replace country codes using a small Python dictionary.

**Input: `orders`**
| id | code |
|---|---|
| 1 | IN |
| 2 | US |
| 3 | UK |

**Solution**
```python
# create_map builds a Spark map literal, usable on executors without a UDF
from itertools import chain
lookup = {"IN": "India", "US": "USA"}
mapping = F.create_map([F.lit(x) for x in chain(*lookup.items())])
df.withColumn("country", F.coalesce(mapping[F.col("code")], F.lit("Unknown"))).show()
```

**Output**
| id | code | country |
|---|---|---|
| 1 | IN | India |
| 2 | US | USA |
| 3 | UK | Unknown |

---

### Q189. UDF vs built-in functions
**Problem:** Label ages as "Adult" (>= 18) or "Minor", first with a UDF, then without one.

**Input: `people`**
| name | age |
|---|---|
| Amit | 25 |
| Riya | 15 |

**Solution**
```python
# Python UDFs are slow (row by row serialization, optimizer cannot see inside)
from pyspark.sql.functions import udf
@udf("string")
def label_udf(age): return "Adult" if age >= 18 else "Minor"
df.withColumn("label_udf", label_udf("age")).show()

# Preferred: built-in expression, same result and much faster
df.withColumn("label", F.when(F.col("age") >= 18, "Adult").otherwise("Minor")).show()
```

**Output**
| name | age | label |
|---|---|---|
| Amit | 25 | Adult |
| Riya | 15 | Minor |

---

### Q190. Pandas UDF (vectorized)
**Problem:** Add a 10 percent hike to salary using a pandas UDF.

**Input: `employees`**
| name | salary |
|---|---|
| Amit | 50000.0 |
| Riya | 40000.0 |

**Solution**
```python
# pandas_udf processes whole batches via Apache Arrow, far faster than a row UDF
import pandas as pd
from pyspark.sql.functions import pandas_udf

@pandas_udf("double")
def hike(s: pd.Series) -> pd.Series:
    return s * 1.10

df.withColumn("new_salary", hike("salary")).show()
```

**Output**
| name | salary | new_salary |
|---|---|---|
| Amit | 50000.0 | 55000.0 |
| Riya | 40000.0 | 44000.0 |

---

### Q191. Records per partition (check data skew)
**Problem:** Find how many rows each partition holds.

**Input: `df`** (6 rows, 3 partitions)

**Solution**
```python
# spark_partition_id() tells which partition each row lives in. Big gaps = skew
(df.withColumn("pid", F.spark_partition_id())
   .groupBy("pid").count().orderBy("pid").show())
```

**Output**
| pid | count |
|---|---|
| 0 | 2 |
| 1 | 2 |
| 2 | 2 |

---

### Q192. Write partitioned Parquet and read with partition pruning
**Problem:** Save sales partitioned by region and read only the East data.

**Input: `sales`**
| id | region | amt |
|---|---|---|
| 1 | East | 100 |
| 2 | West | 200 |
| 3 | East | 300 |

**Solution**
```python
# Partition columns become folders: /out/sales/region=East/..., region=West/...
df.write.mode("overwrite").partitionBy("region").parquet("/out/sales")

# Filtering on the partition column reads ONLY the matching folder (partition pruning)
spark.read.parquet("/out/sales").filter("region = 'East'").show()
```

**Output**
| id | amt | region |
|---|---|---|
| 1 | 100 | East |
| 3 | 300 | East |

---

### Q193. Handling data skew with salting
**Problem:** One key ("A") has most of the rows. Aggregate it without overloading one task.

**Input: `events`**
| key | val |
|---|---|
| A | 1 |
| A | 2 |
| A | 3 |
| B | 4 |

**Solution**
```python
# Two-stage aggregation: add a random salt to split a hot key over several tasks,
# aggregate per (key, salt), then aggregate again per key
SALT = 4
salted = df.withColumn("salt", (F.rand() * SALT).cast("int"))
stage1 = salted.groupBy("key", "salt").agg(F.sum("val").alias("partial"))
stage2 = stage1.groupBy("key").agg(F.sum("partial").alias("total"))
stage2.orderBy("key").show()
# For joins: salt the big side, explode the small side with all salt values
# (Spark 3 AQE also handles skew: spark.sql.adaptive.skewJoin.enabled=true)
```

**Output**
| key | total |
|---|---|
| A | 6 |
| B | 4 |

---

### Q194. Read an explain plan
**Problem:** Check whether a filter is pushed down to the Parquet scan.

**Input:** `orders` parquet file with columns `id, status`

**Solution**
```python
df = spark.read.parquet("/data/orders").filter("status = 'delivered'").select("id")
df.explain(True)   # parsed, analyzed, optimized and physical plans
```

**Output** (key lines of the physical plan)
```
== Physical Plan ==
*(1) Project [id]
+- *(1) Filter (isnotnull(status) AND (status = delivered))
   +- *(1) FileScan parquet [id,status]
        PushedFilters: [IsNotNull(status), EqualTo(status,delivered)]
        ReadSchema: struct<id:int,status:string>
```
Look for: `PushedFilters` (predicate pushdown), `ReadSchema` (column pruning), `BroadcastHashJoin` vs `SortMergeJoin`, and `Exchange` (a shuffle).

---

### Q195. Read Parquet files with different schemas
**Problem:** Combine two Parquet folders where the newer one has an extra column.

**Input: `/data/v1`** (columns id, name) and **`/data/v2`** (columns id, name, age)
| folder | id | name | age |
|---|---|---|---|
| v1 | 1 | Amit | - |
| v2 | 2 | Riya | 30 |

**Solution**
```python
# mergeSchema builds the union of all file schemas; missing columns become NULL
df = spark.read.option("mergeSchema", "true").parquet("/data/v1", "/data/v2")
df.show()
```

**Output**
| id | name | age |
|---|---|---|
| 1 | Amit | null |
| 2 | Riya | 30 |

---

### Q196. Handle corrupt records while reading JSON
**Problem:** Read a JSON file containing a malformed line without failing the job.

**Input: `/data/in.json`**
```
{"id":1,"name":"Amit"}
{bad json line}
```

**Solution**
```python
# PERMISSIVE (default) puts the bad line into _corrupt_record.
# The column must be part of the schema. Other modes: DROPMALFORMED, FAILFAST
schema = StructType([
    StructField("id", IntegerType()), StructField("name", StringType()),
    StructField("_corrupt_record", StringType())])
df = spark.read.schema(schema).option("mode", "PERMISSIVE").json("/data/in.json")
df.cache()      # needed before filtering only on _corrupt_record in some Spark versions
df.show(truncate=False)
```

**Output**
| id | name | _corrupt_record |
|---|---|---|
| 1 | Amit | null |
| null | null | {bad json line} |

---

### Q197. Overwrite only the partitions present in new data
**Problem:** Reload data for one date partition without deleting the other dates.

**Input: `existing folder`** has partitions dt=2024-01-01 and dt=2024-01-02. New data contains only dt=2024-01-02.

**Solution**
```python
# Default "static" mode would delete ALL partitions on overwrite.
# "dynamic" replaces only the partitions that appear in the incoming DataFrame
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
new_df.write.mode("overwrite").partitionBy("dt").parquet("/out/events")
```

**Output**
| partition | after the write |
|---|---|
| dt=2024-01-01 | untouched |
| dt=2024-01-02 | replaced with new data |

---

### Q198. selectExpr for quick transformations
**Problem:** Using SQL expressions, upper-case the name and compute annual salary.

**Input: `employees`**
| id | name | salary |
|---|---|---|
| 1 | amit | 50000 |

**Solution**
```python
# selectExpr accepts SQL snippets, convenient for many small transformations
df.selectExpr("id", "upper(name) as name", "salary * 12 as annual_salary").show()
```

**Output**
| id | name | annual_salary |
|---|---|---|
| 1 | AMIT | 600000 |

---

### Q199. Read a messy CSV with options
**Problem:** Read a pipe-delimited CSV with header, "NA" as null and a custom date format.

**Input: `/data/orders.csv`**
```
id|amount|order_date
1|100|15-01-2024
2|NA|16-01-2024
```

**Solution**
```python
df = (spark.read
      .option("header", True)
      .option("delimiter", "|")
      .option("nullValue", "NA")
      .option("dateFormat", "dd-MM-yyyy")
      .schema("id INT, amount INT, order_date DATE")
      .csv("/data/orders.csv"))
df.show()
```

**Output**
| id | amount | order_date |
|---|---|---|
| 1 | 100 | 2024-01-15 |
| 2 | null | 2024-01-16 |

---

### Q200. End to end mini ETL pipeline
**Problem:** Read raw orders, clean them, enrich with customers, aggregate revenue per city and write the result partitioned by city.

**Input: `raw_orders`**
| order_id | cust_id | amount | status |
|---|---|---|---|
| 1 | 1 | "100" | delivered |
| 1 | 1 | "100" | delivered |
| 2 | 2 | "200" | delivered |
| 3 | 1 | null | delivered |
| 4 | 2 | "50" | cancelled |

**Input: `customers`**
| cust_id | name | city |
|---|---|---|
| 1 | Amit | Pune |
| 2 | Riya | Mumbai |

**Solution**
```python
# 1) Clean: cast types, drop duplicates, drop null amounts, keep delivered orders
clean = (raw_orders
         .withColumn("amount", F.col("amount").cast("double"))
         .dropDuplicates(["order_id"])
         .dropna(subset=["amount"])
         .filter(F.col("status") == "delivered"))

# 2) Enrich: broadcast the small dimension table
enriched = clean.join(F.broadcast(customers), "cust_id", "left")

# 3) Aggregate
result = (enriched.groupBy("city")
          .agg(F.sum("amount").alias("revenue"), F.countDistinct("order_id").alias("orders")))

# 4) Load: write partitioned Parquet (idempotent overwrite)
result.write.mode("overwrite").partitionBy("city").parquet("/out/city_revenue")
result.show()
```

**Output**
| city | revenue | orders |
|---|---|---|
| Pune | 100.0 | 1 |
| Mumbai | 200.0 | 1 |

---

*End of 200 questions. Tip: for every query, be ready to explain the shuffle it causes, why you chose a window function over a self join, and how it behaves on skewed or null data.*

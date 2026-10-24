# 200 Pandas, NumPy & Scikit-learn Interview Questions for Data Engineers (2 Years Experience)

Real-industry, scenario-driven questions with answers an experienced professional would give.

## Table of Contents

| Section | Questions |
|---|---|
| 1. Pandas Fundamentals, Cleaning & I/O | Q1 – Q50 |
| 2. Pandas Advanced: GroupBy, Joins, Time Series, Performance, Pipelines | Q51 – Q100 |
| 3. NumPy | Q101 – Q135 |
| 4. Scikit-learn | Q136 – Q185 |
| 5. End-to-End Industry Scenarios | Q186 – Q200 |

---

# SECTION 1: PANDAS FUNDAMENTALS, CLEANING & I/O

### Q1. What is the difference between a Series, a DataFrame, and an Index?
**Answer:** A Series is a labeled 1-D array; a DataFrame is a 2-D table of Series sharing one index; an Index is the immutable label array used for alignment and lookup. Pandas aligns operations on index labels, not positions, which is a common source of surprising NaNs.

---

### Q2. What are the common ways to create a DataFrame?
**Answer:**
```python
import pandas as pd
pd.DataFrame({"a": [1, 2], "b": ["x", "y"]})            # dict of columns
pd.DataFrame([{"a": 1, "b": "x"}, {"a": 2, "b": "y"}])  # list of records (API/JSON)
pd.DataFrame(rows, columns=["a", "b"])                  # list of tuples from a DB cursor
pd.read_csv("f.csv"); pd.read_parquet("f.parquet")
```

---

### Q3. You receive a new dataset. How do you take a first look at it?
**Answer:**
```python
df.shape; df.head(); df.sample(5)
df.info(memory_usage="deep")      # dtypes, non-null counts, memory
df.describe(include="all")        # summary stats
df.isna().sum()                   # nulls per column
df.nunique()                      # cardinality
df.duplicated().sum()             # duplicate rows
```
Then check key uniqueness, date ranges, and value distributions of important columns before writing any transformation.

---

### Q4. How do you check and change data types?
**Answer:** `df.dtypes`, `df.astype({"id": "int64", "city": "category"})`, `pd.to_numeric`, `pd.to_datetime`, and `df.convert_dtypes()` for nullable dtypes. Fix types at ingestion so downstream joins and aggregations behave consistently.

---

### Q5. `loc` vs `iloc` vs `at` vs `iat`?
**Answer:** `loc` is label-based (slice end inclusive), `iloc` is position-based (end exclusive), `at`/`iat` are fast scalar accessors by label/position.
```python
df.loc[df.age > 30, ["name", "age"]]
df.iloc[:5, :2]
df.at[10, "name"]
```

---

### Q6. How do you filter with multiple conditions? What is the common mistake?
**Answer:** Use `&`, `|`, `~` with parentheses around each condition; `and`/`or` raise errors on Series.
```python
df[(df.country == "IN") & (df.amount > 1000) | df.vip]   # precedence: & before |
df[((df.country == "IN") & (df.amount > 1000)) | df.vip] # be explicit
```

---

### Q7. `query()` vs boolean indexing?
**Answer:** `query` is more readable for long conditions and can reference variables with `@`:
```python
min_amt = 1000
df.query("country == 'IN' and amount > @min_amt")
```
Boolean indexing is more flexible for programmatically built masks, and column names with spaces need backticks in `query`.

---

### Q8. Explain `set_index`, `reset_index`, and MultiIndex. When is an index useful?
**Answer:** `set_index` promotes a column to the index (fast label lookups, alignment, time-series ops); `reset_index` moves it back to a column. MultiIndex has hierarchical labels, produced by `groupby` on multiple keys. Call `reset_index()` before writing to databases or CSV so the keys aren't lost.

---

### Q9. How do you select columns by type or name pattern?
**Answer:**
```python
df.select_dtypes(include="number")
df.select_dtypes(exclude="object")
df.filter(regex="^amt_")        # columns starting with amt_
df.filter(like="date")          # columns containing "date"
```

---

### Q10. `rename` and `drop`?
**Answer:**
```python
df = df.rename(columns={"Cust ID": "customer_id"})
df = df.drop(columns=["tmp1", "tmp2"], errors="ignore")
df = df.drop(index=[0, 5])
```
Most pandas methods return new objects; `inplace=True` isn't faster and is discouraged.

---

### Q11. What is chained assignment and the `SettingWithCopyWarning`?
**Answer:** `df[df.a > 1]["b"] = 0` may modify a temporary copy, so nothing changes. Use a single `.loc` call: `df.loc[df.a > 1, "b"] = 0`. With Copy-on-Write (default in pandas 3.0, opt-in in 2.x) chained assignment never updates the parent frame.

---

### Q12. How do you find missing data per column and as a percentage?
**Answer:**
```python
df.isna().sum()
(df.isna().mean() * 100).round(2).sort_values(ascending=False)
```
Missingness patterns matter: random, systematic (one source system) or structural (field not applicable).

---

### Q13. Fill missing values using group-wise statistics.
**Answer:**
```python
df["salary"] = df["salary"].fillna(df.groupby("dept")["salary"].transform("median"))
```
`transform` returns a Series aligned to the original index, so it fills correctly. Add an `is_imputed` flag column when the fill matters for audit.

---

### Q14. How do you handle missing values in time series?
**Answer:** Forward fill (`ffill`) for last-known-value data like prices, `interpolate(method="time")` for continuous sensor data, and leave NaN where fabricating values would be misleading. Use `limit=` to avoid filling long gaps.

---

### Q15. `dropna` options?
**Answer:** `how="any"/"all"`, `subset=[cols]`, `thresh=n` (keep rows with at least n non-null). Prefer `subset` for required business keys instead of dropping rows with any null anywhere.

---

### Q16. Find and remove duplicates according to a business rule.
**Answer:**
```python
df[df.duplicated(["order_id"], keep=False)].sort_values("order_id")  # inspect first
df = df.sort_values("updated_at").drop_duplicates("order_id", keep="last")
```
Always investigate why duplicates exist (retries, replays, source bugs) rather than just dropping them.

---

### Q17. `map` vs `apply` vs `replace` vs `applymap`/`DataFrame.map`?
**Answer:**
- `Series.map(dict/func)`: element-wise; unmapped dict keys become NaN.
- `Series.replace`: unmapped values stay unchanged.
- `Series.apply(func)`: element-wise function.
- `DataFrame.apply(func, axis=0/1)`: per column or per row.
- `DataFrame.map` (formerly `applymap`): element-wise over the whole frame.

Prefer vectorized or built-in operations over any of these when possible.

---

### Q18. Create a conditional column (e.g., customer tier based on spend).
**Answer:**
```python
import numpy as np
conds = [df.spend >= 100_000, df.spend >= 50_000, df.spend >= 10_000]
df["tier"] = np.select(conds, ["Platinum", "Gold", "Silver"], default="Bronze")
df["is_high"] = np.where(df.spend > 50_000, 1, 0)
```

---

### Q19. Common `.str` cleaning operations and regex extraction?
**Answer:**
```python
df["name"] = df["name"].str.strip().str.title()
df["phone"] = df["phone"].str.replace(r"\D", "", regex=True)
df["domain"] = df["email"].str.extract(r"@([\w.-]+)$")
df["has_gmail"] = df["email"].str.contains("gmail", case=False, na=False)
```
Pass `na=False` to `contains` so NaN doesn't break boolean filtering.

---

### Q20. Split one column into several.
**Answer:**
```python
df[["first", "last"]] = df["full_name"].str.split(" ", n=1, expand=True)
```

---

### Q21. A numeric column contains text like "N/A" and "1,200". How do you convert it?
**Answer:**
```python
df["amount"] = pd.to_numeric(df["amount"].astype(str).str.replace(",", ""), errors="coerce")
```
`errors="coerce"` turns bad values to NaN; count and log how many were coerced (`df["amount"].isna().sum()`) so bad data isn't silently hidden.

---

### Q22. How do you parse dates robustly?
**Answer:**
```python
df["d"] = pd.to_datetime(df["d"], format="%d-%m-%Y", errors="coerce")
df["d"] = pd.to_datetime(df["d"], utc=True)         # tz-aware
```
Always pass an explicit `format` for speed and to avoid day/month ambiguity (01/02/2026 may be 1 Feb or 2 Jan).

---

### Q23. Useful `.dt` accessor features?
**Answer:**
```python
df["year"] = df.d.dt.year
df["month"] = df.d.dt.month
df["dow"] = df.d.dt.dayofweek          # Monday = 0
df["is_weekend"] = df.d.dt.dayofweek >= 5
df["month_start"] = df.d.dt.to_period("M").dt.to_timestamp()
df["days_since"] = (pd.Timestamp.today() - df.d).dt.days
```

---

### Q24. Sorting techniques?
**Answer:**
```python
df.sort_values(["dept", "salary"], ascending=[True, False])
df.nlargest(10, "salary")
df.sort_index()
```
`sort_values(kind="stable")` (or `mergesort`) keeps the original order of ties.

---

### Q25. `value_counts` options?
**Answer:** `df.status.value_counts(normalize=True, dropna=False)` gives proportions including NaN, which is great for quickly spotting skew or unexpected categories.

---

### Q26. `nunique()` vs `unique()`?
**Answer:** `unique()` returns the distinct values array; `nunique()` returns their count (NaN excluded by default, `dropna=False` to include). Use `df.groupby("k").nunique()` to detect keys mapped to multiple values.

---

### Q27. Ranking methods?
**Answer:** `rank(method="average"|"min"|"max"|"first"|"dense")`. `dense` mirrors SQL `DENSE_RANK`, `min` mirrors `RANK`, `first` breaks ties by order of appearance (like `ROW_NUMBER`).

---

### Q28. How do you cap outliers (winsorize)?
**Answer:**
```python
lo, hi = df.amount.quantile([0.01, 0.99])
df["amount_capped"] = df.amount.clip(lo, hi)
```
Keep the original column too, as capping changes totals and should be a documented decision.

---

### Q29. What is the `category` dtype, and when is ordered category useful?
**Answer:** Stores values as integer codes plus a categories lookup, saving memory for low-cardinality columns. An ordered category (`pd.CategoricalDtype(["low","med","high"], ordered=True)`) allows comparisons (`df.risk >= "med"`) and correct sorting.

---

### Q30. Which `read_csv` parameters do you use most in production?
**Answer:**
```python
pd.read_csv(
    "f.csv",
    usecols=["id", "amount", "dt"],
    dtype={"id": "string", "amount": "float64"},
    parse_dates=["dt"],
    na_values=["NA", "null", ""],
    encoding="utf-8-sig",
    on_bad_lines="warn",
    chunksize=500_000,            # optional, returns an iterator
)
```
Reading IDs as strings preserves leading zeros (zip codes, account numbers).

---

### Q31. Scenario: a 15 GB CSV needs a daily summary. How do you read it?
**Answer:**
```python
totals = []
for chunk in pd.read_csv("big.csv", usecols=["dt", "amount"], chunksize=1_000_000):
    totals.append(chunk.groupby("dt")["amount"].sum())
out = pd.concat(totals).groupby(level=0).sum()
```
Aggregate per chunk, then combine partial results. Not every aggregate combines (median doesn't); use Parquet, DuckDB, or Spark for those.

---

### Q32. How do you read JSON (nested or line-delimited)?
**Answer:**
```python
pd.read_json("events.jsonl", lines=True)
pd.json_normalize(payload, record_path="items", meta=["order_id", ["user", "id"]], sep="_")
```

---

### Q33. How do you read from and write to a database?
**Answer:**
```python
from sqlalchemy import create_engine
eng = create_engine("postgresql+psycopg2://user:pwd@host/db")
df = pd.read_sql("select * from orders where dt = %(d)s", eng, params={"d": "2026-10-24"})
df.to_sql("orders_stg", eng, if_exists="append", index=False, chunksize=10_000, method="multi")
```
For millions of rows use the database's bulk loader (`COPY`) instead of `to_sql`.

---

### Q34. `to_parquet` vs `to_csv`: why Parquet?
**Answer:** Parquet is columnar, compressed, typed (schema preserved), and supports column and row-group pruning, so it's typically several times smaller and much faster to read. CSV loses types and has parsing ambiguity (dates, nulls, delimiters).
```python
df.to_parquet("out.parquet", engine="pyarrow", compression="snappy", index=False)
```

---

### Q35. How do you read and write multiple Excel sheets?
**Answer:**
```python
sheets = pd.read_excel("f.xlsx", sheet_name=None)       # dict of DataFrames
with pd.ExcelWriter("out.xlsx", engine="openpyxl") as w:
    df1.to_excel(w, sheet_name="Summary", index=False)
    df2.to_excel(w, sheet_name="Detail", index=False)
```
Excel has a 1,048,576-row limit, so don't use it as a data interchange format for big data.

---

### Q36. What causes `DtypeWarning: Columns have mixed types` and how do you fix it?
**Answer:** Pandas infers types per chunk of a large file, so a column with numbers and text gets mixed types. Fix with explicit `dtype=` (e.g., `str`), `low_memory=False`, or clean the column after reading.

---

### Q37. Why do joins silently fail to match and how do you prevent it?
**Answer:** Key mismatches from whitespace, case, type (int vs str), or leading zeros. Standardize before joining:
```python
for d in (a, b):
    d["key"] = d["key"].astype("string").str.strip().str.upper()
```
Then check match rate after the join (`indicator=True`).

---

### Q38. Standardize column names across messy source files.
**Answer:**
```python
df.columns = (df.columns.str.strip().str.lower()
              .str.replace(r"[^0-9a-z]+", "_", regex=True).str.strip("_"))
```

---

### Q39. How do you detect inconsistent values in a categorical column (e.g., "IN", "India", "india")?
**Answer:** Look at `value_counts()`, normalize case and whitespace, and map variants to canonical values with a mapping dict maintained as reference data. Log unmapped values for review instead of dropping them.

---

### Q40. Scenario: an `amount` column has values like "$1,200.50", "(300)", "-". Clean it.
**Answer:**
```python
s = df["amount"].astype(str).str.strip()
neg = s.str.startswith("(") & s.str.endswith(")")
s = s.str.replace(r"[$,()]", "", regex=True).replace({"-": np.nan, "": np.nan})
df["amount"] = pd.to_numeric(s, errors="coerce")
df.loc[neg, "amount"] *= -1          # accounting negative format
```

---

### Q41. A date column has mixed formats. What do you do?
**Answer:** Try the known formats in order and combine, flagging whatever remains unparsed.
```python
d = pd.to_datetime(df.d, format="%Y-%m-%d", errors="coerce")
d = d.fillna(pd.to_datetime(df.d, format="%d/%m/%Y", errors="coerce"))
df["d"] = d
bad = df[d.isna() & df.d.notna()]
```

---

### Q42. Extract the domain from emails and flag invalid ones.
**Answer:**
```python
pat = r"^[\w.+-]+@([\w-]+\.[\w.-]+)$"
df["domain"] = df.email.str.extract(pat, expand=False)
df["valid_email"] = df.domain.notna()
```

---

### Q43. `stack` and `unstack`?
**Answer:** `unstack` pivots an index level into columns; `stack` does the reverse. Useful after a multi-key `groupby` to get a wide report: `df.groupby(["region", "month"])["amt"].sum().unstack("month")`.

---

### Q44. `pivot_table` with totals and fill values.
**Answer:**
```python
pd.pivot_table(df, index="region", columns="month", values="amt",
               aggfunc="sum", fill_value=0, margins=True)
```
`pivot` fails on duplicate index/column pairs, while `pivot_table` aggregates them.

---

### Q45. What is `crosstab` used for?
**Answer:** Frequency tables of two or more categorical variables: `pd.crosstab(df.channel, df.converted, normalize="index")` gives a conversion rate per channel.

---

### Q46. `cut` vs `qcut`?
**Answer:** `cut` makes bins with fixed edges (age groups); `qcut` makes bins with equal counts (quartiles, deciles). Use `qcut(..., duplicates="drop")` when many identical values collapse the quantile edges.

---

### Q47. How do you sample data, including stratified samples?
**Answer:**
```python
df.sample(n=1000, random_state=42)
df.groupby("label", group_keys=False).apply(lambda g: g.sample(frac=0.1, random_state=42))
```
Always set `random_state` so results are reproducible.

---

### Q48. What is method chaining with `assign` and `pipe`?
**Answer:**
```python
out = (df
       .query("status == 'ACTIVE'")
       .assign(total=lambda x: x.qty * x.price,
               month=lambda x: x.dt.dt.to_period("M"))
       .pipe(add_flags)
       .groupby("month", as_index=False)["total"].sum())
```
Chains avoid intermediate variables and make each step testable via small pure functions.

---

### Q49. Why avoid `iterrows`, and what are the alternatives?
**Answer:** `iterrows` creates a Series per row and loses dtypes; it's very slow. Alternatives, fastest first: vectorized operations, `np.where`/`np.select`, `map`, `itertuples()`, then `apply(axis=1)` as a last resort.

---

### Q50. What does `df.eval()` do?
**Answer:** Evaluates string expressions efficiently, using `numexpr` when installed, for large frames: `df.eval("total = qty * price")`. Gains appear for big arrays with several operations; for small data the overhead isn't worth it.

---

# SECTION 2: PANDAS ADVANCED — GROUPBY, JOINS, TIME SERIES, PERFORMANCE, PIPELINES

### Q51. Explain `agg` vs `transform` vs `apply` vs `filter` in groupby.
**Answer:**
- `agg`: reduces each group to one row (sum, mean).
- `transform`: returns a result the same length as the input (group mean added to every row, percent-of-group).
- `filter`: keeps or drops whole groups by a condition.
- `apply`: flexible but slowest; use only when the others can't express it.
```python
df["pct"] = df.amt / df.groupby("region")["amt"].transform("sum")
big = df.groupby("cust").filter(lambda g: len(g) >= 5)
```

---

### Q52. Multiple and custom aggregations with clean column names.
**Answer:**
```python
df.groupby("region").agg(
    revenue=("amt", "sum"),
    orders=("order_id", "nunique"),
    p90=("amt", lambda s: s.quantile(0.9)),
).reset_index()
```

---

### Q53. Keep only customers with at least 5 orders.
**Answer:**
```python
cnt = df.groupby("cust")["order_id"].transform("count")
df = df[cnt >= 5]            # faster than groupby().filter() on big data
```

---

### Q54. `first`, `last`, `nth`, and `head` per group?
**Answer:** `groupby().first()` returns the first non-null value per column; `nth(0)` and `head(1)` return rows in existing order. Sort first, since "first" means first by current order, not by time.

---

### Q55. Rolling, expanding, and exponentially weighted windows?
**Answer:**
```python
df["ma7"]   = df.groupby("sku")["qty"].transform(lambda s: s.rolling(7, min_periods=1).mean())
df["cum_max"] = df.groupby("sku")["qty"].cummax()
df["ewm"]   = df["price"].ewm(span=10).mean()
```
`rolling` has a fixed window, `expanding` grows from the start, `ewm` weights recent values more.

---

### Q56. Compute month-over-month growth per product.
**Answer:**
```python
m = df.groupby(["product", "month"], as_index=False)["rev"].sum().sort_values(["product", "month"])
m["mom_pct"] = m.groupby("product")["rev"].pct_change() * 100
```
Missing months break `pct_change`; reindex to a complete month range first.

---

### Q57. `cumsum`, `cummax`, `cumcount`: example uses?
**Answer:** Running totals (`cumsum`), high-water marks (`cummax`), and `cumcount()` gives the 0-based sequence number within each group (e.g., the nth order of a customer: `df.groupby("cust").cumcount() + 1`).

---

### Q58. Top-N per group, with ties handled.
**Answer:**
```python
df["rk"] = df.groupby("dept")["salary"].rank(method="dense", ascending=False)
top3 = df[df.rk <= 3]
# no ties wanted:
df.sort_values("salary", ascending=False).groupby("dept").head(3)
```

---

### Q59. Merge when key names differ, and avoiding column clashes.
**Answer:**
```python
orders.merge(cust, left_on="cust_id", right_on="id", how="left", suffixes=("_ord", "_cust"))
```
Drop the redundant key after merging.

---

### Q60. What is `merge_asof` and when is it used?
**Answer:** A nearest-key join on sorted keys, usually timestamps: for each trade attach the most recent quote, or attach the latest exchange rate valid at transaction time.
```python
pd.merge_asof(trades.sort_values("ts"), rates.sort_values("ts"),
              on="ts", by="currency", direction="backward", tolerance=pd.Timedelta("1D"))
```

---

### Q61. Merge on index and `join`?
**Answer:** `a.join(b, how="left")` joins on the index by default; `a.merge(b, left_index=True, right_index=True)` is equivalent. Handy after `set_index` or groupby results.

---

### Q62. How do you protect a merge against bad data?
**Answer:**
```python
m = a.merge(b, on="id", how="left", validate="m:1", indicator=True)
assert len(m) == len(a), "row count changed after join"
match_rate = (m["_merge"] == "both").mean()
```
`validate` raises on duplicate keys; `indicator` shows the match status. Fail or alert if the match rate drops below an agreed threshold.

---

### Q63. `concat` parameters?
**Answer:** `axis=0` stacks rows, `axis=1` places frames side by side, `ignore_index=True` rebuilds the index, `keys=` labels each source, `join="inner"` keeps only common columns. Don't `concat` inside a loop on every iteration; collect frames in a list and concat once (quadratic cost otherwise).

---

### Q64. `combine_first` and `update`?
**Answer:** `a.combine_first(b)` fills nulls in `a` with values from `b` (aligned on index/columns), useful for coalescing multiple sources. `a.update(b)` overwrites `a` in place with non-null values from `b`.

---

### Q65. Anti-join: customers who never ordered.
**Answer:**
```python
m = cust.merge(orders[["cust_id"]].drop_duplicates(), left_on="id", right_on="cust_id",
               how="left", indicator=True)
never = m[m["_merge"] == "left_only"]
# or: cust[~cust.id.isin(orders.cust_id)]
```

---

### Q66. Cross join (Cartesian product)?
**Answer:** `a.merge(b, how="cross")`. Useful for generating a full grid (all stores x all dates) to fill missing combinations, but row counts multiply, so watch memory.

---

### Q67. A join suddenly produced 10x more rows. Diagnose it.
**Answer:** Many-to-many keys. Check `b.duplicated("key").sum()`, dedupe or aggregate the right table to the grain of the join key, use `validate="m:1"`, and assert row counts before and after.

---

### Q68. `resample` vs `groupby` for time series?
**Answer:** `resample` works on a DatetimeIndex and handles frequencies directly, including empty periods: `df.set_index("ts")["amt"].resample("D").sum()`. Use `pd.Grouper(key="ts", freq="D")` inside `groupby` when you need groups plus time buckets.

---

### Q69. Generate date ranges and business days.
**Answer:**
```python
pd.date_range("2026-01-01", "2026-12-31", freq="D")
pd.bdate_range("2026-01-01", periods=20)           # weekdays only
pd.date_range("2026-01-01", periods=12, freq="MS") # month starts
```

---

### Q70. Scenario: daily sales have missing dates for some stores. Fill the gaps with zero.
**Answer:**
```python
idx = pd.MultiIndex.from_product([stores, pd.date_range(start, end)], names=["store", "dt"])
full = (df.set_index(["store", "dt"]).reindex(idx, fill_value=0).reset_index())
```
This makes rolling and lag calculations correct, since missing days would otherwise shrink windows.

---

### Q71. Timestamp vs Period?
**Answer:** `Timestamp` is a point in time; `Period` represents a span (a month, a quarter). `to_period("M")` is useful for monthly grouping and reporting keys without day or time parts.

---

### Q72. How do you create lag features?
**Answer:**
```python
df = df.sort_values(["sku", "dt"])
for k in (1, 7, 28):
    df[f"qty_lag{k}"] = df.groupby("sku")["qty"].shift(k)
```
Always shift within a group, and sort first; otherwise values leak across groups.

---

### Q73. Rolling window based on time, not row count?
**Answer:** `df.set_index("ts").groupby("cust")["amt"].rolling("7D").sum()` computes trailing 7-day totals even when rows are irregularly spaced.

---

### Q74. How do you explode list columns and normalize nested data?
**Answer:** `df.explode("tags")` makes one row per list element; combine with `json_normalize` for nested dicts. Check row counts afterward, because empty lists become a single NaN row.

---

### Q75. After `groupby().agg()` with multiple functions you get MultiIndex columns. Flatten them.
**Answer:**
```python
g = df.groupby("region").agg({"amt": ["sum", "mean"], "qty": "max"})
g.columns = ["_".join(c) for c in g.columns]
g = g.reset_index()
```
Named aggregation avoids this problem.

---

### Q76. How do you replace a slow `apply(axis=1)`?
**Answer:** Convert the logic into column-wise operations: `np.where`, `np.select`, `str` methods, `dt` methods, `map` with a dict, or `merge` with a lookup table. If the logic is truly row-wise and numeric, use NumPy arrays directly or `numba`.

---

### Q77. List the main ways to reduce pandas memory.
**Answer:**
- Read only needed columns (`usecols`).
- Downcast: `pd.to_numeric(s, downcast="integer"|"float")`.
- `category` for repetitive strings, `string[pyarrow]` for text.
- Delete intermediate frames (`del df; gc.collect()`).
- Process in chunks or move to Parquet and DuckDB/Polars.

Typical savings are 50–90%.

---

### Q78. Why can `category` speed up groupby and merge?
**Answer:** Integer codes are compared and hashed instead of Python strings, and storage is smaller. Caveat: `groupby` on categoricals returns all categories by default; use `observed=True` to avoid empty groups.

---

### Q79. What are sparse structures and when do you use them?
**Answer:** `pd.arrays.SparseArray` stores only non-default values, helping with mostly-zero data such as one-hot encoded matrices. For ML, scikit-learn works with `scipy.sparse` matrices (e.g., output from OneHotEncoder and TfidfVectorizer).

---

### Q80. Performance tips for pandas in production jobs?
**Answer:** Vectorize, avoid row loops, use proper dtypes, filter early, select columns early, use `merge` rather than repeated `loc` lookups, avoid repeated `concat`, read Parquet instead of CSV, and profile (`%timeit`, `cProfile`) before optimizing.

---

### Q81. What is Copy-on-Write in pandas?
**Answer:** A mode (opt-in in pandas 2.x with `pd.options.mode.copy_on_write = True`, default in 3.0) where any derived object behaves like an independent copy, with the actual copy deferred until modification. It removes `SettingWithCopyWarning` ambiguity and reduces unnecessary defensive `.copy()` calls.

---

### Q82. What are PyArrow-backed dtypes?
**Answer:** `dtype_backend="pyarrow"` (or `string[pyarrow]`, `int64[pyarrow]`) stores data in Arrow memory: lower memory for strings, proper null handling, and zero-copy exchange with Parquet/Arrow/Polars. Often noticeably faster for string-heavy data.

---

### Q83. Nullable dtypes: `Int64`, `string`, `boolean`, and `pd.NA`?
**Answer:** Classic `int64` can't hold NaN, so a column with a null becomes `float64` and silently changes IDs (`123` becomes `123.0`). Nullable `Int64` keeps integers with `pd.NA`. Use them for IDs and flags.
```python
df["id"] = df["id"].astype("Int64")
```

---

### Q84. How do you parallelize pandas work on one machine?
**Answer:** Split by key or file and use `concurrent.futures.ProcessPoolExecutor` or `multiprocessing`, then concat the results; or switch to Dask/Polars/Modin. Pass file paths to workers rather than large DataFrames to avoid pickling overhead.

---

### Q85. Pandas vs Polars vs Spark vs DuckDB?
**Answer:** Pandas: eager, single-threaded, huge ecosystem, best for small-to-medium data. Polars: multi-threaded, lazy, faster and lower memory. DuckDB: SQL on files, out-of-core. Spark: distributed, for data beyond one machine. Choose by data size, team skills, and platform.

---

### Q86. How do you join two huge DataFrames that don't fit in memory?
**Answer:** Join in partitions (hash both tables on the key into N buckets on disk, join bucket by bucket), use DuckDB/Polars lazy mode for out-of-core joins, or use Spark. Pre-filter and select only the needed columns and dtypes first.

---

### Q87. Scenario: dedupe customers with slightly different names and addresses.
**Answer:** Normalize (lowercase, strip punctuation, standardize abbreviations), build blocking keys (e.g., first 3 letters of surname + postcode) to limit comparisons, score candidate pairs with string similarity (`rapidfuzz`, Jaro-Winkler), set thresholds, send borderline pairs to manual review, and keep a survivorship rule for the golden record. Libraries: `recordlinkage`, `dedupe`.

---

### Q88. Validate data against a schema in production code.
**Answer:**
```python
import pandera as pa
schema = pa.DataFrameSchema({
    "id": pa.Column("Int64", nullable=False, unique=True),
    "amount": pa.Column(float, pa.Check.ge(0)),
    "status": pa.Column(str, pa.Check.isin(["NEW", "PAID", "REFUNDED"])),
}, strict=True)
schema.validate(df, lazy=True)     # lazy collects all errors
```

---

### Q89. Reconcile source and target counts and sums.
**Answer:**
```python
s = src.groupby("dt").agg(rows=("id", "count"), amt=("amt", "sum"))
t = tgt.groupby("dt").agg(rows=("id", "count"), amt=("amt", "sum"))
diff = s.join(t, lsuffix="_src", rsuffix="_tgt", how="outer").fillna(0)
diff["row_diff"] = diff.rows_src - diff.rows_tgt
bad = diff[(diff.row_diff != 0) | ((diff.amt_src - diff.amt_tgt).abs() > 0.01)]
```

---

### Q90. Implement SCD Type 2 in pandas (logic overview).
**Answer:** Join incoming rows to the current dimension on the business key. Compare tracked attributes (use a hash column). For changed keys, close the old row (`end_date = run_date`, `is_current = False`) and add a new row (`start_date = run_date`, `end_date = 9999-12-31`, `is_current = True`). New keys are inserted; unchanged keys are untouched. For large data, do this with SQL `MERGE`.

---

### Q91. Scenario: apply a change feed (insert/update/delete) to a snapshot.
**Answer:**
```python
feed = feed.sort_values("ts").drop_duplicates("id", keep="last")     # last event per key
deleted = feed[feed.op == "D"].id
upserts = feed[feed.op != "D"].drop(columns="op")
snap = snap[~snap.id.isin(feed.id)]                                  # drop old versions
snap = pd.concat([snap, upserts]).reset_index(drop=True)
```
Out-of-order events are why you sort by an event timestamp or sequence number, not arrival order.

---

### Q92. Incremental append without creating duplicates?
**Answer:** Keep a unique business key, load the new batch, anti-join against existing keys (or `drop_duplicates` after concat with `keep="last"`), and prefer an upsert in the database. Make the job rerunnable for the same date range.

---

### Q93. Scenario: build user sessions from clickstream (30-minute gap).
**Answer:**
```python
df = df.sort_values(["user", "ts"])
new = df.groupby("user")["ts"].diff().gt(pd.Timedelta("30min")) | df["user"].ne(df["user"].shift())
df["session_id"] = new.cumsum()
```

---

### Q94. Scenario: cohort retention analysis.
**Answer:**
```python
first = df.groupby("user")["dt"].min().rename("cohort")
df = df.join(first, on="user")
df["cohort_m"] = df.cohort.dt.to_period("M")
df["age_m"] = (df.dt.dt.to_period("M") - df.cohort_m).apply(lambda x: x.n)
ret = df.groupby(["cohort_m", "age_m"])["user"].nunique().unstack(fill_value=0)
ret = ret.div(ret[0], axis=0)          # share of cohort still active
```

---

### Q95. Scenario: funnel conversion (view → cart → purchase).
**Answer:**
```python
steps = ["view", "cart", "purchase"]
users = {s: set(df.loc[df.event == s, "user"]) for s in steps}
funnel = pd.Series({s: len(users[s]) for s in steps})
funnel_pct = funnel / funnel.iloc[0]
```
For strict ordering within a session, compare the first timestamp per step per user.

---

### Q96. Scenario: RFM segmentation.
**Answer:**
```python
snap = df.dt.max() + pd.Timedelta(days=1)
rfm = df.groupby("cust").agg(recency=("dt", lambda s: (snap - s.max()).days),
                             frequency=("order_id", "nunique"),
                             monetary=("amt", "sum"))
rfm["R"] = pd.qcut(rfm.recency, 5, labels=[5, 4, 3, 2, 1])
rfm["F"] = pd.qcut(rfm.frequency.rank(method="first"), 5, labels=[1, 2, 3, 4, 5])
rfm["M"] = pd.qcut(rfm.monetary, 5, labels=[1, 2, 3, 4, 5])
```

---

### Q97. Quantiles, percentiles, and IQR outlier flags?
**Answer:**
```python
df.amt.quantile([0.01, 0.5, 0.99])
q1, q3 = df.amt.quantile([.25, .75]); iqr = q3 - q1
df["is_outlier"] = ~df.amt.between(q1 - 1.5 * iqr, q3 + 1.5 * iqr)
```
Flag first and review with the business; heavy-tailed data like transaction amounts have legitimate large values.

---

### Q98. Compare two DataFrames.
**Answer:** `equals` for exact equality; `compare` for cell-level diffs on identically labeled frames; `merge(indicator=True)` for row-level diffs; and `pd.testing.assert_frame_equal(a, b, check_dtype=False, atol=1e-6)` in tests.

---

### Q99. How do you unit test a pandas transformation?
**Answer:**
```python
def test_add_total():
    inp = pd.DataFrame({"qty": [2, 0], "price": [5.0, 9.0]})
    exp = inp.assign(total=[10.0, 0.0])
    pd.testing.assert_frame_equal(add_total(inp), exp)
```
Test nulls, duplicates, empty frames, and boundary values; keep transformations free of I/O.

---

### Q100. Scenario: your pandas ETL runs fine for a month and then fails with `MemoryError`. What do you do?
**Answer:** Data volume grew. Short term: read fewer columns, set dtypes, process in chunks or by partition. Medium term: switch the heavy steps to DuckDB/Polars or Spark, store intermediate data as Parquet, and add monitoring for input size and runtime. Also check for accidental copies (`concat` in loops, many large intermediates).

---

# SECTION 3: NUMPY

### Q101. Why is a NumPy array faster and smaller than a Python list?
**Answer:** A `ndarray` stores elements of one dtype in a contiguous memory block and operations run in compiled C loops. A list stores pointers to separate Python objects, with type checks on every operation. For numeric work, NumPy is typically 10–100x faster and uses far less memory.

---

### Q102. How do dtypes affect memory and results?
**Answer:** `int8` uses 1 byte per value, `int64` uses 8, and `float32` is half of `float64`.
```python
a = np.array([1, 2, 3], dtype=np.int32)
a.nbytes, a.dtype
a.astype(np.float32)
```
Beware of overflow (`np.int8` wraps around at 127) and precision loss when downcasting.

---

### Q103. `reshape`, `ravel`, and `flatten`: which return views and which copies?
**Answer:** `reshape` and `ravel` return views when possible (contiguous memory); `flatten` always returns a copy. Check with `np.shares_memory(a, b)` or `b.base is a`.

---

### Q104. Explain indexing: slicing, boolean masks, and fancy indexing.
**Answer:**
```python
a[1:4]                     # slice -> view
a[a > 10]                  # boolean mask -> copy
a[[0, 2, 5]]               # fancy indexing -> copy
m[:, 1]                    # column 1 of a 2-D array
m[np.ix_([0, 2], [1, 3])]  # sub-matrix by rows and columns
```
Slices share memory with the original, while mask and fancy indexing create copies.

---

### Q105. Explain broadcasting rules.
**Answer:** Shapes are compared from the trailing dimension; two dimensions are compatible if they're equal or one of them is 1 (the missing dimensions are treated as 1).
```python
X = np.random.rand(1000, 3)
X_centered = X - X.mean(axis=0)     # (1000,3) - (3,)  -> works
```
Broadcasting avoids explicit loops and copies, but can silently produce wrong shapes like `(n,1)` + `(n,)` giving `(n,n)`.

---

### Q106. Vectorization vs loops: demonstrate.
**Answer:**
```python
# loop
out = [x * 1.18 if x > 1000 else x for x in prices]
# vectorized
prices = np.asarray(prices)
out = np.where(prices > 1000, prices * 1.18, prices)
```
Vectorized code is shorter and far faster for large arrays.

---

### Q107. What does `axis` mean in aggregations?
**Answer:** The axis along which the operation collapses. For a 2-D array, `axis=0` aggregates down rows (one result per column) and `axis=1` aggregates across columns (one result per row). `keepdims=True` retains the dimension for later broadcasting.

---

### Q108. How do you handle NaN in aggregations?
**Answer:** `np.sum`/`np.mean` return NaN if any value is NaN. Use `np.nansum`, `np.nanmean`, `np.nanstd`, `np.nanpercentile`, or mask with `a[~np.isnan(a)]`.

---

### Q109. `np.where` vs `np.select`?
**Answer:** `np.where(cond, x, y)` handles one condition; `np.select(conds, choices, default)` handles several conditions in priority order (first match wins), like a SQL `CASE WHEN`.

---

### Q110. How do you get unique values and their counts?
**Answer:**
```python
vals, counts = np.unique(a, return_counts=True)
vals, idx, inv = np.unique(a, return_index=True, return_inverse=True)
```
`np.unique` sorts the input (O(n log n)); for categorical counts on huge data, pandas `value_counts` may be easier.

---

### Q111. `sort`, `argsort`, `argpartition`?
**Answer:** `np.sort` returns sorted values; `np.argsort` returns the indices that would sort them; `np.argpartition(a, k)` finds the k smallest/largest in O(n) without a full sort. Use `argpartition` for top-K selection on big arrays.
```python
top5_idx = np.argpartition(scores, -5)[-5:]
```

---

### Q112. `concatenate`, `stack`, `vstack`, `hstack`?
**Answer:** `concatenate` joins along an existing axis; `stack` creates a new axis; `vstack`/`hstack` are convenience wrappers for axis 0 and 1. Shapes must match on the non-joined axes.

---

### Q113. `dot`, `matmul`, `@`, `einsum`, and element-wise `*`?
**Answer:** `*` is element-wise. `a @ b` / `np.matmul` is matrix multiplication (with broadcasting for stacks of matrices). `np.dot` behaves similarly for 1-D/2-D. `np.einsum("ij,jk->ik", a, b)` expresses arbitrary tensor contractions.

---

### Q114. How do you generate reproducible random numbers?
**Answer:**
```python
rng = np.random.default_rng(42)
rng.normal(0, 1, size=1000)
rng.choice(a, size=100, replace=False)
rng.integers(0, 10, size=5)
```
The `Generator` API is preferred over legacy `np.random.seed`. Pass a seed or the `rng` object around for reproducibility.

---

### Q115. `arange` vs `linspace`?
**Answer:** `arange(start, stop, step)` uses a step size (floating-point steps can give an unexpected element count); `linspace(start, stop, num)` gives exactly `num` evenly spaced points including the endpoint by default.

---

### Q116. How do you detect and handle NaN and infinity?
**Answer:** `np.isnan`, `np.isinf`, `np.isfinite`, and `np.nan_to_num(a, nan=0.0, posinf=..., neginf=...)`. Division by zero yields `inf` (or NaN for 0/0) with a warning; guard with `np.divide(a, b, out=np.zeros_like(a), where=b != 0)`.

---

### Q117. Why shouldn't you compare floats with `==`?
**Answer:** Floating-point representation errors (`0.1 + 0.2 != 0.3`). Use `np.isclose(a, b, rtol=1e-9, atol=1e-12)` or `np.allclose` for arrays.

---

### Q118. Percentiles and quantiles in NumPy?
**Answer:** `np.percentile(a, [25, 50, 75])` and `np.quantile(a, [0.25, 0.5, 0.75])`; interpolation can be controlled with `method=`. Use `np.nanpercentile` when NaNs are present.

---

### Q119. `cumsum`, `cumprod`, `diff`?
**Answer:** `np.cumsum` for running totals, `np.diff(a)` for differences between consecutive elements (period-over-period change), and `np.cumprod(1 + returns) - 1` for compounded growth.

---

### Q120. `histogram` and `digitize`?
**Answer:** `np.histogram(a, bins=10)` gives counts and bin edges; `np.digitize(a, bins)` assigns each value its bin index, which is useful for custom bucketing such as age bands or price tiers.

---

### Q121. What is `np.searchsorted` useful for?
**Answer:** Binary search on a sorted array in O(log n) per query: mapping values to intervals, finding insertion points, or doing fast as-of lookups (e.g., find the applicable tax slab or price tier for each transaction).

---

### Q122. `np.clip`?
**Answer:** `np.clip(a, lo, hi)` limits values to a range, a quick way to cap outliers or enforce valid ranges (e.g., probabilities in [0, 1]).

---

### Q123. Implement standardization and min-max scaling in NumPy.
**Answer:**
```python
z  = (X - X.mean(axis=0)) / X.std(axis=0)
mm = (X - X.min(axis=0)) / (X.max(axis=0) - X.min(axis=0))
```
Guard against zero std/range (constant columns). In ML, fit scaling parameters on training data only.

---

### Q124. Detect outliers with a z-score in NumPy.
**Answer:**
```python
z = (a - a.mean()) / a.std()
outliers = a[np.abs(z) > 3]
```
Mean and std are themselves sensitive to outliers; for skewed data prefer the median/MAD or IQR rule.

---

### Q125. Correlation with NumPy?
**Answer:** `np.corrcoef(x, y)` returns the correlation matrix (Pearson); `np.corrcoef(X, rowvar=False)` treats columns as variables. Correlation doesn't imply causation, and it only captures linear relationships.

---

### Q126. Compute a moving average efficiently.
**Answer:**
```python
def moving_avg(a, w):
    c = np.cumsum(np.insert(a, 0, 0.0))
    return (c[w:] - c[:-w]) / w
# or: np.convolve(a, np.ones(w)/w, mode="valid")
```
`cumsum` is O(n), independent of window size.

---

### Q127. What is `np.memmap` and when would you use it?
**Answer:** Memory-maps a binary file as an array so only the parts you touch are loaded: useful for arrays larger than RAM (large sensor logs, embedding matrices).
```python
m = np.memmap("data.bin", dtype="float32", mode="r", shape=(10_000_000, 16))
```

---

### Q128. Saving and loading arrays?
**Answer:** `np.save`/`np.load` for single arrays (`.npy`), `np.savez_compressed` for several arrays, `np.loadtxt`/`np.genfromtxt` for text. Use Parquet/Arrow for tabular data shared across systems; `.npy` is Python-specific, and `allow_pickle=True` is a security risk with untrusted files.

---

### Q129. What are structured arrays?
**Answer:** Arrays with named fields of different dtypes (like C structs): `np.dtype([("id","i8"),("price","f8")])`. They're compact and fast for fixed schemas, but pandas is usually friendlier for heterogeneous tabular data.

---

### Q130. `tile` vs `repeat`?
**Answer:** `np.repeat([1,2], 3)` gives `[1,1,1,2,2,2]` (each element repeated); `np.tile([1,2], 3)` gives `[1,2,1,2,1,2]` (the whole array repeated). Often used to build grids and test data.

---

### Q131. What does `meshgrid` do?
**Answer:** Builds coordinate matrices from coordinate vectors, e.g., to evaluate a function on a 2-D grid or to generate all combinations of two parameters.

---

### Q132. Linear algebra essentials in `numpy.linalg`?
**Answer:** `inv`, `solve` (prefer `solve(A, b)` over `inv(A) @ b`: faster and more stable), `det`, `eig`, `svd`, `norm`, `lstsq`. SVD underlies PCA and many recommender approaches.

---

### Q133. How do you know whether two arrays share memory?
**Answer:** `np.shares_memory(a, b)` or `b.base is a`. This matters because modifying a view changes the original, which can be a hidden bug; use `.copy()` when you need independence.

---

### Q134. Memory layout: C-order vs Fortran-order, and why does it matter?
**Answer:** C order is row-major (rows contiguous), Fortran order is column-major. Operations along the contiguous axis are faster due to CPU cache effects. `np.ascontiguousarray(a)` fixes layout before heavy numerical work or when passing to C libraries.

---

### Q135. Scenario: compute cosine similarity between a query vector and 1 million item vectors.
**Answer:**
```python
def cosine_sim(q, M):
    qn = q / np.linalg.norm(q)
    Mn = M / np.linalg.norm(M, axis=1, keepdims=True)
    return Mn @ qn

scores = cosine_sim(q, M)
top = np.argpartition(scores, -10)[-10:]
top = top[np.argsort(-scores[top])]
```
Pre-normalize `M` once and store as `float32` to cut memory and time; for large scale use an ANN index (FAISS).

---

# SECTION 4: SCIKIT-LEARN

### Q136. Explain scikit-learn's estimator API.
**Answer:** Estimators implement `fit(X, y)`; transformers add `transform(X)` (and `fit_transform`); predictors add `predict(X)` (plus `predict_proba`/`score`). Hyperparameters are set in the constructor, learned attributes end with `_` (e.g., `coef_`), and `get_params`/`set_params` enable tuning and cloning.

---

### Q137. How do you split data correctly?
**Answer:**
```python
from sklearn.model_selection import train_test_split
X_tr, X_te, y_tr, y_te = train_test_split(X, y, test_size=0.2, stratify=y, random_state=42)
```
Use `stratify` for classification so class ratios are preserved; for time series split by time, not randomly.

---

### Q138. What is data leakage? Give real examples.
**Answer:** Information from outside the training set (or the future) influences the model, giving unrealistically good validation scores. Examples:
- Fitting a scaler or imputer on the full dataset before splitting.
- Using a feature that is only known after the outcome (e.g., "refund_issued" when predicting churn).
- Random split of time-ordered data, so the future trains the past.
- Same customer in both train and test (use `GroupKFold`).

Prevent with Pipelines, time-aware splits, and feature availability reviews.

---

### Q139. Why use `Pipeline` and `ColumnTransformer`?
**Answer:** They bundle preprocessing and model into one object so the same steps run during training, cross-validation, and prediction, which prevents leakage and train/serve skew.
```python
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.impute import SimpleImputer
from sklearn.ensemble import RandomForestClassifier

num = Pipeline([("imp", SimpleImputer(strategy="median")), ("sc", StandardScaler())])
cat = Pipeline([("imp", SimpleImputer(strategy="most_frequent")),
                ("oh", OneHotEncoder(handle_unknown="ignore"))])
pre = ColumnTransformer([("num", num, num_cols), ("cat", cat, cat_cols)])
model = Pipeline([("pre", pre), ("clf", RandomForestClassifier(n_estimators=300, random_state=42))])
model.fit(X_tr, y_tr)
```

---

### Q140. Imputation options in scikit-learn?
**Answer:** `SimpleImputer` (mean, median, most_frequent, constant), `KNNImputer` (uses nearest neighbors), `IterativeImputer` (models each feature from others, experimental). Add `add_indicator=True` so the model knows which values were missing. Many tree-based models (e.g., `HistGradientBoosting`) handle NaN natively.

---

### Q141. `StandardScaler` vs `MinMaxScaler` vs `RobustScaler`?
**Answer:** Standard: zero mean/unit variance (linear models, SVM, kNN, PCA). MinMax: scales to [0, 1] (neural nets, bounded inputs), sensitive to outliers. Robust: uses median and IQR, good with outliers. Tree-based models don't need scaling.

---

### Q142. Categorical encoding options?
**Answer:** `OneHotEncoder` for nominal low-cardinality features; `OrdinalEncoder` for ordered categories (specify `categories`); target/mean encoding (`TargetEncoder`, needs cross-fitting to avoid leakage) for high cardinality; hashing for very high cardinality. Always set `handle_unknown` so unseen categories in production don't crash scoring.

---

### Q143. How do you handle a categorical column with 50,000 unique values (e.g., ZIP code, product ID)?
**Answer:** Group rare categories into "other" (`min_frequency` in `OneHotEncoder`), use target encoding with cross-validation, hashing trick, or embeddings; consider deriving meaningful features (region from ZIP) instead of the raw ID.

---

### Q144. Feature selection approaches?
**Answer:** Filter: `VarianceThreshold`, `SelectKBest` (chi2, f_classif, mutual_info). Wrapper: `RFE`. Embedded: L1 regularization (Lasso), tree importances. Do selection inside the cross-validation Pipeline to avoid leakage.

---

### Q145. What is PCA and when do you use it?
**Answer:** Principal Component Analysis projects data onto orthogonal directions of maximum variance. Use for dimensionality reduction, noise reduction, and visualization. Scale features first; choose components by cumulative explained variance (`pca.explained_variance_ratio_.cumsum()`). Components are less interpretable than original features.

---

### Q146. Cross-validation strategies: when to use which?
**Answer:**
- `KFold`: general regression.
- `StratifiedKFold`: classification, especially imbalanced.
- `GroupKFold`: grouped data (multiple rows per customer/device) so a group never spans train and validation.
- `TimeSeriesSplit`: time-ordered data; training always precedes validation.
```python
from sklearn.model_selection import cross_val_score, StratifiedKFold
cross_val_score(model, X, y, cv=StratifiedKFold(5, shuffle=True, random_state=42), scoring="roc_auc")
```

---

### Q147. `GridSearchCV` vs `RandomizedSearchCV`?
**Answer:** Grid tries every combination (expensive, fine for small spaces). Randomized samples a fixed number of combinations from distributions and usually finds good settings far faster. For larger spaces use Bayesian tools (Optuna) or `HalvingRandomSearchCV`. Always tune on a pipeline and keep a final untouched test set.

---

### Q148. Explain bias-variance tradeoff and overfitting.
**Answer:** High bias (underfitting): model too simple, poor on both train and validation. High variance (overfitting): excellent on train, poor on validation. Fix overfitting with more data, regularization, simpler models, early stopping, and feature reduction; fix underfitting with richer features or models. Use learning curves to diagnose.

---

### Q149. L1 vs L2 regularization?
**Answer:** L2 (Ridge) shrinks coefficients smoothly and handles correlated features; L1 (Lasso) can drive coefficients to exactly zero (feature selection); ElasticNet mixes both. In sklearn `LogisticRegression`, `C` is the inverse of regularization strength (smaller `C` means stronger regularization).

---

### Q150. Linear regression assumptions and how to check them?
**Answer:** Linearity, independent errors, constant variance (homoscedasticity), roughly normal residuals, and little multicollinearity. Check residual plots, VIF, and Durbin-Watson for time-ordered data. Violations call for transformations (log of skewed targets), interactions, or different models.

---

### Q151. When do you use logistic regression, and how do you interpret it?
**Answer:** A strong, interpretable baseline for binary classification. It outputs probabilities; coefficients represent change in log-odds (`np.exp(coef_)` gives odds ratios). Needs scaled features for good optimization/regularization, and works well with sparse high-dimensional data like TF-IDF.

---

### Q152. How do decision trees work and what are their weaknesses?
**Answer:** They split data recursively on features to reduce impurity (Gini/entropy for classification, MSE for regression). Pros: no scaling, handles nonlinearity and mixed types, interpretable. Cons: overfit easily and are unstable. Control with `max_depth`, `min_samples_leaf`, and pruning (`ccp_alpha`).

---

### Q153. Random Forest vs Gradient Boosting?
**Answer:** Random Forest: many deep trees trained independently on bootstrap samples and averaged (bagging); reduces variance, robust, easy to tune. Gradient boosting: trees built sequentially, each correcting previous errors; usually higher accuracy but more sensitive to tuning and overfitting. In sklearn use `HistGradientBoostingClassifier` (fast, native NaN support); XGBoost/LightGBM/CatBoost are common at scale.

---

### Q154. Scenario: only 1% of transactions are fraud. How do you handle the imbalance?
**Answer:**
- Use metrics beyond accuracy (PR-AUC, recall at a fixed precision).
- Stratified splitting and CV.
- `class_weight="balanced"` in the model.
- Resampling (SMOTE or undersampling) only on training folds, via `imblearn.pipeline.Pipeline`.
- Tune the decision threshold based on business costs.
- Consider anomaly detection as a complement.

A model predicting "no fraud" every time gets 99% accuracy and is useless.

---

### Q155. Explain precision, recall, F1, ROC-AUC, and PR-AUC. When use each?
**Answer:** Precision = TP/(TP+FP) ("of flagged, how many were right"); recall = TP/(TP+FN) ("of all positives, how many we caught"); F1 is their harmonic mean. ROC-AUC measures ranking quality across thresholds but can look optimistic with heavy imbalance; PR-AUC is more informative for rare positives. Choose by cost: fraud/cancer screening favors recall, spam blocking favors precision.

---

### Q156. How do you choose the classification threshold?
**Answer:** Default 0.5 is rarely optimal. Use `precision_recall_curve` or `roc_curve` on validation data and pick the threshold that meets a business constraint (e.g., precision >= 90%) or minimizes expected cost, then fix it in the deployment config. `predict_proba` plus a custom threshold, rather than `predict`.

---

### Q157. Regression metrics: MAE, RMSE, R², MAPE?
**Answer:** MAE: average absolute error, robust and easy to explain. RMSE: penalizes large errors more. R²: share of variance explained (can be misleading, and negative on bad models). MAPE: percentage error, breaks near zero actuals. Pick the metric that matches business loss (for demand forecasting, asymmetric costs may need custom losses).

---

### Q158. Feature importance: impurity-based vs permutation?
**Answer:** Impurity-based importance (`feature_importances_`) is fast but biased toward high-cardinality/continuous features and computed on training data. Permutation importance (`sklearn.inspection.permutation_importance`) measures the drop in validation score when a feature is shuffled; it is more reliable but affected by correlated features. SHAP gives per-prediction explanations.

---

### Q159. K-Means: how do you choose k and what are its limitations?
**Answer:** Use the elbow method on inertia, silhouette score, or business needs. Limitations: assumes spherical clusters of similar size, sensitive to scale (standardize first) and initialization (`n_init`, `k-means++`), and to outliers. Use `MiniBatchKMeans` for large data.

---

### Q160. Alternatives to K-Means?
**Answer:** DBSCAN (density-based, finds arbitrary shapes and noise, no k required, sensitive to `eps`), hierarchical/agglomerative clustering (dendrograms, small data), Gaussian Mixture Models (soft clustering). Pick according to data shape and noise.

---

### Q161. How do you detect anomalies with scikit-learn?
**Answer:**
```python
from sklearn.ensemble import IsolationForest
iso = IsolationForest(contamination=0.01, random_state=42).fit(X)
scores = iso.decision_function(X)       # lower = more anomalous
flags = iso.predict(X) == -1
```
Others: `LocalOutlierFactor`, `OneClassSVM`. Without labels, validate flagged cases with domain experts.

---

### Q162. How do you save and load models? What are the risks?
**Answer:**
```python
import joblib
joblib.dump(model, "model_v3.joblib")
model = joblib.load("model_v3.joblib")
```
Save the entire Pipeline, record library versions (pickle compatibility breaks across versions), version artifacts with metadata (training data version, metrics), and never load pickles from untrusted sources. For portability consider ONNX.

---

### Q163. Write a custom transformer.
**Answer:**
```python
from sklearn.base import BaseEstimator, TransformerMixin

class LogAmount(BaseEstimator, TransformerMixin):
    def __init__(self, col="amount"): self.col = col
    def fit(self, X, y=None): return self
    def transform(self, X):
        X = X.copy()
        X[self.col] = np.log1p(X[self.col].clip(lower=0))
        return X
```
Keep learned state in attributes ending with `_` created in `fit`.

---

### Q164. `FunctionTransformer`?
**Answer:** Wraps a stateless function (like `np.log1p`) as a transformer so it can sit inside a Pipeline without writing a class.

---

### Q165. Text features: `CountVectorizer`, `TfidfVectorizer`, `HashingVectorizer`?
**Answer:** Count: raw term counts. TF-IDF: down-weights terms common across documents. Hashing: fixed-size, stateless and memory-bounded (no vocabulary), good for streaming or huge vocabularies, but not invertible. Output is a sparse matrix, which works well with `LogisticRegression` and `SGDClassifier`.

---

### Q166. How do you train on data larger than memory?
**Answer:** Use estimators with `partial_fit` (`SGDClassifier`, `SGDRegressor`, `MiniBatchKMeans`, `MultinomialNB`) fed from chunks, `HashingVectorizer` for text, subsample for prototyping, or move to distributed libraries (Spark MLlib, Dask-ML, XGBoost distributed).

---

### Q167. What is probability calibration?
**Answer:** Predicted probabilities should match observed frequencies (of cases with 0.8, about 80% are positive). Random forests and boosted models are often miscalibrated. Use `CalibratedClassifierCV` (`sigmoid` or `isotonic`) and check with `calibration_curve`; important when probabilities feed pricing or risk decisions.

---

### Q168. What is train/serve skew and how do you prevent it?
**Answer:** The features computed in production differ from those in training (different code paths, units, null handling, time windows). Prevent by putting all preprocessing in one Pipeline/feature library used by both paths, using a feature store, adding schema and range validation at scoring time, and monitoring feature distributions.

---

### Q169. How do you validate a forecasting or time-dependent model?
**Answer:** Use `TimeSeriesSplit` or a rolling-origin backtest, never random shuffling. Build lag/rolling features using only past data, compare against a naive baseline (last value, seasonal naive), and evaluate with metrics that match the business (MAE, WAPE).

---

### Q170. How do you detect data drift?
**Answer:** Compare production feature distributions to training: PSI (Population Stability Index), KS test for numeric features (`scipy.stats.ks_2samp`), chi-square for categoricals, plus monitoring prediction distribution and, when labels arrive, model performance. Alert on thresholds (PSI > 0.2 is a common warning), then investigate upstream changes before retraining.

---

### Q171. How do you operationalize batch scoring in a data pipeline?
**Answer:** Load the versioned Pipeline artifact, read the day's feature table (Parquet/warehouse), validate the schema, call `predict_proba` in chunks, write scores with `model_version`, `scored_at`, and `run_id`, and make the write idempotent (overwrite the partition). Monitor volume, null rates, and score distribution.

---

### Q172. Why does k-NN need scaling and what are its production drawbacks?
**Answer:** It relies on distances, so large-range features dominate unless scaled. Drawbacks: prediction is slow on large datasets (stores all training data), suffers in high dimensions, and uses lots of memory. Approximate nearest neighbor indexes help.

---

### Q173. When would you use SVM?
**Answer:** Medium-sized datasets with clear margins and high dimensions, using scaled features. Kernel SVMs scale poorly beyond ~100k rows; `LinearSVC` or `SGDClassifier(loss="hinge")` scale better. Probabilities need `probability=True` (slow) or calibration.

---

### Q174. When does Naive Bayes make sense?
**Answer:** A fast, strong baseline for text classification (`MultinomialNB`) and very high-dimensional sparse data, training in one pass. It assumes feature independence, so its probabilities are poorly calibrated, but rankings are often good.

---

### Q175. Ensembles: bagging, boosting, voting, stacking?
**Answer:** Bagging averages models trained on bootstrap samples (reduces variance). Boosting trains models sequentially on errors (reduces bias). Voting combines predictions of different models. Stacking trains a meta-model on out-of-fold predictions of base models (`StackingClassifier`); watch complexity and leakage.

---

### Q176. How do you handle outliers in ML?
**Answer:** Investigate first (error vs real extreme). Options: cap/winsorize, log transform, robust scalers, robust models (tree-based, Huber loss), or remove confirmed errors. Never remove outliers using the test data or after seeing validation results.

---

### Q177. What is multicollinearity and how do you detect it?
**Answer:** Highly correlated features make linear model coefficients unstable and hard to interpret. Detect with the correlation matrix and VIF (> 5–10 is a warning sign). Fix by dropping or combining features, PCA, or regularization. Tree models are less affected for prediction but importances get split.

---

### Q178. Give feature engineering examples from real data.
**Answer:** Dates: day of week, month, days since last event, holiday flags. Transactions: counts/sums over the last 7/30/90 days, average basket, recency. Ratios: debt-to-income. Text: length, keyword flags. Interaction and log-transformed features. Always compute windows using only information available at prediction time.

---

### Q179. What are learning curves and validation curves?
**Answer:** `learning_curve` plots train and validation scores versus training size (shows if more data would help); `validation_curve` plots scores versus a hyperparameter value (shows the over/underfitting regime).

---

### Q180. What is early stopping?
**Answer:** Stop training when validation performance stops improving. `HistGradientBoosting*` supports `early_stopping=True` and `n_iter_no_change`; it saves time and limits overfitting.

---

### Q181. How do you make ML results reproducible?
**Answer:** Fix `random_state` everywhere (splits, models, CV), pin library versions, version data snapshots and code (Git, DVC), log parameters and metrics (MLflow), and keep deterministic preprocessing in a Pipeline.

---

### Q182. Strategies when scikit-learn is too slow for your data?
**Answer:** Sample for experimentation, use `n_jobs=-1`, use faster estimators (`HistGradientBoosting`, `SGD*`, `MiniBatch*`), reduce dimensionality, convert to `float32`, and use sparse formats. Beyond that, LightGBM/XGBoost, Dask-ML, Spark MLlib, or GPU libraries.

---

### Q183. Why start with a baseline model such as `DummyClassifier`?
**Answer:** A baseline (majority class, mean, last value) sets the minimum bar. If a complex model barely beats it, the problem may be the data or features. It's also the first sanity check in a pipeline.
```python
from sklearn.dummy import DummyClassifier
DummyClassifier(strategy="most_frequent").fit(X_tr, y_tr).score(X_te, y_te)
```

---

### Q184. How do you keep DataFrame column names through a Pipeline?
**Answer:** `pipeline.set_output(transform="pandas")` (sklearn 1.2+) makes transformers return DataFrames; `get_feature_names_out()` gives the output feature names, which are useful for importances and debugging.

---

### Q185. How do you monitor a model in production?
**Answer:** Track input data quality and drift, prediction distribution, latency and failures, and business KPIs. Compare predictions with actual outcomes once labels arrive (performance decay), keep a champion/challenger setup, and define retraining triggers (time-based or drift/performance-based) with alerting.

---

# SECTION 5: END-TO-END INDUSTRY SCENARIOS

### Q186. Scenario: build a customer churn pipeline end to end.
**Answer:**
1. **Data**: join customers, transactions, tickets to one row per customer at a snapshot date; label = churned in the next 30 days.
2. **Features**: recency/frequency/monetary, 7/30/90-day trends, support tickets, tenure, using only data before the snapshot.
3. **Split**: time-based train/validation/test.
4. **Model**: `Pipeline(ColumnTransformer + HistGradientBoostingClassifier)`; baseline logistic regression.
5. **Evaluate**: PR-AUC, recall at top-decile, lift; choose a threshold based on retention campaign cost.
6. **Deploy**: weekly batch scoring written to a table with model version; monitor drift.

---

### Q187. Scenario: credit card fraud detection model has 99.9% accuracy but the business is unhappy. Why?
**Answer:** Accuracy is meaningless under extreme imbalance. Report precision, recall, PR-AUC, and cost-based metrics; adjust thresholds; use class weights or resampling in training folds; add behavioral features (velocity, device change, geo distance); and set up a manual-review capacity constraint (top-N alerts per day).

---

### Q188. Scenario: how do you feed consistent features to training and online/batch prediction?
**Answer:** Define feature logic once (SQL/dbt or a feature library), materialize it in a feature store or tables with timestamps, use point-in-time-correct joins for training (no future leakage), version feature definitions, and validate schema and ranges before scoring.

---

### Q189. Scenario: a pandas job that took 10 minutes now takes 3 hours. Diagnose and fix.
**Answer:** Profile first. Common causes: input growth, a many-to-many join explosion, `apply(axis=1)` or `iterrows` loops, `concat` inside a loop, string columns instead of category, and swapping from memory pressure. Fix with vectorization, validated joins, proper dtypes, chunking, Parquet, and DuckDB/Polars/Spark if needed. Add runtime and row-count monitoring.

---

### Q190. Scenario: `merge` of two big DataFrames raises MemoryError.
**Answer:** Reduce columns and downcast types before merging, dedupe keys, filter rows early, merge in partitions by key hash or date, use `category` for the key if repeated strings, or push the join to DuckDB/SQL/Spark. Check for duplicate keys causing explosion first.

---

### Q191. Scenario: training metrics are great but production performance is poor. List the possible causes.
**Answer:** Data leakage in training, train/serve feature differences, data drift or a changed population, label definition mismatches, a wrong threshold, stale models, upstream data quality issues, and an unrepresentative validation split. Investigate by replaying production data through the training pipeline and comparing feature distributions.

---

### Q192. Scenario: prepare data for daily demand forecasting across 5,000 SKUs and 200 stores.
**Answer:** Build a complete store-SKU-day grid (fill missing days with 0 only when truly no sales, mark stock-outs separately), create lag and rolling features within group (7/14/28), calendar features (weekday, holidays, promotions), price features, and use rolling-origin backtesting. Consider one global model (LightGBM) over thousands of local models.

---

### Q193. Scenario: what data quality checks run before data enters an ML training set?
**Answer:** Schema and dtypes, null rates per column, duplicates on keys, ranges and allowed values, label distribution, freshness, row-count versus last run, distribution shift versus the previous training set, and leakage checks (features with suspiciously high importance). Fail the pipeline or quarantine the batch when thresholds are breached.

---

### Q194. Scenario: batch vs real-time scoring: how do you choose?
**Answer:** Batch (nightly/hourly) when decisions aren't time-critical (marketing, churn lists): simpler, cheaper, easy to monitor. Real-time (API) when decisions occur during user interactions (fraud at checkout, recommendations): needs low latency, online feature lookup, high availability, and a fallback. Many systems combine them (precomputed features plus real-time scoring).

---

### Q195. Scenario: clean noisy IoT sensor data (spikes, gaps, duplicates, out-of-order events).
**Answer:**
```python
df = (df.sort_values(["device", "ts"])
        .drop_duplicates(["device", "ts"], keep="last"))
df = df[df.temp.between(-40, 125)]                                     # physical limits
df["temp"] = df.groupby("device")["temp"].transform(
    lambda s: s.rolling(5, center=True, min_periods=1).median())      # despike
df = (df.set_index("ts").groupby("device")["temp"]
        .resample("1min").mean().interpolate(limit=5).reset_index())  # regular grid, small gaps only
```
Log the counts removed at each step, and keep raw data untouched.

---

### Q196. Scenario: segment customers for marketing.
**Answer:** Build customer-level features (RFM, category mix, channel), log-transform skewed values, scale, optionally reduce with PCA, run K-Means for several k and choose with silhouette plus business interpretability, profile each cluster (mean of key metrics), name segments, and write assignments with a model version. Re-run on a schedule and track segment stability.

---

### Q197. Scenario: the source system adds, renames, or drops columns, breaking your pandas ETL. How do you make it robust?
**Answer:** Select and rename an explicit set of columns with a mapping, validate the schema on read (Pandera or a column-set check), add missing optional columns with defaults (`df.reindex(columns=expected)`), log and alert on unexpected columns, quarantine files that break required-column rules, and agree on a data contract with the producer.

---

### Q198. Scenario: analyze an A/B test with data prepared in pandas.
**Answer:**
```python
g = df.groupby("variant")["converted"].agg(["sum", "count"])
from statsmodels.stats.proportion import proportions_ztest
stat, p = proportions_ztest(g["sum"], g["count"])
```
Check sample ratio mismatch (traffic split matches design), run duration and sample size set beforehand, no peeking-based early stops, one row per user, and report effect size with confidence intervals, not just the p-value.

---

### Q199. Scenario: how do you explain a model's prediction to a business stakeholder?
**Answer:** Use global importance (permutation importance), partial dependence/ICE for feature effects, and SHAP values for individual predictions ("this customer is high risk mainly due to 3 failed payments and a 60-day inactivity"). For regulated decisions prefer inherently interpretable models or provide reason codes. Translate into business language and be clear about correlation vs causation.

---

### Q200. "Tell me about a data project using pandas / scikit-learn." How do you answer?
**Answer:** Use a structured story:
- **Problem and impact**: business question, data volume, and who used the output.
- **Data work**: sources, cleaning issues (duplicates, nulls, bad dates), joins, and validation you built.
- **Method**: features, model/baseline, validation scheme, and why you chose them.
- **Challenges**: one concrete problem (leakage, imbalance, memory, drift) and how you solved it, with numbers (runtime cut from 3 hours to 20 minutes, recall improved from 60% to 78%).
- **Production**: pipeline, scheduling, monitoring, versioning.
- **Reflection**: what you'd do next (better features, drift alerts, moving heavy steps to Spark or DuckDB).

Be ready to defend each decision and discuss trade-offs.

---

## Final Preparation Tips

1. **Practice on real messy data.** Interviewers care more about cleaning, joining, and validating than about fancy models.
2. **Know the "why" behind pandas choices**: vectorization, dtypes, memory, and when to leave pandas.
3. **Master leakage and validation.** It's the most common scikit-learn interview theme.
4. **Be fluent in metrics** and when each is appropriate, especially with imbalanced data.
5. **Write from memory**: top-N per group, latest record per key, rolling features, sessionization, a full Pipeline with ColumnTransformer.
6. **Connect everything to production**: idempotency, monitoring, drift, versioning, and data contracts.

Good luck with your interview!

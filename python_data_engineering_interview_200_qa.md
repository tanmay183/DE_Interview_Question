# 200 Python Interview Questions for Data Engineers (2 Years Experience)

Real-industry, scenario-driven questions with answers an experienced professional would give.

## Table of Contents

| Section | Questions |
|---|---|
| 1. Core Python | Q1 – Q35 |
| 2. OOP, Testing & Best Practices | Q36 – Q50 |
| 3. Pandas & NumPy | Q51 – Q90 |
| 4. File Formats & Storage | Q91 – Q100 |
| 5. PySpark | Q101 – Q130 |
| 6. Python + Databases / SQL | Q131 – Q140 |
| 7. Airflow & Orchestration | Q141 – Q150 |
| 8. APIs, Streaming, Cloud, Quality | Q151 – Q170 |
| 9. Performance & Concurrency | Q171 – Q180 |
| 10. Coding Rounds & Production Scenarios | Q181 – Q200 |

---

# SECTION 1: CORE PYTHON

### Q1. What is the difference between mutable and immutable types? Why does it matter in data pipelines?
**Answer:** Mutable objects (`list`, `dict`, `set`, `bytearray`) can be changed in place; immutable ones (`int`, `float`, `str`, `tuple`, `frozenset`, `bytes`) cannot. It matters because:
- Immutable objects are hashable (if their contents are), so they can be dict keys or set members (e.g., `(customer_id, date)` as a key).
- Passing a mutable object into a function can cause side effects on the caller's data.
- Immutable config objects are safer to share across threads.

---

### Q2. Difference between list, tuple, set, and dict? When do you use each?
**Answer:**
- **list**: ordered, mutable, allows duplicates. Use for sequences of records.
- **tuple**: ordered, immutable. Use for fixed records, dict keys, returning multiple values.
- **set**: unordered, unique, O(1) membership. Use for deduplication and comparing key sets (e.g., finding IDs in source but missing in target).
- **dict**: key-value, O(1) average lookup. Use for lookups and mappings (e.g., code → description).

---

### Q3. What are the time complexities of common operations on list, dict, and set?
**Answer:**
- `list`: index O(1), append O(1) amortized, `x in list` O(n), insert at front O(n).
- `dict`/`set`: get, set, delete, and `in` are O(1) average, O(n) worst case.
- Sorting is O(n log n).

**Scenario:** Checking 1M IDs against a list of 1M IDs with `in` is O(n²). Converting the list to a `set` first drops it to roughly O(n).

---

### Q4. Shallow copy vs deep copy?
**Answer:** A shallow copy (`copy.copy`, `list[:]`, `.copy()`) creates a new outer container but shares the inner objects. A deep copy (`copy.deepcopy`) recursively copies everything.
```python
import copy
a = [[1, 2], [3]]
b = copy.copy(a);     b[0].append(99)   # a also changes
c = copy.deepcopy(a); c[0].append(100)  # a unchanged
```
In pipelines, always deep copy config dicts before modifying them per-run.

---

### Q5. `is` vs `==`?
**Answer:** `==` compares values, `is` compares object identity (same memory object). Use `is` only for `None`, `True`, `False` (`if x is None`). Small ints and some strings are interned, so `is` can give misleading results.

---

### Q6. What are `*args` and `**kwargs`?
**Answer:** `*args` collects extra positional arguments into a tuple; `**kwargs` collects extra keyword arguments into a dict.
```python
def run_job(name, *args, **kwargs):
    print(name, args, kwargs)
run_job("etl", 1, 2, env="prod", retries=3)
```
Useful for wrapper functions, decorators, and passing options through to underlying libraries.

---

### Q7. What is the mutable default argument problem?
**Answer:** Default values are evaluated once at function definition time.
```python
def add(item, bucket=[]):    # BUG: the same list is reused across calls
    bucket.append(item); return bucket

def add(item, bucket=None):  # FIX
    bucket = [] if bucket is None else bucket
    bucket.append(item); return bucket
```
This causes real bugs, such as records from a previous batch leaking into the next batch.

---

### Q8. List comprehension vs generator expression?
**Answer:** `[x*2 for x in data]` builds the entire list in memory. `(x*2 for x in data)` is lazy and yields one item at a time with constant memory. Use generators for big data or streaming; use lists when you need to index, reuse, or take `len`.

---

### Q9. What are generators and `yield`? Scenario: process a 50 GB CSV on a 8 GB machine.
**Answer:** A generator function uses `yield` to produce values lazily, preserving its state between calls.
```python
def read_rows(path):
    with open(path) as f:
        next(f)                    # skip header
        for line in f:
            yield line.rstrip("\n").split(",")

for batch in read_rows("big.csv"):
    ...
```
Only one row is in memory at a time. In pandas you'd use `chunksize`.

---

### Q10. Iterable vs iterator?
**Answer:** An iterable has `__iter__` and returns an iterator (list, dict, str). An iterator has `__next__` and keeps state; it raises `StopIteration` when exhausted. Iterators can be consumed only once, a common bug when reusing a generator twice.

---

### Q11. What is a decorator? Write a retry decorator for flaky API calls.
**Answer:** A decorator is a function that wraps another function to add behavior.
```python
import time, functools

def retry(times=3, delay=2, exceptions=(Exception,)):
    def deco(fn):
        @functools.wraps(fn)
        def wrapper(*a, **kw):
            for attempt in range(1, times + 1):
                try:
                    return fn(*a, **kw)
                except exceptions as e:
                    if attempt == times:
                        raise
                    time.sleep(delay * 2 ** (attempt - 1))   # exponential backoff
        return wrapper
    return deco

@retry(times=4, delay=1, exceptions=(ConnectionError,))
def fetch(): ...
```

---

### Q12. What are context managers? When do you write a custom one?
**Answer:** They guarantee setup and cleanup via `with`. Custom ones are useful for DB connections, temp files, timers, and locks.
```python
from contextlib import contextmanager
import time

@contextmanager
def timer(label):
    s = time.perf_counter()
    try:
        yield
    finally:
        print(f"{label}: {time.perf_counter()-s:.2f}s")

with timer("load"):
    ...
```

---

### Q13. Explain LEGB scope rule.
**Answer:** Python looks up names in order: Local → Enclosing → Global → Built-in.

---

### Q14. `global` vs `nonlocal`?
**Answer:** `global` lets a function rebind a module-level variable; `nonlocal` lets a nested function rebind a variable in the enclosing function. Avoid `global` in pipelines; pass state explicitly or use classes.

---

### Q15. What is a closure?
**Answer:** A nested function that remembers variables from its enclosing scope after the outer function returns.
```python
def make_multiplier(n):
    return lambda x: x * n
double = make_multiplier(2)
```
Used for configurable transformers and decorators.

---

### Q16. Explain `map`, `filter`, `reduce`, and `lambda`.
**Answer:** `map(f, it)` applies `f` to each item; `filter(f, it)` keeps items where `f` is truthy; `functools.reduce(f, it)` folds to a single value. `lambda` is an anonymous one-expression function. Comprehensions are generally more readable than `map`/`filter`.

---

### Q17. What is the GIL and how does it affect data engineering code?
**Answer:** The Global Interpreter Lock lets only one thread execute Python bytecode at a time in CPython. Consequences:
- CPU-bound work gains nothing from threads → use `multiprocessing` or PySpark.
- I/O-bound work (API calls, S3 reads) does benefit from threads because the GIL is released during I/O.
- Libraries like NumPy/pandas release the GIL in C code for many operations.

---

### Q18. Threading vs multiprocessing vs asyncio: when to use which?
**Answer:**
- **Threading**: I/O-bound, simple, shared memory (calling 500 REST endpoints).
- **Multiprocessing**: CPU-bound, separate memory (parsing/compressing many files, heavy transforms).
- **asyncio**: very high-concurrency I/O with `async` libraries (aiohttp, asyncpg), single thread, lowest overhead.

---

### Q19. Explain `try / except / else / finally`.
**Answer:** `try` runs the risky code; `except` handles errors; `else` runs only if no exception occurred; `finally` always runs (cleanup). Catch specific exceptions, never a bare `except:`, because it hides `KeyboardInterrupt` and real bugs.

---

### Q20. How do you design custom exceptions for a pipeline?
**Answer:**
```python
class PipelineError(Exception): ...
class SchemaMismatchError(PipelineError): ...
class DataQualityError(PipelineError): ...

try:
    validate(df)
except DataQualityError as e:
    alert(e); raise
```
A hierarchy lets the orchestrator decide: retry on transient errors, fail fast on data quality errors.

---

### Q21. What are dunder (magic) methods? Difference between `__init__` and `__new__`?
**Answer:** Dunder methods like `__init__`, `__repr__`, `__len__`, `__eq__`, `__enter__` customize object behavior. `__new__` creates the instance (rarely overridden, used for singletons/immutables); `__init__` initializes the already-created instance.

---

### Q22. `@staticmethod`, `@classmethod`, `@property`?
**Answer:**
- `@staticmethod`: no access to `self` or `cls`, a utility function in the class namespace.
- `@classmethod`: receives `cls`; used for alternate constructors (`Config.from_yaml(path)`).
- `@property`: access a method like an attribute, for computed or validated fields.

---

### Q23. What are dataclasses and why use them?
**Answer:** `@dataclass` auto-generates `__init__`, `__repr__`, `__eq__`.
```python
from dataclasses import dataclass, field

@dataclass(frozen=True)
class JobConfig:
    source: str
    target: str
    batch_size: int = 10_000
    tags: list = field(default_factory=list)
```
Great for typed, immutable pipeline configs.

---

### Q24. Why use type hints? Do they enforce types?
**Answer:** They improve readability, IDE support, and static checking with `mypy`. They are not enforced at runtime (use `pydantic` if you need runtime validation).
```python
def load(path: str, limit: int | None = None) -> list[dict]: ...
```

---

### Q25. How does Python manage memory?
**Answer:** Private heap, reference counting plus a cyclic garbage collector for reference cycles. Objects are freed when their ref count hits zero. Memory leaks usually come from global caches, unclosed resources, or lingering references.

---

### Q26. What is `__slots__`?
**Answer:** It replaces the per-instance `__dict__` with fixed attributes, reducing memory when creating millions of small objects.

---

### Q27. Remove duplicates from a list while preserving order.
**Answer:**
```python
def dedupe(seq):
    seen = set()
    return [x for x in seq if not (x in seen or seen.add(x))]

list(dict.fromkeys(seq))   # simpler, works for hashable items (3.7+)
```

---

### Q28. How do you merge two dictionaries?
**Answer:**
```python
c = a | b          # 3.9+, b wins on conflicts
c = {**a, **b}     # older versions
a.update(b)        # in place
```

---

### Q29. Explain `defaultdict`, `Counter`, `deque`, `namedtuple`.
**Answer:**
- `defaultdict(list)`: auto-creates missing keys (grouping records).
- `Counter`: frequency counts, `most_common(n)`.
- `deque`: O(1) appends and pops on both ends (sliding windows, queues).
- `namedtuple`: lightweight immutable record with named fields.

---

### Q30. How does sorting work with `key`? Is it stable?
**Answer:** `sorted(data, key=lambda r: (r["dept"], -r["salary"]))`. Python's sort (Timsort) is stable: equal elements keep their original order, which allows multi-pass sorting.

---

### Q31. Explain slicing, including negative indices and steps.
**Answer:** `seq[start:stop:step]`; `seq[-3:]` last three, `seq[::-1]` reversed, `seq[::2]` every second item. Slicing creates a new object (copy for lists).

---

### Q32. f-strings vs `.format()` vs `%`?
**Answer:** f-strings (3.6+) are fastest and most readable: `f"{name}: {value:,.2f}"`. Avoid building SQL with f-strings (SQL injection); use parameterized queries.

---

### Q33. How do you read a very large file efficiently?
**Answer:** Iterate line by line (`for line in f`), use `pandas.read_csv(chunksize=...)`, read binary in blocks (`f.read(1024*1024)`), or use Parquet/Arrow with column and row-group pruning. Never call `f.read()` or `readlines()` on a huge file.

---

### Q34. How do you manage dependencies and environments?
**Answer:** Use isolated virtual environments (`venv`, `conda`, `poetry`, `uv`), pin versions in `requirements.txt` or `poetry.lock`, separate dev and prod dependencies, and build Docker images from locked files so dev, test, and prod match.

---

### Q35. What does `if __name__ == "__main__":` do?
**Answer:** It runs code only when the file is executed directly, not when imported. It allows a module to be both reusable and runnable (e.g., a job entry point).

---

# SECTION 2: OOP, TESTING & BEST PRACTICES

### Q36. Explain inheritance, MRO, and `super()`.
**Answer:** A child class inherits from a parent. MRO (Method Resolution Order, C3 linearization) decides which class's method is used with multiple inheritance (`Class.__mro__`). `super()` calls the next class in the MRO.

---

### Q37. How would you design a reusable pipeline base class using ABCs?
**Answer:**
```python
from abc import ABC, abstractmethod

class BasePipeline(ABC):
    def run(self):
        data = self.extract()
        data = self.transform(data)
        self.load(data)

    @abstractmethod
    def extract(self): ...
    @abstractmethod
    def transform(self, data): ...
    @abstractmethod
    def load(self, data): ...
```
This is the template method pattern: shared flow (logging, retries) and source-specific implementations.

---

### Q38. Composition vs inheritance?
**Answer:** Inheritance means "is-a"; composition means "has-a". Prefer composition for flexibility. For example, a `Pipeline` that holds a `Reader`, `Transformer`, and `Writer` is easier to test and swap than a deep class hierarchy.

---

### Q39. What is duck typing / polymorphism?
**Answer:** If an object has the required methods, it can be used regardless of its class. Any object with `.read()` works as a file-like input, so tests can pass `io.StringIO` instead of a real file.

---

### Q40. Useful `itertools` and built-ins for data work?
**Answer:** `enumerate`, `zip`, `zip_longest`, `chain`, `islice` (batching), `groupby` (needs sorted input), `product`, `accumulate`.
```python
from itertools import islice
def batches(it, n):
    it = iter(it)
    while chunk := list(islice(it, n)):
        yield chunk
```

---

### Q41. What is `functools.lru_cache`?
**Answer:** It memoizes function results. Great for repeated lookups (e.g., mapping a country code to a region). Arguments must be hashable. Beware unbounded growth with `maxsize=None`.

---

### Q42. `pickle` vs JSON for serialization?
**Answer:** JSON is human-readable, language-neutral, and safe, but limited to basic types. Pickle handles arbitrary Python objects but is Python-only and insecure for untrusted data (arbitrary code execution). For data interchange prefer JSON, Parquet, or Avro.

---

### Q43. Scenario: parse web server logs with regex to count errors per hour.
**Answer:**
```python
import re
from collections import Counter

pat = re.compile(r'\[(\d{2}/\w{3}/\d{4}):(\d{2}):\d{2}:\d{2} .*?\] ".*?" (\d{3})')
errors = Counter()
with open("access.log") as f:
    for line in f:
        m = pat.search(line)
        if m and m.group(3).startswith("5"):
            errors[(m.group(1), m.group(2))] += 1
```
Compile once, stream the file, count with `Counter`.

---

### Q44. How do you handle time zones correctly?
**Answer:** Store and process everything in UTC; convert to local time only for display. Use timezone-aware datetimes.
```python
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
ist = datetime(2026, 10, 24, 22, 0, tzinfo=ZoneInfo("Asia/Kolkata"))
utc = ist.astimezone(timezone.utc)
```
Avoid naive datetimes; they cause off-by-5:30 bugs between systems.

---

### Q45. Why not use `float` for money? What do you use?
**Answer:** Floats are binary approximations (`0.1 + 0.2 != 0.3`). Use `decimal.Decimal` (created from strings) or store integer paise/cents. In Spark use `DecimalType(18,2)`.

---

### Q46. How do you unit test a transformation function? Using pytest and mock.
**Answer:**
```python
import pandas as pd
from unittest.mock import patch

def add_total(df):
    df = df.copy(); df["total"] = df.qty * df.price; return df

def test_add_total():
    df = pd.DataFrame({"qty": [2], "price": [5.0]})
    assert add_total(df)["total"].iloc[0] == 10.0

@patch("pipeline.requests.get")
def test_fetch(mock_get):
    mock_get.return_value.json.return_value = {"ok": True}
    ...
```
Keep transforms as pure functions, mock external I/O, and use fixtures for sample data.

---

### Q47. Logging best practices in pipelines?
**Answer:** Use the `logging` module (not `print`), set levels (DEBUG/INFO/WARNING/ERROR), include context (job name, batch id, row counts, durations), log to stdout in containers, never log secrets or PII, and use `logger.exception()` inside `except` to capture stack traces.
```python
import logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
log = logging.getLogger(__name__)
log.info("Loaded %d rows into %s", n, table)
```

---

### Q48. How do you manage configuration and secrets?
**Answer:** Keep config in YAML/JSON/env vars, separated per environment. Secrets belong in a secret manager (AWS Secrets Manager, Azure Key Vault, HashiCorp Vault) or the orchestrator's connections store, never in code or Git. Read them at runtime via `os.environ` or the SDK.

---

### Q49. Modern Python features worth knowing?
**Answer:** f-strings, walrus operator `:=`, `match` statement (3.10), union types `int | None`, `dataclasses`, `pathlib`, `zoneinfo`, `typing` improvements, and better error messages.

---

### Q50. What is idempotency and how do you achieve it in Python pipelines?
**Answer:** Running a job multiple times for the same input gives the same result with no duplicates. Techniques: overwrite a partition instead of appending, `MERGE`/upsert on a business key, write to temp then atomic rename, deterministic file names, and track processed batches in a control table. This makes retries and backfills safe.

---

# SECTION 3: PANDAS & NUMPY

### Q51. Scenario: a 20 GB CSV doesn't fit in memory. How do you process it with pandas?
**Answer:** Read in chunks, aggregate incrementally, and write out results.
```python
import pandas as pd
parts = []
for chunk in pd.read_csv("sales.csv", chunksize=500_000, usecols=["region", "amount"]):
    parts.append(chunk.groupby("region")["amount"].sum())
result = pd.concat(parts).groupby(level=0).sum()
```
Also use `usecols`, explicit `dtype`, or switch to Parquet, Dask, Polars, DuckDB, or PySpark.

---

### Q52. How do you reduce DataFrame memory usage?
**Answer:**
- Downcast numerics: `pd.to_numeric(s, downcast="integer")`.
- Use `category` for low-cardinality strings.
- Load only needed columns.
- Use nullable or smaller types (`int32`, `float32`).
- Check with `df.info(memory_usage="deep")`.

Often cuts memory by 60–80%.

---

### Q53. `loc` vs `iloc`?
**Answer:** `loc` is label-based and slices are inclusive; `iloc` is position-based and the end is exclusive. `df.loc[df.age > 30, "name"]` for boolean filtering with a column.

---

### Q54. How do you handle missing values?
**Answer:** First understand why they're missing. Options: `dropna()`, `fillna()` with constant, median, or group-wise value, forward/backward fill for time series, or flag with an indicator column. Never silently fill business-critical fields. Document the rule.
```python
df["salary"] = df["salary"].fillna(df.groupby("dept")["salary"].transform("median"))
```

---

### Q55. Explain merge types (inner, left, right, outer) with a use case.
**Answer:** Inner keeps matches only; left keeps all left rows (enrich orders with customer data); right is the mirror; outer keeps everything (reconciliation).
```python
orders.merge(customers, on="customer_id", how="left", validate="m:1")
```
`validate` catches duplicate-key issues early.

---

### Q56. `merge` vs `join` vs `concat`?
**Answer:** `merge` joins on columns (SQL style), `join` joins on index by default, `concat` stacks frames vertically (`axis=0`) or side by side (`axis=1`).

---

### Q57. Explain `groupby` with multiple aggregations.
**Answer:**
```python
df.groupby("region").agg(
    total=("amount", "sum"),
    avg=("amount", "mean"),
    orders=("order_id", "nunique"),
).reset_index()
```
Named aggregation gives clean output column names.

---

### Q58. `apply` vs vectorized operations: which is faster and why?
**Answer:** Vectorized operations run in optimized C loops; `apply` calls Python per row/element and can be 10–100× slower.
```python
df["total"] = df.qty * df.price                       # fast
df["total"] = df.apply(lambda r: r.qty * r.price, axis=1)  # slow
```
Use `np.where`, `np.select`, or `.str`/`.dt` accessors instead of row-wise apply.

---

### Q59. How do you find and remove duplicates?
**Answer:**
```python
df.duplicated(subset=["id"], keep=False)      # inspect all copies
df.drop_duplicates(subset=["id"], keep="last")
```
Decide the business rule: exact duplicates versus the latest version of a record.

---

### Q60. Explain `pivot`, `pivot_table`, and `melt`.
**Answer:** `pivot_table` reshapes long to wide with aggregation (sales by month × product). `melt` does the reverse: wide to long (columns `jan`, `feb`... become rows), which is useful before loading into normalized tables.

---

### Q61. Rolling, rank, shift: how do you replicate SQL window functions?
**Answer:**
```python
df["prev_amt"]  = df.groupby("cust")["amt"].shift(1)
df["rank"]      = df.groupby("dept")["sal"].rank(method="dense", ascending=False)
df["roll7"]     = df.groupby("cust")["amt"].transform(lambda s: s.rolling(7).mean())
df["running"]   = df.groupby("cust")["amt"].cumsum()
```

---

### Q62. Scenario: keep only the latest record per customer from a change feed.
**Answer:**
```python
latest = (df.sort_values("updated_at")
            .drop_duplicates("customer_id", keep="last"))
# alternative
latest = df.loc[df.groupby("customer_id")["updated_at"].idxmax()]
```

---

### Q63. What is `SettingWithCopyWarning`?
**Answer:** It appears when you assign to a slice that may be a view or a copy (chained indexing like `df[df.a > 1]["b"] = 0`). Fix with `.loc` (`df.loc[df.a > 1, "b"] = 0`) or an explicit `.copy()`.

---

### Q64. How do you do time-series aggregation (daily to monthly)?
**Answer:**
```python
df["ts"] = pd.to_datetime(df["ts"], utc=True)
monthly = df.set_index("ts").resample("MS")["amount"].sum()
```
Handle missing periods with `.asfreq()` or `reindex` with a full date range.

---

### Q65. How do you clean messy string columns?
**Answer:**
```python
df["email"] = df["email"].str.strip().str.lower()
df["phone"] = df["phone"].str.replace(r"\D", "", regex=True)
df["name"]  = df["name"].str.title()
```

---

### Q66. When do you use the `category` dtype?
**Answer:** For columns with few unique values and many rows (country, status). Saves memory and speeds up groupby and comparisons.

---

### Q67. Scenario: flatten nested JSON from an API into a table.
**Answer:**
```python
import pandas as pd
df = pd.json_normalize(data, record_path=["items"],
                       meta=["order_id", ["customer", "id"]], sep="_")
```
For lists inside columns, use `explode()`.

---

### Q68. How do you detect outliers?
**Answer:** IQR rule (below Q1−1.5×IQR or above Q3+1.5×IQR), z-score, or domain thresholds.
```python
q1, q3 = df.amt.quantile([.25, .75]); iqr = q3 - q1
out = df[(df.amt < q1 - 1.5*iqr) | (df.amt > q3 + 1.5*iqr)]
```
In pipelines, flag and quarantine rather than silently delete.

---

### Q69. How do you implement data quality checks in pandas?
**Answer:** Check row counts, nulls in required columns, uniqueness of keys, allowed values, ranges, and referential integrity; fail or quarantine on violation.
```python
assert df["id"].is_unique, "duplicate ids"
assert df["amount"].ge(0).all(), "negative amounts"
assert df["email"].notna().mean() > 0.98, "too many null emails"
```
For production, use Great Expectations, Pandera, or Soda.

---

### Q70. Scenario: reconcile source vs target. Find records missing on either side.
**Answer:**
```python
m = src.merge(tgt, on="id", how="outer", indicator=True)
only_src = m[m["_merge"] == "left_only"]
only_tgt = m[m["_merge"] == "right_only"]
```

---

### Q71. A join suddenly doubled your row count. Why, and how do you prevent it?
**Answer:** Duplicate keys on the right side create a many-to-many join explosion. Prevent with `validate="m:1"`, dedupe the dimension first, and assert row counts before and after the join.

---

### Q72. Cumulative sum and percent of total per group?
**Answer:**
```python
df["cum"] = df.groupby("region")["amt"].cumsum()
df["pct"] = df["amt"] / df.groupby("region")["amt"].transform("sum")
```

---

### Q73. What does `explode` do?
**Answer:** Turns each element of a list-like column into its own row (e.g., one order row with a list of tags becomes one row per tag).

---

### Q74. How do you move data between pandas and a database efficiently?
**Answer:**
```python
from sqlalchemy import create_engine
eng = create_engine(url)
for chunk in pd.read_sql("select * from big", eng, chunksize=100_000):
    ...
df.to_sql("t", eng, if_exists="append", index=False, chunksize=10_000, method="multi")
```
For big loads, use database-native bulk paths (`COPY`, `COPY INTO`) instead of row-wise inserts.

---

### Q75. Pandas vs PySpark: how do you choose?
**Answer:** Pandas for data that fits in single-machine memory (rule of thumb under a few GB), quick analysis, and small jobs. PySpark when data is tens of GB or larger, you need distributed compute, or the platform is already Spark-based (Databricks, EMR). Middle ground: Polars, DuckDB, Dask.

---

### Q76. Implement SCD Type 2 logic conceptually in pandas.
**Answer:** Compare incoming vs current dimension rows on the business key. For changed rows: set `end_date = today` and `is_current = False` on the old row, and append a new row with `start_date = today`, `end_date = 9999-12-31`, `is_current = True`. New keys are appended; unchanged rows are left alone. At scale, do this with SQL `MERGE` or Delta Lake.

---

### Q77. How do you validate schema in code?
**Answer:**
```python
import pandera as pa
schema = pa.DataFrameSchema({
    "id": pa.Column(int, unique=True),
    "amount": pa.Column(float, pa.Check.ge(0)),
    "country": pa.Column(str, pa.Check.isin(["IN", "US", "UK"])),
})
schema.validate(df)
```

---

### Q78. What is NumPy broadcasting?
**Answer:** NumPy automatically expands arrays of different shapes for element-wise operations when dimensions are compatible (`arr - arr.mean(axis=0)` subtracts a row from every row). It avoids Python loops and explicit copies.

---

### Q79. Get the top 3 salaries per department.
**Answer:**
```python
top3 = (df.sort_values("salary", ascending=False)
          .groupby("dept").head(3))
```

---

### Q80. How do you bucket continuous values?
**Answer:**
```python
df["band"] = pd.cut(df["age"], bins=[0, 18, 35, 60, 120],
                    labels=["minor", "young", "adult", "senior"])
```
`pd.qcut` makes equal-sized quantile buckets.

---

### Q81. How do you profile memory and speed of pandas code?
**Answer:** `df.memory_usage(deep=True)`, `%timeit`/`%memit` in notebooks, `cProfile`, `tracemalloc`, and `memory_profiler` for line-level analysis.

---

### Q82. Read multiple Excel sheets into one DataFrame.
**Answer:**
```python
sheets = pd.read_excel("report.xlsx", sheet_name=None)   # dict of DataFrames
df = pd.concat(sheets.values(), keys=sheets.keys(), names=["sheet"]).reset_index(level=0)
```

---

### Q83. How do you deal with encoding errors in CSV files?
**Answer:** Try `encoding="utf-8-sig"` (BOM), `latin-1`/`cp1252`, or detect with `chardet`. As a last resort `encoding_errors="replace"`. Standardize to UTF-8 on ingestion and log the original encoding.

---

### Q84. How do you handle bad rows in a CSV (extra commas, broken lines)?
**Answer:**
```python
df = pd.read_csv("f.csv", on_bad_lines="warn")   # or "skip"
```
Better: capture bad lines in a rejects file for review, as silently dropping data hides upstream problems.

---

### Q85. Filtering techniques: `isin`, `query`, `between`?
**Answer:**
```python
df[df.status.isin(["A", "B"])]
df.query("amount > 100 and country == 'IN'")
df[df.date.between("2026-01-01", "2026-03-31")]
```

---

### Q86. Mapping codes to values with `map` / `replace`?
**Answer:** `df["status"].map({1: "ACTIVE", 0: "INACTIVE"})` gives NaN for unmapped keys (good for spotting unknowns); `replace` leaves unmapped values unchanged.

---

### Q87. Scenario: implement incremental (watermark) loading from a source table.
**Answer:**
1. Read the last successful watermark (max `updated_at`) from a control table.
2. Query `WHERE updated_at > :watermark`.
3. Load the rows with upsert logic.
4. Update the watermark only after the load succeeds.
```python
wm = get_watermark("orders")
df = pd.read_sql("select * from orders where updated_at > %(wm)s", eng, params={"wm": wm})
```
Use an overlap window (e.g., minus 5 minutes) with dedupe to survive late commits.

---

### Q88. Scenario: sessionize clickstream events (new session after 30 min of inactivity).
**Answer:**
```python
df = df.sort_values(["user", "ts"])
gap = df.groupby("user")["ts"].diff() > pd.Timedelta(minutes=30)
df["session_id"] = gap.groupby(df["user"]).cumsum()
```

---

### Q89. How do you compare two DataFrames?
**Answer:** `df1.equals(df2)` for strict equality, `df1.compare(df2)` for cell differences (same shape and labels), `pd.testing.assert_frame_equal` in tests, and the outer-merge indicator for row-level reconciliation.

---

### Q90. How do you handle timezone-aware columns?
**Answer:**
```python
df["ts"] = pd.to_datetime(df["ts"], utc=True)
df["ts_ist"] = df["ts"].dt.tz_convert("Asia/Kolkata")
```
Mixing naive and aware datetimes raises errors, so normalize on ingestion.

---

# SECTION 4: FILE FORMATS & STORAGE

### Q91. CSV vs JSON vs Parquet vs Avro vs ORC?
**Answer:**
| Format | Layout | Best for |
|---|---|---|
| CSV | Row, text, no schema | Simple exchange, small data |
| JSON/JSONL | Row, text, nested | APIs, logs, semi-structured |
| Parquet | Columnar, binary, schema, compressed | Analytics, data lakes |
| ORC | Columnar, Hive ecosystem | Hive/Presto workloads |
| Avro | Row, binary, schema evolution | Kafka, streaming, write-heavy |

---

### Q92. How do you read and write partitioned Parquet in Python?
**Answer:**
```python
df.to_parquet("out/", partition_cols=["year", "month"], engine="pyarrow", compression="snappy")
pd.read_parquet("out/", filters=[("year", "=", 2026)])
```
Partition filters skip whole folders (partition pruning).

---

### Q93. How do you choose partition columns?
**Answer:** Pick columns commonly used in filters with low-to-moderate cardinality (date, region). Avoid high-cardinality columns (user_id), which create millions of tiny files. Aim for partition files of roughly 128 MB–1 GB.

---

### Q94. What is JSON Lines (NDJSON) and why prefer it for big data?
**Answer:** One JSON object per line. It can be streamed and split line by line, unlike a single giant JSON array that needs full parsing.
```python
import json
with open("events.jsonl") as f:
    for line in f:
        rec = json.loads(line)
```

---

### Q95. What is schema evolution and how do you handle it?
**Answer:** Source schemas change (new columns, type changes). Strategies: formats with schema evolution support (Avro, Parquet with `mergeSchema`, Delta), additive-only changes, a schema registry, defaulting missing columns, and alerting on breaking changes (dropped columns, type narrowing) instead of failing silently.

---

### Q96. Compression: snappy vs gzip vs zstd?
**Answer:** Snappy: fast, moderate ratio (Parquet default, good for analytics). Gzip: higher ratio, slower, not splittable as raw text files. Zstd: great balance of ratio and speed. For text (CSV/JSON) on Spark, gzip files are not splittable, so use many smaller files or a splittable codec/format.

---

### Q97. What is the small files problem?
**Answer:** Thousands of tiny files overload the metadata layer and create excessive tasks in Spark/Hive, slowing reads. Fix by compacting (`coalesce`/`repartition` before write, Delta `OPTIMIZE`), batching writes, and tuning streaming trigger intervals.

---

### Q98. Read a file from S3 using boto3.
**Answer:**
```python
import boto3, io, pandas as pd
s3 = boto3.client("s3")
obj = s3.get_object(Bucket="my-bucket", Key="raw/2026/10/24/data.csv")
df = pd.read_csv(io.BytesIO(obj["Body"].read()))
# or: pd.read_parquet("s3://my-bucket/path/", storage_options={...}) with s3fs
```
Use IAM roles rather than hardcoded keys.

---

### Q99. How do you process a huge JSON file that is one big array?
**Answer:** Use a streaming parser like `ijson` to iterate items without loading the whole file.
```python
import ijson
with open("big.json", "rb") as f:
    for item in ijson.items(f, "item"):
        process(item)
```

---

### Q100. How do you read XML or Avro in Python?
**Answer:** XML: `xml.etree.ElementTree.iterparse` for streaming big files, or `lxml`; `pd.read_xml` for small flat files. Avro: `fastavro.reader(f)` yields records with schema embedded in the file.

---

# SECTION 5: PYSPARK

### Q101. Explain Spark architecture.
**Answer:** The **driver** runs your program, builds the DAG, and schedules tasks. A **cluster manager** (YARN, Kubernetes, Standalone, Databricks) allocates resources. **Executors** run tasks on partitions and hold cached data. A job splits into stages (at shuffle boundaries) and stages into tasks (one per partition).

---

### Q102. RDD vs DataFrame vs Dataset?
**Answer:** RDD: low-level, no schema, no optimizer. DataFrame: schema-based, optimized by Catalyst, the default in PySpark. Dataset: typed API available only in Scala/Java. In PySpark, use DataFrames.

---

### Q103. Transformations vs actions? What is lazy evaluation?
**Answer:** Transformations (`select`, `filter`, `join`, `groupBy`) build a plan and don't execute. Actions (`count`, `collect`, `show`, `write`) trigger execution. Laziness lets Spark optimize the whole plan (predicate pushdown, column pruning, join reordering) before running.

---

### Q104. Narrow vs wide transformations?
**Answer:** Narrow: each output partition depends on one input partition (`map`, `filter`), with no shuffle. Wide: data moves across partitions (`groupBy`, `join`, `distinct`, `repartition`), causing a shuffle and a new stage.

---

### Q105. What is a shuffle and why is it expensive?
**Answer:** Redistribution of data across executors by key. It involves disk writes, network transfer, and serialization. Minimize with broadcast joins, filtering early, pre-partitioned data, and fewer wide operations. Tune `spark.sql.shuffle.partitions` (default 200).

---

### Q106. `repartition` vs `coalesce`?
**Answer:** `repartition(n)` does a full shuffle and can increase or decrease partitions with even sizes. `coalesce(n)` only decreases, avoids a full shuffle by merging partitions, and may leave uneven sizes. Use `coalesce` before writing to reduce output file count.

---

### Q107. Scenario: join a 2 TB fact table with a 50 MB dimension table. What do you do?
**Answer:** Broadcast the small table so no shuffle of the big table is needed.
```python
from pyspark.sql.functions import broadcast
result = fact.join(broadcast(dim), "product_id", "left")
```
Auto-broadcast threshold is `spark.sql.autoBroadcastJoinThreshold` (default 10 MB).

---

### Q108. Scenario: a join/groupBy has one task running for hours while others finish in seconds. What is it?
**Answer:** **Data skew**: one key (e.g., `customer_id = NULL` or a huge customer) holds a large share of the data. Fixes:
- Enable AQE skew join handling (`spark.sql.adaptive.skewJoin.enabled`).
- Salting: add a random suffix to the hot key on the big side and explode the small side.
- Filter or handle NULL/default keys separately.
- Broadcast the smaller side if possible.
```python
from pyspark.sql import functions as F
N = 10
big  = big.withColumn("salt", (F.rand()*N).cast("int"))
small = small.withColumn("salt", F.explode(F.array([F.lit(i) for i in range(N)])))
joined = big.join(small, ["key", "salt"])
```

---

### Q109. `cache()` vs `persist()`? When to use?
**Answer:** `cache()` = `persist(MEMORY_AND_DISK)` for DataFrames. Use when a DataFrame is reused by multiple actions. `persist` lets you choose storage levels. Always `unpersist()` when done. Don't cache everything: it wastes memory and may slow the job.

---

### Q110. Scenario: get the latest record per key in PySpark.
**Answer:**
```python
from pyspark.sql import Window, functions as F
w = Window.partitionBy("customer_id").orderBy(F.col("updated_at").desc())
latest = (df.withColumn("rn", F.row_number().over(w))
            .filter("rn = 1").drop("rn"))
```

---

### Q111. Built-in functions vs Python UDF vs Pandas UDF?
**Answer:** Built-ins run in the JVM and are fully optimized: always prefer them. Python UDFs serialize row by row between JVM and Python (slow, opaque to the optimizer). Pandas (vectorized) UDFs use Arrow and process batches, so they are much faster than plain UDFs when you truly need custom Python logic.

---

### Q112. Why define a schema explicitly instead of `inferSchema`?
**Answer:** Inference requires an extra pass over the data (slow), may guess wrong types, and can differ between runs. An explicit `StructType` is faster, deterministic, and acts as a contract.
```python
from pyspark.sql.types import *
schema = StructType([StructField("id", LongType(), False),
                     StructField("amt", DecimalType(18, 2)),
                     StructField("ts", TimestampType())])
df = spark.read.schema(schema).csv(path, header=True)
```

---

### Q113. How do you handle nulls in PySpark?
**Answer:** `df.na.fill({"qty": 0})`, `df.na.drop(subset=["id"])`, `F.coalesce(col1, col2, F.lit(0))`, and `col.isNull()`. Remember that null comparisons with `==` return null, so use `eqNullSafe` when needed.

---

### Q114. Scenario: read nested JSON with arrays and flatten it.
**Answer:**
```python
from pyspark.sql import functions as F
df = spark.read.json(path)
flat = (df.select("order_id", F.explode("items").alias("item"), "customer.id")
          .select("order_id", "id", "item.sku", "item.qty"))
```
`explode_outer` keeps rows whose array is empty or null.

---

### Q115. Join types in Spark, and how do you find records in A but not in B?
**Answer:** inner, left, right, full, left_semi (filter A by existence in B), left_anti (rows of A with no match in B).
```python
missing = a.join(b, "id", "left_anti")
```

---

### Q116. `groupByKey` vs `reduceByKey` (RDD)? What is the DataFrame equivalent?
**Answer:** `reduceByKey` combines values locally on each partition before the shuffle (map-side combine); `groupByKey` shuffles all values and can cause OOM. DataFrame `groupBy().agg()` already does partial aggregation, so prefer it.

---

### Q117. How do you decide the number of partitions?
**Answer:** Target 128–256 MB per partition. For input of 100 GB that is about 400–800 partitions. Also consider total cores (2–4 tasks per core). Too few means OOM and underused cores; too many means scheduling overhead.

---

### Q118. Scenario: Spark job fails with OutOfMemoryError. How do you troubleshoot?
**Answer:**
1. Identify whether it is the driver (e.g., `collect()`, `toPandas()` on big data) or an executor (skew, huge partitions, wide aggregation).
2. Check the Spark UI for stages with skewed task times or big spill.
3. Increase partitions (`spark.sql.shuffle.partitions`), fix skew, avoid `collect`.
4. Tune `executor.memory`, `memoryOverhead`, and `driver.memory`.
5. Filter and select columns early, broadcast only truly small tables.

---

### Q119. What do you check in the Spark UI?
**Answer:** Jobs and stages timeline, task duration distribution (skew), shuffle read/write sizes, spill to disk, GC time, the SQL tab for the physical plan (broadcast or sort-merge join), and the storage tab for cached data.

---

### Q120. What are Catalyst and Tungsten?
**Answer:** Catalyst is the query optimizer (logical plan, optimization rules like predicate pushdown and column pruning, physical plan). Tungsten optimizes execution with off-heap memory management, cache-aware computation, and whole-stage code generation.

---

### Q121. What is Adaptive Query Execution (AQE)?
**Answer:** Spark 3+ re-optimizes the plan at runtime using actual shuffle statistics: it coalesces small shuffle partitions, switches join strategies (sort-merge to broadcast), and splits skewed partitions. Enable with `spark.sql.adaptive.enabled=true`.

---

### Q122. Write modes and partitioned writes: how do you safely overwrite only specific partitions?
**Answer:** Modes: `append`, `overwrite`, `ignore`, `errorifexists`. To overwrite only the partitions present in the incoming data:
```python
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
df.write.mode("overwrite").partitionBy("dt").parquet(path)
```
Plain `overwrite` in static mode would wipe the whole table.

---

### Q123. Delta Lake: what does it add, and how do you upsert?
**Answer:** ACID transactions, schema enforcement/evolution, time travel, and `MERGE`. Upsert:
```python
from delta.tables import DeltaTable
tgt = DeltaTable.forPath(spark, path)
(tgt.alias("t").merge(src.alias("s"), "t.id = s.id")
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute())
```
Also `OPTIMIZE`, `VACUUM`, and `DESCRIBE HISTORY`.

---

### Q124. Structured Streaming: explain checkpoints, triggers, and watermarks.
**Answer:** Checkpoints store offsets and state, so the job restarts exactly where it stopped. Triggers control batch frequency. Watermarks define how late data may arrive; state older than the watermark is dropped.
```python
(stream.withWatermark("event_ts", "10 minutes")
       .groupBy(F.window("event_ts", "5 minutes"), "user")
       .count()
       .writeStream.option("checkpointLocation", "/chk/x")
       .outputMode("update").start())
```

---

### Q125. How do you deduplicate in streaming?
**Answer:** `dropDuplicates(["event_id"])` combined with a watermark so state doesn't grow unbounded; or `foreachBatch` with a `MERGE` into the sink on the event key.

---

### Q126. How do you avoid producing small files in Spark writes?
**Answer:** `coalesce`/`repartition` by partition columns before writing, set `maxRecordsPerFile`, use Delta `OPTIMIZE`/auto-compaction, and schedule periodic compaction jobs.

---

### Q127. Scenario: size executors for a 500 GB job on a cluster.
**Answer:** Common starting point: 4–5 cores per executor, memory around 4–8 GB per core including overhead, leave resources for the OS/driver, and set `spark.sql.shuffle.partitions` based on the data volume. Then iterate using the Spark UI (spill, GC, task time) rather than guessing. Consider dynamic allocation.

---

### Q128. Broadcast variables vs accumulators?
**Answer:** Broadcast variables ship a read-only value (e.g., a lookup dict) once per executor. Accumulators are write-only-from-tasks counters read by the driver (e.g., counting bad records). Accumulator updates inside transformations may be double-counted on retries, so use them in actions or only for diagnostics.

---

### Q129. Faster distinct counts on huge data?
**Answer:** `F.approx_count_distinct("user_id", 0.02)` uses HyperLogLog, trading ~2% error for a big speedup and less memory; exact `countDistinct` needs a heavy shuffle.

---

### Q130. Spark SQL: temp views and reading query plans?
**Answer:**
```python
df.createOrReplaceTempView("orders")
spark.sql("select region, sum(amt) from orders group by region").explain(True)
```
`explain` shows parsed, analyzed, optimized, and physical plans, and is the starting point for performance tuning.

---

# SECTION 6: PYTHON + DATABASES / SQL

### Q131. How do you manage database connections in Python?
**Answer:** Use SQLAlchemy engines with connection pooling (`pool_size`, `max_overflow`, `pool_pre_ping=True`), context managers to ensure connections close, and one engine per process (not per query).

---

### Q132. Scenario: inserting 5 million rows row-by-row takes hours. How do you speed it up?
**Answer:** Batch with `executemany`/`execute_values`, or better, use the database's bulk loader: PostgreSQL `COPY`, Snowflake `COPY INTO` from staged files, Redshift `COPY` from S3, SQL Server `BULK INSERT`. Commit per batch, not per row, and disable or defer indexes during massive loads.
```python
from psycopg2.extras import execute_values
execute_values(cur, "insert into t (a,b) values %s", rows, page_size=10_000)
```

---

### Q133. How do you prevent SQL injection?
**Answer:** Use parameterized queries and never concatenate user input.
```python
cur.execute("select * from users where id = %s", (user_id,))   # safe
cur.execute(f"select * from users where id = {user_id}")        # unsafe
```
Identifiers (table names) can't be parameterized, so validate them against an allow-list.

---

### Q134. How do you do an upsert in PostgreSQL from Python?
**Answer:**
```sql
INSERT INTO customers (id, name, updated_at)
VALUES (%s, %s, %s)
ON CONFLICT (id) DO UPDATE
SET name = EXCLUDED.name, updated_at = EXCLUDED.updated_at;
```
For warehouses, `MERGE INTO` serves the same purpose.

---

### Q135. Scenario: read a 100M-row table without running out of memory.
**Answer:** Use a server-side (streaming) cursor and fetch in batches.
```python
with conn.cursor(name="stream") as cur:        # psycopg2 server-side cursor
    cur.itersize = 50_000
    cur.execute("select * from big_table")
    for row in cur:
        ...
```
Or `pd.read_sql(..., chunksize=...)`, or partition the extraction by key ranges and run in parallel.

---

### Q136. Explain transactions, commit, and rollback in pipelines.
**Answer:** A transaction groups statements atomically (all or nothing). Wrap load steps so a failure triggers `rollback()` and doesn't leave half-loaded tables. A common pattern is load to a staging table, validate, then swap or merge into the target within one transaction.

---

### Q137. When do you transform in SQL (ELT) vs in Python (ETL)?
**Answer:** Push heavy set-based transformations (joins, aggregations) into the warehouse/SQL, which is optimized and scales; use Python for orchestration, API ingestion, complex parsing, ML, or logic awkward in SQL. Modern stacks lean toward ELT (load raw first, transform with SQL/dbt).

---

### Q138. What is CDC (Change Data Capture)?
**Answer:** Capturing inserts, updates, and deletes from a source and applying them downstream. Approaches: log-based (Debezium reading binlog/WAL into Kafka; low impact, captures deletes), timestamp/version column queries, triggers, or snapshot diffs. Log-based is the most reliable for deletes.

---

### Q139. Full load vs incremental load?
**Answer:** Full load reloads everything (simple, expensive, fine for small dimensions). Incremental loads only new or changed rows using a watermark column or CDC (efficient but must handle late data, deletes, and failures). Typical practice is incremental daily plus periodic full reconciliation.

---

### Q140. Scenario: the source team added a new column and your job broke. How do you make it resilient?
**Answer:** Select explicit columns rather than `SELECT *` or positional loads, validate the schema at ingestion and alert on drift, enable additive schema evolution for raw/bronze layers (e.g., `mergeSchema`), keep a schema registry or contract with the producer, and quarantine files with incompatible schemas instead of failing the whole pipeline.

---

# SECTION 7: AIRFLOW & ORCHESTRATION

### Q141. What are DAG, task, operator, and sensor in Airflow?
**Answer:** A DAG is a directed acyclic graph of tasks with dependencies and a schedule. A task is an instance of an operator (PythonOperator, BashOperator, etc.). A sensor is an operator that waits for a condition (file arrives, partition exists).

---

### Q142. What makes an Airflow task idempotent? What are `catchup` and backfill?
**Answer:** Use the logical date (`{{ ds }}`) to derive input and output (write to a date partition and overwrite) rather than "now". `catchup=True` makes the scheduler create runs for missed intervals since `start_date`; backfill reruns historical intervals. Idempotent tasks make both safe.

---

### Q143. What is XCom and what are its limits?
**Answer:** Mechanism for small values passed between tasks via the metadata DB. Don't pass DataFrames or large payloads; write them to S3/GCS/a table and pass the path or key.

---

### Q144. How do you handle retries, SLAs, and alerting?
**Answer:**
```python
default_args = {
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "on_failure_callback": notify_slack,
}
```
Use SLA/timeout settings, failure callbacks, and email/Slack/PagerDuty alerts. Fail loudly on data quality issues rather than retrying them endlessly.

---

### Q145. When do you use sensors, and what is `mode="reschedule"`?
**Answer:** Sensors wait for external events. In `poke` mode they hold a worker slot; in `reschedule` mode they release the slot between checks, which is better for long waits. Deferrable operators/triggers are even more efficient.

---

### Q146. What is the TaskFlow API?
**Answer:** Decorator-based DAG authoring (`@dag`, `@task`) where return values pass between tasks automatically via XCom, giving cleaner Python-native code.
```python
@dag(schedule="@daily", start_date=datetime(2026, 1, 1), catchup=False)
def sales():
    @task
    def extract(): return "s3://bucket/raw/x.csv"
    @task
    def load(path): ...
    load(extract())
sales()
```

---

### Q147. Scenario: process an unknown number of files each day. How do you build tasks dynamically?
**Answer:** Use dynamic task mapping: `load.expand(path=list_files())` creates one mapped task instance per file at runtime, with independent retries and parallelism.

---

### Q148. Explain Airflow executors.
**Answer:** `SequentialExecutor` (testing only), `LocalExecutor` (single machine), `CeleryExecutor` (distributed workers), `KubernetesExecutor` (a pod per task, good isolation and scaling). Choose based on scale and infrastructure.

---

### Q149. How do you manage credentials in Airflow?
**Answer:** Use Connections and Variables (stored encrypted) or a secrets backend (AWS Secrets Manager, Vault). DAG code references connection IDs, never raw passwords.

---

### Q150. Write a simple Airflow DAG that extracts, transforms, and loads daily data with date-based partitions.
**Answer:**
```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

def extract(ds, **_):   print(f"extract data for {ds}")
def transform(ds, **_): print(f"transform {ds}")
def load(ds, **_):      print(f"overwrite partition dt={ds}")

with DAG("daily_sales", start_date=datetime(2026, 1, 1), schedule="@daily",
         catchup=False, default_args={"retries": 2, "retry_delay": timedelta(minutes=5)}) as dag:
    e = PythonOperator(task_id="extract", python_callable=extract)
    t = PythonOperator(task_id="transform", python_callable=transform)
    l = PythonOperator(task_id="load", python_callable=load)
    e >> t >> l
```

---

# SECTION 8: APIs, STREAMING, CLOUD & DATA QUALITY

### Q151. Scenario: ingest data from a paginated REST API. How do you do it reliably?
**Answer:**
```python
import requests

def fetch_all(url, token):
    s = requests.Session()
    s.headers["Authorization"] = f"Bearer {token}"
    page = 1
    while True:
        r = s.get(url, params={"page": page, "per_page": 500}, timeout=30)
        r.raise_for_status()
        data = r.json()
        if not data["results"]:
            break
        yield from data["results"]
        page += 1
```
Always set timeouts, reuse a Session, handle pagination types (page, offset, cursor/next-link), and persist raw responses before transforming.

---

### Q152. How do you handle API rate limits and transient failures?
**Answer:** Respect `429` and the `Retry-After` header, use exponential backoff with jitter, retry only on retryable codes (429, 5xx, timeouts), cap the retries, and throttle concurrency. With `requests`, mount an `HTTPAdapter` with `urllib3.util.Retry`.

---

### Q153. API tokens expire mid-run. What do you do?
**Answer:** Wrap calls so a `401` triggers a token refresh and a single retry; cache the token with its expiry time and refresh proactively before it expires.

---

### Q154. Polling vs webhooks?
**Answer:** Polling: you ask on a schedule (simple, delayed, wasteful). Webhooks: the source pushes events (near real-time, but you need a reliable endpoint, queueing, and idempotent handling). Often a webhook lands events on a queue, and a batch job reconciles by polling as a safety net.

---

### Q155. Scenario: write a Kafka consumer in Python. How do you avoid losing data?
**Answer:**
```python
from confluent_kafka import Consumer
c = Consumer({"bootstrap.servers": "b:9092", "group.id": "etl",
              "enable.auto.commit": False, "auto.offset.reset": "earliest"})
c.subscribe(["orders"])
while True:
    msg = c.poll(1.0)
    if msg is None or msg.error(): continue
    process(msg.value())
    c.commit(msg)          # commit after successful processing
```
Committing after processing gives at-least-once delivery, so duplicates are possible and the sink must be idempotent.

---

### Q156. At-most-once vs at-least-once vs exactly-once?
**Answer:** At-most-once: commit before processing, so data can be lost. At-least-once: commit after processing, so duplicates are possible. Exactly-once: achieved through transactional/idempotent writes (Kafka transactions, or at-least-once plus dedupe/upsert on a unique key at the sink).

---

### Q157. What is a dead letter queue (DLQ)?
**Answer:** A place (topic/table/bucket) where records that repeatedly fail processing are sent along with the error details, so the main pipeline keeps flowing and bad data can be inspected and replayed later.

---

### Q158. List more than 1000 objects in S3 with boto3.
**Answer:** `list_objects_v2` returns at most 1000 keys per call, so use a paginator.
```python
paginator = s3.get_paginator("list_objects_v2")
for page in paginator.paginate(Bucket="b", Prefix="raw/2026/10/"):
    for obj in page.get("Contents", []):
        print(obj["Key"])
```

---

### Q159. Scenario: process files automatically when they land in S3.
**Answer:** S3 event notification → Lambda (small/quick work) or SQS → worker, or trigger a Glue/Airflow/Step Functions run. Design for duplicates (events can repeat), validate the file name and schema, and move processed files to a `processed/` prefix or record them in a control table.

---

### Q160. What is AWS Glue and when would you use it over EMR/Databricks?
**Answer:** Glue is serverless Spark ETL with a Data Catalog and crawlers: low operations overhead, good for moderate ETL on AWS. EMR/Databricks give more control over cluster tuning and libraries and are often better for heavy or long-running workloads and advanced Delta usage.

---

### Q161. How do you handle secrets in cloud pipelines?
**Answer:** IAM roles/managed identities for service access, Secrets Manager/Key Vault for credentials, rotate regularly, least privilege, and never store credentials in code, notebooks, logs, or Git history.

---

### Q162. What does good pipeline logging/monitoring look like?
**Answer:** Structured logs with run id, batch id, source, row counts in/out, duration, and error details; metrics (freshness, volume, failure rate, latency); dashboards and alerts on SLA breach, volume anomalies, and schema drift; and an audit table of runs.

---

### Q163. How do you implement data quality in production?
**Answer:** Run automated checks at ingestion and before publishing: schema, null rates, uniqueness, referential integrity, ranges, freshness, and volume versus a baseline. Tools: Great Expectations, Soda, dbt tests, Deequ. Decide per rule whether to warn, quarantine bad rows, or block the load.

---

### Q164. How do you structure code so transformations are testable?
**Answer:** Keep transformation logic in pure functions (DataFrame in, DataFrame out) separate from I/O. Test with small in-memory DataFrames (a local SparkSession fixture for PySpark), cover edge cases (nulls, duplicates, empty input), and run tests in CI.

---

### Q165. How do you test code that calls an external API?
**Answer:** Mock the HTTP layer (`unittest.mock`, `responses`, `requests-mock`), use recorded fixtures for typical, empty, error, and paginated responses, and keep a small separate integration test against a sandbox.

---

### Q166. What does CI/CD for a data pipeline look like?
**Answer:** On every pull request: lint (`ruff`/`flake8`), type check (`mypy`), unit tests, and DAG import checks. On merge: build a Docker image, deploy to staging, run smoke or integration tests with sample data, then promote to production with versioned artifacts and the ability to roll back.

---

### Q167. Why containerize pipelines with Docker?
**Answer:** Reproducible environments, identical dev/test/prod behavior, dependency isolation, and easy deployment to Kubernetes, ECS, or Airflow's `KubernetesPodOperator`. Keep images small, pin dependency versions, and run as a non-root user.

---

### Q168. How do you detect that a pipeline is silently producing wrong data?
**Answer:** Row-count and sum reconciliation versus source, freshness checks, anomaly detection on daily volumes and null rates, distribution checks on key metrics, and downstream consumer-facing tests. A green job doesn't prove correct data, so monitor the data, not just the job status.

---

### Q169. How do you handle PII?
**Answer:** Minimize collection, classify sensitive columns, mask or hash (salted SHA-256) or tokenize where possible, encrypt at rest and in transit, restrict access (RBAC, column-level security), keep PII out of logs, and follow retention and deletion rules (GDPR/DPDP).
```python
import hashlib
hashed = hashlib.sha256((salt + email).encode()).hexdigest()
```

---

### Q170. What audit/metadata columns do you add to tables and why?
**Answer:** `ingested_at`, `source_system`, `batch_id`/`run_id`, `source_file`, `record_hash`, and `is_deleted`. They support lineage, debugging, reprocessing, dedupe, and change detection.

---

# SECTION 9: PERFORMANCE & CONCURRENCY

### Q171. Scenario: call 10,000 API endpoints quickly. How?
**Answer:** The work is I/O-bound, so use threads (or asyncio) with a bounded pool and rate limiting.
```python
from concurrent.futures import ThreadPoolExecutor, as_completed
def get(u): return requests.get(u, timeout=20).json()
with ThreadPoolExecutor(max_workers=20) as ex:
    futures = {ex.submit(get, u): u for u in urls}
    for f in as_completed(futures):
        try: handle(f.result())
        except Exception as e: log.error("%s failed: %s", futures[f], e)
```

---

### Q172. Scenario: convert and compress 5,000 large files. What approach?
**Answer:** CPU-bound, so use `multiprocessing` / `ProcessPoolExecutor` (one worker per core) to bypass the GIL. Beware of memory (each process has its own copy), pass file paths instead of big objects, and consider Spark if the volume keeps growing.

---

### Q173. How does asyncio work, and when do you use it?
**Answer:** A single-threaded event loop switches between coroutines at `await` points. Best for thousands of concurrent network calls.
```python
import asyncio, aiohttp
async def fetch(s, u):
    async with s.get(u) as r: return await r.json()
async def main(urls):
    async with aiohttp.ClientSession() as s:
        return await asyncio.gather(*(fetch(s, u) for u in urls))
```
Use a semaphore to cap concurrency; don't call blocking code inside coroutines.

---

### Q174. How do you profile slow Python code?
**Answer:** `cProfile` / `snakeviz` for function-level time, `line_profiler` for line-level, `time.perf_counter`, and `tracemalloc` for memory. Find the real bottleneck first (often I/O or a join), not the code you assume is slow.

---

### Q175. Dask vs Spark?
**Answer:** Dask scales the pandas/NumPy API from one machine to a small cluster with minimal code changes and is Python-native. Spark is a mature, JVM-based, heavily optimized engine for large clusters, SQL, streaming, and the lakehouse ecosystem. Pick Dask for pandas-style workloads and Spark for enterprise-scale ETL.

---

### Q176. Why are people adopting Polars and DuckDB?
**Answer:** Polars: Rust-based, multi-threaded, lazy evaluation with query optimization, far faster and leaner than pandas on a single machine. DuckDB: embedded analytical SQL engine that queries Parquet/CSV directly and handles larger-than-memory data. Both are great for single-node jobs that don't justify Spark.
```python
import duckdb
duckdb.sql("select region, sum(amt) from 'data/*.parquet' group by 1").df()
```

---

### Q177. Your long-running Python job's memory keeps growing. How do you debug it?
**Answer:** Suspects: unbounded caches or lists, global references, unclosed connections or files, and accumulating DataFrames in a loop. Use `tracemalloc` snapshots or `objgraph` to find growing objects, process data in batches, `del` large objects, and delegate to short-lived subprocesses if needed.

---

### Q178. How do you tune batch size?
**Answer:** Too small means high overhead per call; too large means memory pressure and long retry units. Test a few sizes (e.g., 1k, 10k, 100k) measuring throughput, memory, and failure recovery cost, then pick the knee of the curve and make it configurable.

---

### Q179. How do generator pipelines help?
**Answer:** Chaining generators (read → parse → filter → batch → write) gives streaming processing with constant memory and no intermediate lists.
```python
rows    = read_rows(path)
parsed  = (parse(r) for r in rows)
valid   = (r for r in parsed if r["amount"] > 0)
for batch in batches(valid, 10_000):
    write(batch)
```

---

### Q180. When do you NOT use Spark?
**Answer:** When data fits comfortably on one machine (a few GB), the job is tiny or latency-sensitive, or the overhead of cluster startup and a distributed system outweighs the benefit. A well-written pandas, Polars, or DuckDB job is simpler, cheaper, and often faster.

---

# SECTION 10: CODING ROUNDS & PRODUCTION SCENARIOS

### Q181. Find the first non-repeating character in a string.
**Answer:**
```python
from collections import Counter
def first_unique(s):
    c = Counter(s)
    return next((ch for ch in s if c[ch] == 1), None)
```
O(n) time.

---

### Q182. Two-sum: return indices of two numbers adding up to a target.
**Answer:**
```python
def two_sum(nums, target):
    seen = {}
    for i, n in enumerate(nums):
        if target - n in seen:
            return [seen[target - n], i]
        seen[n] = i
```
O(n) using a hash map.

---

### Q183. Word frequency from a large text file (top 10 words).
**Answer:**
```python
from collections import Counter
import re
c = Counter()
with open("big.txt") as f:
    for line in f:
        c.update(re.findall(r"\w+", line.lower()))
print(c.most_common(10))
```
Streams the file, so memory is bounded by the vocabulary size.

---

### Q184. Merge overlapping intervals (e.g., session or maintenance windows).
**Answer:**
```python
def merge(intervals):
    intervals.sort(key=lambda x: x[0])
    out = [intervals[0]]
    for s, e in intervals[1:]:
        if s <= out[-1][1]:
            out[-1][1] = max(out[-1][1], e)
        else:
            out.append([s, e])
    return out
```

---

### Q185. Flatten a nested dictionary (e.g., JSON from an API).
**Answer:**
```python
def flatten(d, parent="", sep="_"):
    out = {}
    for k, v in d.items():
        key = f"{parent}{sep}{k}" if parent else k
        if isinstance(v, dict):
            out.update(flatten(v, key, sep))
        else:
            out[key] = v
    return out
```

---

### Q186. Check whether a string is a palindrome (ignoring case and non-alphanumerics).
**Answer:**
```python
def is_pal(s):
    t = [c.lower() for c in s if c.isalnum()]
    return t == t[::-1]
```

---

### Q187. Top K frequent elements.
**Answer:**
```python
from collections import Counter
import heapq
def top_k(nums, k):
    return [x for x, _ in Counter(nums).most_common(k)]
# or heapq.nlargest(k, c, key=c.get)
```

---

### Q188. Group anagrams.
**Answer:**
```python
from collections import defaultdict
def group(words):
    g = defaultdict(list)
    for w in words:
        g["".join(sorted(w))].append(w)
    return list(g.values())
```

---

### Q189. Given a log file, count requests per status code and the slowest 5 endpoints.
**Answer:** Stream lines, parse with a compiled regex or `split`, and keep a `Counter` for statuses plus a `defaultdict(list)` (or running max/heap) for latencies per endpoint.
```python
import heapq
slow = []
for ep, ms in parse(path):
    heapq.heappush(slow, (ms, ep))
    if len(slow) > 5: heapq.heappop(slow)
```
A min-heap of size 5 keeps memory constant.

---

### Q190. Mask email addresses in a dataset (e.g., `john.doe@x.com` → `j*****e@x.com`).
**Answer:**
```python
def mask(email):
    user, dom = email.split("@")
    if len(user) <= 2: return "*" * len(user) + "@" + dom
    return user[0] + "*" * (len(user) - 2) + user[-1] + "@" + dom
```
In pandas: `df["email"].map(mask)`; in Spark, use `regexp_replace` for speed.

---

### Q191. Compare two large CSV files and report the differences.
**Answer:** Load both with explicit dtypes (in chunks or Spark if large), outer-merge on the key with `indicator=True` for missing/extra rows, then compare the remaining columns for the matched keys. Alternatively hash each row (`record_hash`) and compare the hashes for a fast first pass.

---

### Q192. Scenario: the job shows SUCCESS but yesterday's data is missing downstream. How do you investigate?
**Answer:**
1. Check row counts at each stage (source, staging, target) for that date.
2. Look for a wrong filter or watermark (timezone mismatch: UTC vs IST can shift a day's data).
3. Check whether an upstream file or table arrived late, so the job ran on empty input.
4. Check for overwrite or partition mistakes and silent exception handling.
5. Fix: reload the date (idempotent rerun), add a row-count and freshness check that fails on zero rows, and add an alert.

---

### Q193. Scenario: late-arriving data. How do you design for it?
**Answer:** Partition by event date but process by ingestion time, reprocess a rolling window (e.g., last 3 days) with idempotent upserts, use watermarks in streaming, keep `ingested_at` for audit, and communicate the data completeness SLA to consumers.

---

### Q194. Scenario: after a retry, duplicate records appear in the target. Why and how do you fix it?
**Answer:** The first attempt partially wrote data and the retry appended again (non-idempotent load). Fixes: write to a staging table and `MERGE`/upsert on a business key, overwrite the partition instead of appending, use a batch id to delete-then-insert, or dedupe in the consumption layer. Also make the write atomic.

---

### Q195. Scenario: a daily job that took 30 minutes now takes 6 hours. How do you approach it?
**Answer:**
1. Compare with a past run: did the input volume change or is it a new skew?
2. Check the Spark UI or query plan for a changed join strategy, spill, skew, or small files.
3. Look at the source (slow query, locks, throttled API) and the sink (index/locking).
4. Check the cluster (fewer nodes, noisy neighbours) and any code or config changes.
5. Apply targeted fixes (broadcast, repartition, partition pruning, caching, incremental logic) and add runtime alerts so regressions are caught early.

---

### Q196. Scenario: how do you backfill one year of data safely?
**Answer:** Make the job parameterized by date and idempotent, run in date chunks (e.g., weekly) with limited parallelism so as not to overload sources or the cluster, validate each chunk against source counts, keep production schedules unaffected (separate backfill DAG/runs or Airflow backfill), and monitor cost and throttling.

---

### Q197. Scenario: design a daily pipeline from a third-party REST API to a data warehouse.
**Answer:**
1. **Extract**: Airflow task calls the API with incremental parameters (`updated_since`), handles pagination, retries, and rate limits.
2. **Land raw**: write the untouched JSON to S3 partitioned by `dt` (replayable, auditable).
3. **Transform**: Spark or Python flattens and cleans into Parquet; validates schema and quality.
4. **Load**: staging table then `MERGE` into the warehouse on the business key (idempotent).
5. **Operate**: logging, row-count reconciliation, alerts, and a control table with watermarks and run status.

---

### Q198. Scenario: how do you handle corrupt or malformed records in an ingestion job?
**Answer:** Don't fail the whole batch for a few bad records and don't silently drop them. Parse in permissive mode, route bad rows plus the error reason to a quarantine location (or `_corrupt_record` in Spark), set an error-rate threshold above which the job fails, and alert the data owner.
```python
df = spark.read.option("mode", "PERMISSIVE") \
     .option("columnNameOfCorruptRecord", "_corrupt").schema(schema).json(path)
```

---

### Q199. How do you test a pipeline before releasing it to production?
**Answer:** Unit tests for transformations, integration tests on a sample dataset in a staging environment, data-quality assertions, a comparison of outputs against the old pipeline (parallel run) for migrations, performance testing with production-like volume, a rollback plan, and a monitored first production run.

---

### Q200. "Tell me about a pipeline you built." What do interviewers look for, and how do you answer?
**Answer:** Use a structured story:
- **Context**: business problem, data sources, volume (e.g., 50M rows/day), SLA.
- **Design**: architecture, tools (Python, Spark, Airflow, S3, warehouse), and why you chose them.
- **Your contribution**: specific things you owned (ingestion framework, incremental logic, data quality checks).
- **Challenges**: a real problem (skew, late data, schema drift, duplicates) and how you solved it with measurable results (runtime cut from 3h to 40 min, cost reduced 30%).
- **Reliability**: monitoring, retries, idempotency, alerting.
- **Learning**: what you'd improve next.

Be ready to defend each decision and answer follow-ups about trade-offs.

---


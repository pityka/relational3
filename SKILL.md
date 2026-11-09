---
name: using-ra3
description: >
  How to use the pityka `ra3` library (io.github.pityka/ra3-core) as a client:
  a type-safe relational query engine for Scala 3 over large chunked columnar
  datasets. Covers importing CSV/Parquet/streams, naming and typing columns,
  building row-level and table-level query expressions, filters, projections,
  group-by reductions, equi-joins, full/partial reductions, and evaluating a
  query to a `Table` or an `fs2.Stream` on a `tasks` runtime. Load this skill
  ALWAYS when writing or reviewing Scala that imports `ra3.*`, calls
  `ra3.importCsv` / `ra3.importFromStream` / `ra3.query` / `ra3.reduce`, builds
  a `TableExpr`, or uses `.groupBy` / `.inner` / `.outer` / `.schema` /
  `.evaluate` from ra3.
---

# Using the `ra3` library

RA3 runs **type-safe relational queries over large tabular data** held in a
chunked columnar format. A query is a value you build, print, reuse, and
evaluate. Evaluation runs on a `tasks` distributed runtime, so every query
needs an implicit task system in scope.

Dependency: `io.github.pityka::ra3-core`. Scala 3 only. Named tuples carry the
column names and types through the whole query at compile time; on Scala 3.9+
named tuples are stable and need no language flag.

Read the `using-tasks` skill as well. RA3 evaluation is a `tasks` workflow, so
caching, the implicit `TaskSystemComponents`, and `SharedFile` behave as that
skill describes.

---

## 1. The 30-second mental model

```
import file  ──►  TableExpr / Table        (names + types known at compile time)
      │
      ▼
.schema { t => ... }  or  for { t <- ... }  gives a reference to the columns
      │
build row expressions on columns:  t.value * 2, t.key === 0, t.a.sum
      │
wrap in a table operation:  ra3.query(...), .groupBy.reduceTotal(...), .inner(...)
      │
      ▼
TableExpr[Result]   ──►  .render     prints the plan
                    ──►  .evaluate   IO[Table]
                    ──►  .evaluateToStream   IO[fs2.Stream[IO, NamedTuple]]
```

Three layers exist:

- **Columns** carry a name and one of five types: `I32` (Int), `I64` (Long),
  `F64` (Double), `Str` (String), `Inst` (Instant).
- **Row expressions** act element-wise on columns: comparisons, arithmetic,
  string and instant operations, and aggregations such as `sum` or `first`.
- **Table expressions** (`TableExpr[T]`) hold a whole query plan. You compose
  them, render them, and evaluate them.

---

## 2. Standing up the runtime

Every evaluation needs an implicit task system. `defaultTaskSystem` returns a
cats-effect `Resource`:

```scala
import ra3.*
import tasks.*
import cats.effect.*
import com.typesafe.config.ConfigFactory

val config = ConfigFactory.parseString(
  """tasks.fileservice.storageURI=./storage/
     tasks.disableRemoting = true
  """
)

defaultTaskSystem(Some(config)).map(_._1).use { implicit tsc =>
  // build and evaluate queries here
  query.evaluate
}
```

`tsc` is an implicit `TaskSystemComponents`. The import functions need it; so do
`evaluate` and `evaluateToStream`.

A file on disk enters the runtime as a `SharedFile`:

```scala
SharedFile(uri = tasks.util.Uri(s"file://${new java.io.File(path).getAbsolutePath}"))
```

---

## 3. Importing data

### CSV with named, typed columns

`ra3.importCsv` reads a named tuple of column definitions and returns a
`TableExpr` whose field names and types match that tuple:

```scala
val transactions =
  ra3.importCsv(
    file = fileHandle,
    name = fileHandle.name,
    columns = (
      customerIn  = ra3.CSVColumnDefinition.StrColumn(0),
      customerOut = ra3.CSVColumnDefinition.StrColumn(1),
      category    = ra3.CSVColumnDefinition.StrColumn(2),
      categoryId  = ra3.CSVColumnDefinition.I32Column(3),
      value       = ra3.CSVColumnDefinition.F64Column(4),
      time        = ra3.CSVColumnDefinition.InstantColumn(5)
    ),
    maxSegmentLength = 1_000_000,
    recordSeparator = "\n",
    fieldSeparator = '\t',
    compression = Some(ra3.CompressionFormat.Gzip)
  )
```

Each `CSVColumnDefinition` holds the 0-based column index in the file. The tuple
field names (`customerIn`, `value`, …) become the column names. `maxSegmentLength`
sets the chunk size; each column splits into segments of that length.

Column definitions: `StrColumn`, `I32Column`, `I64Column`, `F64Column`,
`InstantColumn`. `StrColumn` takes an optional missing-value string;
`InstantColumn` takes an optional parser.

### Other sources

- `ra3.importFromStream(stream, uniqueId, minimumSegmentSize, maximumSegmentSize)`
  imports an `fs2.Stream[IO, NamedTuple]`. It returns `IO[Table]`.
- `ra3.importParquet(file, name, maxSegmentLength)` returns `IO[Table]`.
- `ra3.importCsvUntyped(file, name, columns, maxSegmentLength, ...)` takes a
  `Seq[CSVColumnDefinition]` and returns a `Table` without compile-time names.

---

## 4. Naming and typing an existing `Table`

An imported `Table` (from a stream or Parquet) carries data but not always
compile-time names. Two methods attach them:

- `.rename[("a", "b", ...)]` sets the column names on a positional table.
- `.as[("c0", "c1"), (I32Var, I32Var)]` sets both names and the column types.

The type tags are `I32Var`, `I64Var`, `F64Var`, `StrVar`, `InstVar`.

```scala
val table = imported.rename[("key", "value")]

val typed =
  imported.as[("c0", "c1", "c2"), (I32Var, I32Var, I32Var)]
```

---

## 5. Getting column references: `.schema` and `for`

A query reads columns through a reference bound inside a scope. Two spellings
exist; they build the same plan.

**`.schema`** takes the column reference and returns a `TableExpr`:

```scala
table.schema { t =>
  ra3.query(
    t.key.as("key") :*
      (t.value > 0.0).ifelse(t.value, 0.0).as("clamped")
  )
}
```

**`for`-comprehension**: a `TableExpr` is monadic. Each `<-` binds a reference to
an evaluated intermediate table. `.in` binds the result of a step and gives a
local reference:

```scala
for {
  t <- transactions
  avg <- t.customerOut.groupBy
           .withPartitionBase(16)
           .reduceTotal(
             t.customerOut.first.as("customer") :*
               (t.value.sum / t.value.count).as("avg")
           )
           .in(_.tap("grouped by out"))
} yield avg
```

`.tap("label")` prints a sample of an intermediate table and passes it on.
Nest `.schema` blocks to bring two tables into scope at once (see joins below).

---

## 6. Row expressions

Row expressions act element-wise on columns. Build a projection by naming each
result column with `.as("name")` and joining them with `:*`.

```scala
ra3.select0
  :* t.a.as("a")
  :* (t.a * t.b).as("product")
  :* t.a.concatenate(t.b).as("ab")
```

- `ra3.select0` (also `ra3.S`) starts an empty projection. `.extend(col.as(...))`
  adds one column.
- `ra3.all(t)` selects every column of `t`. `ra3.allWithPrefix(t, "l_")` prefixes
  the names.

**Comparisons** return a boolean-like column: `===`, `>`, `>=`, `<`, `<=`.
Combine them with `&&` and `||`. `col.containedIn(Set(...))` tests set
membership. Comparisons with a missing value yield false.

**Arithmetic** on numeric columns: `+`, `-`, `*`, `/`.

**Conditionals**: `predicate.ifelse(whenTrue, whenFalse)`.

**Missing values**: `col.isMissing` selects the missing rows. Each type encodes
missing as a sentinel (`Int.MinValue`, `Long.MinValue`, `Double.NaN`, and the
string `ra3.MissingString`).

**Conversions and string/instant operations**: `col.toDouble`,
`strCol.concatenate(other)`, and the instant extraction operations.

**Aggregations** (used inside group-by and reduce projections): `.first`,
`.sum`, `.count`, `.min`, `.max`.

---

## 7. Filter and project: `query`, `select`, `count`

`ra3.query(projection)` (alias `ra3.select`) builds a row-wise filter-and-project
query over the one table the projection refers to. Attach a filter with
`.where(predicate)`:

```scala
table.schema { t =>
  ra3.query(ra3.all(t).where(t.a > 5 && t.a <= 15))
}
```

`ra3.count(projection)` counts the rows that pass the filter instead of returning
them.

---

## 8. Group-by and reductions

Group on a column, then reduce each group to one row. The projection names the
group key and the aggregates.

```scala
t.customerIn.groupBy
  .withPartitionBase(16)
  .reduceTotal(
    t.customerIn.first.as("customer") :*
      (t.value.sum / t.value.count).as("avg") :*
      t.value.min.as("min") :*
      t.value.max.as("max")
  )
```

- `.withPartitionBase(n)` sets how many partitions the shuffle uses.
- `.reduceTotal(projection)` reduces each full group to one row.
- `.reducePartial(projection)` reduces each segment independently, so a group may
  arrive in several partial rows. It is cheaper. For an associative and
  distributive aggregate, follow a partial reduction with a total reduction that
  combines the partials:

```scala
t.customerIn.groupBy
  .reducePartial(
    t.customerIn.first.as("customer") :*
      t.value.sum.as("sum") :*
      t.value.count.as("count")
  )
  .schema { p =>
    p.customerIn.groupBy.reduceTotal(
      p.customer.first.as("customer") :*
        (p.sum.sum / p.count.sum).as("avg")
    )
  }
```

### Whole-table reductions

`ra3.reduce(projection)` reduces the whole table to a single row (one group). It
reads the needed columns into memory. `ra3.partialReduce(projection)` reduces
each segment to one row without a global combine.

---

## 9. Joins

Joins are always equi-joins, defined on two columns. Bring both tables into
scope, then call `.inner`, `.outer`, `.left`, or `.right` on the left key column
with the right key column as the argument. `.select(projection)` projects the
joined table.

```scala
left.schema { l =>
  right.schema { r =>
    l.id
      .inner(r.id)
      .withPartitionBase(3)
      .select(
        l.name.as("name") :*
          r.score.as("score")
      )
  }
}
```

An outer join leaves the unmatched key missing on one side. Coalesce the two
keys with `isMissing` and `ifelse`:

```scala
a.name
  .outer(b.name)
  .select(
    a.name.isMissing.ifelse(b.name, a.name).as("name") :*
      a.valueA.as("valueA") :*
      b.valueB.as("valueB")
  )
```

Filter a join with `.where` on the projection:

```scala
.select(
  (a.k.as("k") :* a.a.as("a") :* b.b.as("b")).where(a.a >= 20)
)
```

Joins never match on missing key values. Tuning knobs: `.withPartitionBase(n)`,
`.withPartitionLimit(n)`, `.withMaxSegmentsBufferingAtOnce(n)`.

A self-join brings the same table into scope twice through two nested `.schema`
blocks.

---

## 10. Concatenating tables

`ra3.concatenate(tables*)` appends the rows of several tables with matching
column names and types. It returns `IO[Table]`. On a built query, `.in(t =>
t.concat(t))` concatenates a table with itself inside the plan.

---

## 11. Evaluating and reading results

Build the plan, then run it.

- `query.render` returns a printable description of the plan.
- `query.evaluate` returns `IO[Table]`.
- `query.evaluateToStream` returns `IO[fs2.Stream[IO, NamedTuple]]`, where each
  element is a named tuple of the query's result type. Field access stays typed:
  `v.name`, `v.score`.
- `query.evaluateToStreamOfSingleColumn` streams a single-column result.

On a resulting `Table`:

- `table.showSample()` returns `IO[String]` with a sample of rows.
- `table.exportToCsv()` writes CSV files and returns their handles.
- `table.bufferStream` streams the segments in memory; `table.bufferSegment(i)`
  buffers one.

A full run:

```scala
defaultTaskSystem(Some(config)).map(_._1).use { implicit tsc =>
  for {
    fileHandle <- SharedFile(uri = tasks.util.Uri(s"file://$absPath"))
    query       = defineQuery(fileHandle)
    _          <- IO.println(query.render)
    result     <- query.evaluate
    sample     <- result.showSample()
    _          <- IO.println(sample)
    files      <- result.exportToCsv()
    rows       <- query.evaluateToStream.flatMap(_.take(10).compile.toList)
    _          <- IO.println(rows)
  } yield ()
}
```

Because evaluation runs on `tasks`, a second evaluation of the same query hits
the cache. Reason about cache keys with the `using-tasks` skill.

---

## 12. Missing-value semantics

- A comparison with a missing value returns false, as with NaN.
- Filter for missing rows with `col.isMissing`.
- Logic operators follow three-valued (Kleene) logic.
- A join never matches on a missing key.
- `group by` treats missing as its own group.

---

## 13. Gotchas

- Every import, `evaluate`, and `evaluateToStream` needs the implicit
  `TaskSystemComponents` in scope.
- A `name` passed to an import must be unique within a run.
- `ra3.query` / `reduce` / `count` require the projection to refer to exactly one
  table. Two tables in one projection fail at runtime; join first.
- Name every projected column with `.as`, and build the projection tuple with
  `:*`.
- Pick `maxSegmentLength` to the data size: too small makes many segments, too
  large raises memory per segment.

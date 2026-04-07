# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

RA3 is a type-safe relational query engine for Scala 3 that operates on large tabular datasets in chunked columnar format. It supports partitioned joins, groupings, projections, filters, top-K selection, and CSV I/O. Distributed execution is handled via the `tasks-core` library.

## Build & Test

```bash
sbt test                          # Run all tests (uses -Xmx4G via fork)
sbt "core/testOnly ra3.SimpleQuerySuite"  # Run a single test suite
sbt compile                       # Compile only
```

CI runs: `sbt -J-Xmx3000m test` on Java 17 (Temurin).

Tests use MUnit and Specs2 with ScalaCheck for property-based tests. Test parallelism is disabled (`Test / parallelExecution := false`).

## Scala Version & Compiler

Scala 3.6.4 only. `-Xfatal-warnings` is enabled — all warnings are errors. Experimental features are enabled: named tuples (`-language:experimental.namedTuples`) and `-experimental` for macros.

## Architecture

Single SBT module: `core` (published as `ra3-core`), source in `core/src/main/scala/ra3/`.

### Type System & Data Model

Five column types: `I32` (Int), `I64` (Long), `F64` (Double), `Str` (String), `Inst` (Instant/datetime as epoch millis). Column type metadata lives in `columntag.scala`. Missing values are encoded as sentinel values (Int.MinValue, Long.MinValue, Double.NaN, `"\u0001"`).

Tables are collections of aligned column segments. Segments are the disk I/O unit; buffers are in-memory representations. Type-specific buffer implementations live in `bufferimpl/`.

### Query Layers

- **Row-level expressions** (`lang/`): `Expr[A]` AST with operations in `lang/ops/`, syntax sugar in `lang/syntax/`, evaluation in `lang/runtime/`.
- **Table-level expressions** (`tablelang/`): `TableExpr[T]` AST for query plans. `Schema.scala` handles type-safe schema. `Render.scala` produces human-readable query descriptions.
- **Entry point** (`package.scala`): CSV import functions, query builders, main API surface.

### Query Operators

- `SimpleQuery.scala` — SELECT/WHERE (element-wise filter + projection)
- `Equijoin.scala` — equi-join with partitioning (inner/left/right/full outer)
- `GroupedTable.scala` — GROUP BY operations
- `ReduceTable.scala` — aggregation/reduction after grouping
- `PartitionedTable.scala` — data partitioning and shuffling

### Distributed Execution

`ts/` contains task definitions that integrate with `tasks-core` for distributed/parallel segment processing. Key tasks: `ImportCsv`, `ExportCsv`, `SimpleQuery`, `ComputeJoinIndex`, `MakeGroupMap`, `ExtractGroups`.

### Join Infrastructure

`join/` has type-specific joiner implementations (`JoinerIntImpl`, `JoinerLongImpl`, etc.) with locator patterns in `join/locator/`. `hashtable/` has package-private hash table implementations used for grouping and joining.

### Missing Value Semantics

- Comparisons with missing always return false (like NaN)
- Use explicit `isMissing` to filter for missing
- Logic operators follow three-valued logic (Kleene)
- Joins do not match on missing values
- GROUP BY treats missing as a separate group

## Example App

`example_cli/` is a standalone scala-cli project (separate build). Open in a separate workspace.

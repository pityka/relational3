RA3: Scala library for queries on large tabular datasets
========================================================

Introduction
============
RA3 implements relational algebra styles queries on disk backed tabular datasets.

- Paritioned joins and groupings
- Inner, left, right and full outer joins
- Projections and filters. Fully expressive, type safe query language in projections and filters. 
- Selection of top K elements 
- Data import and export to and from csv.


Missing value (null) semantics
==============================
- missing value is sorted below non missing values
- direct literal or column comparison with a missing value always returns false (like Double.NaN)
- filter for missing values is possible with explicit isMissing (like sql is null)
- NOT TRUE at the moment: joins do not join on missing values i.e.: 
- -  inner join does not return missing values in the join column
- - outer join returns one row for each missing value row, with nothing joined from other tables (ie all missing)
- group by on missing treats missing as an other (single) group
- count distinct in groups do not count missing values
- min/max in groups do not count missing values
- all elementwise operations, except predicates propagate missing value i.e. if either of their input is missing the result is missing
- predicates evaluate to false
- printf etc formats missing into a non missing string

# RA3: Scala library for queries on large tabular datasets


RA3 implements queries on large tabular datasets.

- Partitioned joins and groupings.
- Inner, left, right and full outer joins.
- Projections and filters. 
- Selection of top K elements 
- Data import and export to and from csv.
- RA3 supports only integer, long, double, string and instant (datetime) data types.

## Documentation

### Language imports
There is no need to import anything to use ra3, except for saving keystrokes. 
`import ra3._` imports no implicits into the scope. 
All of the public api is reachable from the ra3 package.

The API of ra3 is quite small:
- import of CSV
- export of CSV
- an embedded query language with projection, filter, count, join, group-by table level operators and a set of column level operators



### Configuration and runtime

RA3 is built on top of tasks for its distributed runtime and storage abstraction.

### Getting data into RA3

RA3 can not query directly data in CSV or other formats. 
Before use, data must be imported into RA3, at the moment it can only read CSV formatted files.
See the scaladoc on the `ra3` package for the method which imports data from CSV.

### Querying data

RA3 uses an embedded query language, whose syntax tree is then serialized and executed in a distributed and checkpointed manner. 



### Exporting 

Data can be copied out to CSV format.

### Missing value (null) semantics


- direct literal or column comparison with a missing value always returns false (like Double.NaN)
- filter for missing values is possible with explicit isMissing (like sql is null)
- joins do not join on missing values i.e.: 
- -  inner join does not return missing values in the join column
- - outer join returns one row for each missing value row, with nothing joined from other tables (ie all missing)
- group by on missing treats missing as an other (single) group
- count distinct in groups do not count missing values
- min/max in groups do not count missing values
- all elementwise operations, including predicates propagate missing value i.e. if either of their input is missing the result is missing, except for the isMissing oeprator
- logic operators follow 3 values logic (https://en.wikipedia.org/wiki/Three-valued_logic#Kleene_and_Priest_logics)
- printf etc formats missing into a non missing string
- Missing values are encoded as one of the values of the data type. As such these values can not be used for to encode their usual non-missing semantics: Double.NaN, Long.MinValue, Int.MinValue, "\u0001". 
- ordering of missing values below or above other elements are unobservable in ra3 because there is no full table sort operation. The operation which selects the top-k elements will not return missing values.
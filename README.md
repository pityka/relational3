# RA3: Scala library for queries on large tabular datasets

RA3 implements queries on large tabular datasets.

- Partitioned joins and groupings.
- Inner, left, right and full outer joins.
- Projections and filters. 
- Selection of top K elements 
- Data import and export to and from csv.
- RA3 supports only integer, long, double, string and instant (datetime) data types.

## Documentation

For user facing documentation see the scaladoc of the ra3 package.

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

# Build and test

`sbt test`

# License

licensed under the Apache License, Version 2.0 (the "License"); you may not
use this file except in compliance with the License. You may obtain a copy
of the License at

http://www.apache.org/licenses/LICENSE-2.0

unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
warranties OR CONDITIONS OF ANY KIND, either express or implied. See the
license for the specific language governing permissions and limitations
under the License.

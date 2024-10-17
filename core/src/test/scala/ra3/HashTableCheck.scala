/** Copyright (c) 2013 Saddle Development Team
  *
  * Licensed under the Apache License, Version 2.0 (the "License"); you may not
  * use this file except in compliance with the License. You may obtain a copy
  * of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
  * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
  * License for the specific language governing permissions and limitations
  * under the License.
  *
  * Changes:
  *   - added tests for missing values
  */
package ra3

import org.specs2.mutable.Specification
import org.specs2.ScalaCheck
import org.scalacheck.Arbitrary
import org.scalacheck.Prop.*
import ra3.join.*

class HashTableCheck extends Specification with ScalaCheck {
  "Int Index Tests" in {

    implicit val arbIndexS = Arbitrary(IndexArbitraries.indexStrWithDupsNonnull)

    "generic table" in {
      forAll { (ix1: Vector[String]) =>
        val map = ra3.hashtable.GenericTable.build(ix1.toArray, null)
        ix1.zipWithIndex
          .forall { case (key, idx) =>
            map.lookupIdx(key) == ix1.zipWithIndex
              .filter(_._1 == key)
              .map(_._2)
              .toVector
              .head && map.lookupAllIdx(key).toVector == ix1.zipWithIndex
              .filter(_._1 == key)
              .map(_._2)
              .toVector && map.lookupIdx(key + "dfasfdsfdsfsf") == -1
          }
          .must_==(true)
      }
    }
  }

}

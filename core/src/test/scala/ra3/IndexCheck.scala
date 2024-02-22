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
import org.scalacheck.Prop._
import org.saddle._

class IndexCheck extends Specification with ScalaCheck {
  "Int Index Tests" in {
    implicit val arbIndex = Arbitrary(IndexArbitraries.indexIntWithDups)

    "index joins work" in {
      forAll { (ix1: Index[Int], ix2: Index[Int]) =>
        val all = Seq(
          index.LeftJoin,
          index.RightJoin,
          index.OuterJoin,
          index.InnerJoin
        ) map { jointype =>
          val res = ix1.join(ix2, how = jointype)

          val exp = res.index.toVec
          val lix = ix1.toVec
          val rix = ix2.toVec
          val lft = res.lTake.map(x => lix.take(x)) getOrElse lix fillNA {
            exp.raw(_)
          }
          val rgt = res.rTake.map(x => rix.take(x)) getOrElse rix fillNA {
            exp.raw(_)
          }

          lft must_== exp
          rgt must_== exp
        }
        all.foldLeft(true)((acc, v) => acc && v.isSuccess)
      }
    }

  }

}

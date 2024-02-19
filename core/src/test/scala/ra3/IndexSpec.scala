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
  */
  package ra3


import org.specs2.mutable.Specification
import org.saddle._

/** User: Adam Klein Date: 2/19/13 Time: 7:17 PM
  */
class IndexSpec extends Specification {

  "Index Joins" should {

    "Outer join with dup missing " in {
      val ix1 = Index(0, 1, Int.MinValue, Int.MinValue, Int.MinValue)
      val ix2 = Index(1, 2, Int.MinValue, Int.MinValue, Int.MinValue)
      val res = (new ra3.join.JoinerImpl[Int])
        .join(ix1, ix2, index.OuterJoin)

      res.lTake.get must_== Array(0, 1, 2, 3, 4, -1, -1, -1, -1)
      res.rTake.get must_== Array(-1, 0, -1, -1, -1, 1, 2, 3, 4)

    }
    "Inner join with dup missing " in {
      val ix1 = Index(0, 1, Int.MinValue, Int.MinValue, Int.MinValue)
      val ix2 = Index(1, 2, Int.MinValue, Int.MinValue, Int.MinValue)
      val res = (new ra3.join.JoinerImpl[Int])
        .join(ix1, ix2, index.InnerJoin)

      res.lTake.get must_== Array(1)
      res.rTake.get must_== Array(0)

    }
    "Left outer join with dup missing " in {
      val ix1 = Index(0, 1, Int.MinValue, Int.MinValue, Int.MinValue)
      val ix2 = Index(1, 1, 2, Int.MinValue, Int.MinValue, Int.MinValue)
      val res = (new ra3.join.JoinerImpl[Int])
        .join(ix1, ix2, index.LeftJoin)

      res.lTake.get must_== Array(0, 1, 1, 2, 3, 4)
      res.rTake.get must_== Array(-1, 0, 1, -1, -1, -1)

    }

    "Left outer join with dup missing " in {
      val ix1 = Index(0, 1, Int.MinValue, Int.MinValue, Int.MinValue)
      val ix2 = Index(1, 2, Int.MinValue, Int.MinValue, Int.MinValue)
      val res = (new ra3.join.JoinerImpl[Int])
        .join(ix1, ix2, index.LeftJoin)

      res.lTake.isEmpty must_== true
      res.rTake.get must_== Array(-1, 0, -1, -1, -1)

    }

    "Outer join of same non-unique indexes " in {
      val ix1 = Index(0, 0)
      val ix2 = Index(0, 0)
      val res = (new ra3.join.JoinerImpl[Int])
        .join(ix1, ix2, index.OuterJoin)

      res.lTake.get must_== Array(0, 0, 1, 1)
      res.rTake.get must_== Array(0, 1, 0, 1)

    }

    "Unique sorted left join" in {
      val ix1 = Index(0, 1, 2)
      val ix2 = Index(1, 2, 3)

      val res =
        (new ra3.join.JoinerImpl[Int]).join(ix1, ix2, how = index.LeftJoin)
      res.lTake must_== None
      res.rTake.get must_== Array(-1, 0, 1)
    }

    "Unique sorted right join" in {
      val ix1 = Index(0, 1, 2)
      val ix2 = Index(1, 2, 3)

      val res =
        (new ra3.join.JoinerImpl[Int]).join(ix1, ix2, how = index.RightJoin)

      res.lTake.get must_== Array(1, 2, -1)
      res.rTake must_== None
    }

    "Unique sorted inner join" in {
      val ix1 = Index(0, 1, 2)
      val ix2 = Index(1, 2, 3)

      val res =
        (new ra3.join.JoinerImpl[Int]).join(ix1, ix2, how = index.InnerJoin)
      res.lTake.get must_== Array(1, 2)
      res.rTake.get must_== Array(0, 1)
    }

    "Unique sorted outer join" in {
      val ix1 = Index(0, 1, 2)
      val ix2 = Index(1, 2, 3)

      val res =
        (new ra3.join.JoinerImpl[Int]).join(ix1, ix2, how = index.OuterJoin)

      res.lTake.get must_== Array(0, 1, 2, -1)
      res.rTake.get must_== Array(-1, 0, 1, 2)
    }

    "Unique unsorted left join" in {
      val ix1 = Index(1, 0, 2)
      val ix2 = Index(2, 3, 1)

      val res =
        (new ra3.join.JoinerImpl[Int]).join(ix1, ix2, how = index.LeftJoin)

      res.lTake must_== None
      res.rTake.get must_== Array(2, -1, 0)
    }

    "Unique unsorted right join" in {
      val ix1 = Index(1, 0, 2)
      val ix2 = Index(2, 1, 3)

      val res =
        (new ra3.join.JoinerImpl[Int]).join(ix1, ix2, how = index.RightJoin)

      res.lTake.get must_== Array(2, 0, -1)
      res.rTake must_== None
    }

    "Unique unsorted inner join" in {
      val ix1 = Index(1, 0, 2)
      val ix2 = Index(2, 1, 3)

      val res =
        (new ra3.join.JoinerImpl[Int]).join(ix1, ix2, how = index.InnerJoin)

      res.lTake.get must_== Array(0, 2)
      res.rTake.get must_== Array(1, 0)
    }

    "Unique unsorted outer join" in {
      val ix1 = Index(1, 0, 2)
      val ix2 = Index(2, 1, 3)

      val res =
        (new ra3.join.JoinerImpl[Int]).join(ix1, ix2, how = index.OuterJoin)

      res.lTake.get must_== Array(0, 1, 2, -1)
      res.rTake.get must_== Array(1, -1, 0, 2)
    }

    "Non-unique sorted left join" in {
      val ix1 = Index(0, 1, 1, 2)
      val ix2 = Index(1, 2, 2, 3)

      val res =
        (new ra3.join.JoinerImpl[Int]).join(ix1, ix2, how = index.LeftJoin)

      res.lTake.get must_== Array(0, 1, 2, 3, 3)
      res.rTake.get must_== Array(-1, 0, 0, 1, 2)
    }

    "Non-unique sorted left join [case 2]" in {
      val ix1 = Index(0, 1, 1, 2)
      val ix2 = Index(1)

      val res1 =
        (new ra3.join.JoinerImpl[Int]).join(ix1, ix2, how = index.LeftJoin)

      res1.lTake must_== None
      res1.rTake.get must_== Array(-1, 0, 0, -1)

      val res2 = ix2.join(ix1, how = index.LeftJoin)

      res2.index must_== Index(1, 1)
      res2.lTake.get must_== Array(0, 0)
      res2.rTake.get must_== Array(1, 2)
    }

    "Non-unique sorted left join [case 3]" in {
      val ix1 = Index(0, 1, 1, 2, 2)
      val ix2 = Index(1, 2, 2, 3)

      val res =
        (new ra3.join.JoinerImpl[Int]).join(ix1, ix2, how = index.LeftJoin)

      res.lTake.get must_== Array(0, 1, 2, 3, 3, 4, 4)
      res.rTake.get must_== Array(-1, 0, 0, 1, 2, 1, 2)
    }

    "Non-unique sorted right join" in {
      val ix1 = Index(0, 1, 1, 2)
      val ix2 = Index(1, 2, 2, 3)

      val res =
        (new ra3.join.JoinerImpl[Int]).join(ix1, ix2, how = index.RightJoin)

      res.lTake.get must_== Array(1, 2, 3, 3, -1)
      res.rTake.get must_== Array(0, 0, 1, 2, 3)
    }

    "Non-unique sorted inner join" in {
      val ix1 = Index(0, 1, 1, 2)
      val ix2 = Index(1, 2, 2, 3)

      val res =
        (new ra3.join.JoinerImpl[Int]).join(ix1, ix2, how = index.InnerJoin)

      res.lTake.get must_== Array(1, 2, 3, 3)
      res.rTake.get must_== Array(0, 0, 1, 2)
    }

    "Non-unique sorted inner join [case 2]" in {
      val ix1 = Index(1, 1, 3, 4)
      val ix2 = Index(1)

      val res1 =
        (new ra3.join.JoinerImpl[Int]).join(ix1, ix2, how = index.InnerJoin)

      res1.lTake.get must_== Array(0, 1)
      res1.rTake.get must_== Array(0, 0)

      val res2 = ix2.join(ix1, how = index.InnerJoin)

      res2.index must_== Index(1, 1)
      res2.lTake.get must_== Array(0, 0)
      res2.rTake.get must_== Array(0, 1)
    }

    "Non-unique sorted inner join [case 3]" in {
      val ix1 = Index(0, 1, 1, 2, 2)
      val ix2 = Index(1, 2, 2, 3)

      val res =
        (new ra3.join.JoinerImpl[Int]).join(ix1, ix2, how = index.InnerJoin)
      res.lTake.get must_== Array(1, 2, 3, 4, 3, 4)
      res.rTake.get must_== Array(0, 0, 1, 1, 2, 2)
    }

    "Non-unique sorted outer join" in {
      val ix1 = Index(0, 1, 1, 2)
      val ix2 = Index(1, 2, 2, 3)

      val res =
        (new ra3.join.JoinerImpl[Int]).join(ix1, ix2, how = index.OuterJoin)

      res.lTake.get must_== Array(0, 1, 2, 3, 3, -1)
      res.rTake.get must_== Array(-1, 0, 0, 1, 2, 3)
    }

    "Non-unique sorted outer join [case 2]" in {
      val ix1 = Index(0, 1, 1, 2)
      val ix2 = Index(1)

      val res1 =
        (new ra3.join.JoinerImpl[Int]).join(ix1, ix2, how = index.OuterJoin)

      res1.lTake.get must_== Array(0, 1, 2, 3)
      res1.rTake.get must_== Array(-1, 0, 0, -1)

      val res2 = ix2.join(ix1, how = index.OuterJoin)

      res2.index must_== Index(0, 1, 1, 2)
      res2.lTake.get must_== Array(-1, 0, 0, -1)
      res2.rTake.get must_== Array(0, 1, 2, 3)
    }

    "Non-unique unsorted left join" in {
      val ix1 = Index(1, 1, 2, 0)
      val ix2 = Index(1, 3, 2, 2)

      val res =
        (new ra3.join.JoinerImpl[Int]).join(ix1, ix2, how = index.LeftJoin)

      res.lTake.get must_== Array(0, 1, 2, 2, 3)
      res.rTake.get must_== Array(0, 0, 2, 3, -1)
    }

    "Non-unique unsorted left join [case 2]" in {
      val ix1 = Index(1, 1, 2, 2, 0)
      val ix2 = Index(1, 3, 2, 2)

      val res =
        (new ra3.join.JoinerImpl[Int]).join(ix1, ix2, how = index.LeftJoin)

      res.lTake.get must_== Array(0, 1, 2, 2, 3, 3, 4)
      res.rTake.get must_== Array(0, 0, 2, 3, 2, 3, -1)
    }

    "Non-unique unsorted right join" in {
      val ix1 = Index(1, 1, 2, 0)
      val ix2 = Index(1, 3, 2, 2)

      val res =
        (new ra3.join.JoinerImpl[Int]).join(ix1, ix2, how = index.RightJoin)

      res.lTake.get must_== Array(0, 1, -1, 2, 2)
      res.rTake.get must_== Array(0, 0, 1, 2, 3)
    }

    "Non-unique unsorted inner join" in {
      val ix1 = Index(1, 1, 2, 0)
      val ix2 = Index(1, 3, 2, 2)

      val res =
        (new ra3.join.JoinerImpl[Int]).join(ix1, ix2, how = index.InnerJoin)

      res.lTake.get must_== Array(0, 1, 2, 2)
      res.rTake.get must_== Array(0, 0, 2, 3)
    }

    "Non-unique unsorted outer join" in {
      val ix1 = Index(1, 1, 2, 0)
      val ix2 = Index(1, 3, 2, 2)

      val res =
        (new ra3.join.JoinerImpl[Int]).join(ix1, ix2, how = index.OuterJoin)

      res.lTake.get must_== Array(0, 1, 2, 2, 3, -1)
      res.rTake.get must_== Array(0, 0, 2, 3, -1, 1)
    }

  }
}

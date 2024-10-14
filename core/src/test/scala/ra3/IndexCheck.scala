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

class IndexCheck extends Specification with ScalaCheck {
  "Int Index Tests" in {
      def Index(i: Int*) = ra3.join.locator.LocatorInt.fromKeys(i.toArray)
      def IndexL(i: Long*) = ra3.join.locator.LocatorLong.fromKeys(i.toArray)
      def IndexD(i: Double*) = ra3.join.locator.LocatorDouble.fromKeys(i.toArray)
      def IndexS(i: String*) = ra3.join.locator.LocatorAny.fromKeys(i.toArray)


    implicit val arbIndex = Arbitrary(IndexArbitraries.indexIntWithDups)
    implicit val arbIndexLong = Arbitrary(IndexArbitraries.indexLongWithDups)
    implicit val arbIndexD = Arbitrary(IndexArbitraries.indexDoubleWithDups)
    implicit val arbIndexS = Arbitrary(IndexArbitraries.indexStrWithDups)

    "index joins work Int" in {
      forAll { (ix1: Vector[Int], ix2: Vector[Int]) =>
        val all = Seq(
          LeftJoin,
          RightJoin,
          OuterJoin,
          InnerJoin
        ) map { jointype =>
          val res = ra3.join.JoinerImplInt.join(Index(ix1*),Index(ix2*),jointype) 
          import org.saddle.*
          val lix = ix1.toVec
          val rix = ix2.toVec
          val lft = res.lTake.map(x => lix.take(x)).getOrElse(lix)
          val rgt = res.rTake.map(x => rix.take(x)).getOrElse(rix)

          

          lft.toSeq.zip(rgt.toSeq).forall{ case (a,b) => 
            if (ix1.contains(b) && ix2.contains(a)) a == b 
            else if (a == BufferInt.MissingValue) ix2.contains(b) && !(b  == BufferInt.MissingValue)
            else if (b  == BufferInt.MissingValue) ix1.contains(a) && !(a == BufferInt.MissingValue)
            else ???
          }.&&{
            val all = (ix1 ++ ix2).distinct.filterNot(_ == ra3.BufferInt.MissingValue)
            all.forall{x =>

              val c1 =  ix1.count(_ == x)
              val c2 = ix2.count(_ == x)
              jointype match {
                case InnerJoin => 
                  val m1 = c1 * c2                  
                  val m2 = c2 * c1
                    lft.toArray.count(_ == x) ==m1  && rgt.toArray.count(_ == x) == m2
                case LeftJoin => 
                  val m1 = c1 * math.max(1,c2)                  
                  val m2 = c2 * math.max(0,c1)                  
                    lft.toArray.count(_ == x) ==m1  && rgt.toArray.count(_ == x) == m2
                case RightJoin => 
                   val m1 = c1 * math.max(0,c2)                  
                  val m2 = c2 * math.max(1,c1)                  
                    lft.toArray.count(_ == x) ==m1  && rgt.toArray.count(_ == x) == m2
                case OuterJoin =>
                  val m1 = c1 * math.max(1,c2)                  
                  val m2 = c2 * math.max(1,c1)                  
                    lft.toArray.count(_ == x) ==m1  && rgt.toArray.count(_ == x) == m2
              }
              
            }
          }.must_==(true)
          
        }
        all.foldLeft(true)((acc, v) => acc && v.isSuccess)
      }
    }
    "index joins work Long" in {
      forAll { (ix1: Vector[Long], ix2: Vector[Long]) =>
        val all = Seq(
          LeftJoin,
          RightJoin,
          OuterJoin,
          InnerJoin
        ) map { jointype =>
          val res = ra3.join.JoinerImplLong.join(IndexL(ix1*),IndexL(ix2*),jointype) 
          import org.saddle.*
          val lix = ix1.toVec
          val rix = ix2.toVec
          val lft = res.lTake.map(x => lix.take(x)).getOrElse(lix)
          val rgt = res.rTake.map(x => rix.take(x)).getOrElse(rix)

          

          lft.toSeq.zip(rgt.toSeq).forall{ case (a,b) => 
            if (ix1.contains(b) && ix2.contains(a)) a == b 
            else if (a == BufferLong.MissingValue) ix2.contains(b) && !(b  == BufferLong.MissingValue)
            else if (b  == BufferLong.MissingValue) ix1.contains(a) && !(a == BufferLong.MissingValue)
            else ???
          }.&&{
            val all = (ix1 ++ ix2).distinct.filterNot(_ == ra3.BufferLong.MissingValue)
            all.forall{x =>

              val c1 =  ix1.count(_ == x)
              val c2 = ix2.count(_ == x)
              jointype match {
                case InnerJoin => 
                  val m1 = c1 * c2                  
                  val m2 = c2 * c1
                    lft.toArray.count(_ == x) ==m1  && rgt.toArray.count(_ == x) == m2
                case LeftJoin => 
                  val m1 = c1 * math.max(1,c2)                  
                  val m2 = c2 * math.max(0,c1)                  
                    lft.toArray.count(_ == x) ==m1  && rgt.toArray.count(_ == x) == m2
                case RightJoin => 
                   val m1 = c1 * math.max(0,c2)                  
                  val m2 = c2 * math.max(1,c1)                  
                    lft.toArray.count(_ == x) ==m1  && rgt.toArray.count(_ == x) == m2
                case OuterJoin =>
                  val m1 = c1 * math.max(1,c2)                  
                  val m2 = c2 * math.max(1,c1)                  
                    lft.toArray.count(_ == x) ==m1  && rgt.toArray.count(_ == x) == m2
              }
              
            }
          }.must_==(true)
          
        }
        all.foldLeft(true)((acc, v) => acc && v.isSuccess)
      }
    }
    "index joins work Double" in {
      forAll { (ix1: Vector[Double], ix2: Vector[Double]) =>
        val all = Seq(
          LeftJoin,
          RightJoin,
          OuterJoin,
          InnerJoin
        ) map { jointype =>
          val res = ra3.join.JoinerImplDouble.join(IndexD(ix1*),IndexD(ix2*),jointype) 
          import org.saddle.*
          val lix = ix1.toVec
          val rix = ix2.toVec
          val lft = res.lTake.map(x => lix.take(x)).getOrElse(lix)
          val rgt = res.rTake.map(x => rix.take(x)).getOrElse(rix)

          

          val pass = lft.toSeq.zip(rgt.toSeq).forall{ case (a,b) => 
            if (ix1.contains(b) && ix2.contains(a)) a == b 
            else if (a.isNaN && b.isNaN) ix1.exists(_.isNaN) && ix2.exists(_.isNaN)
            else if (a.isNaN) ix2.contains(b) && !b.isNaN
            else if (b.isNaN) ix1.contains(a) && !a.isNaN
            else ???
          }.&&{
            val all = (ix1 ++ ix2).distinct.filterNot(_.isNaN)
            all.forall{x =>

              val c1 =  ix1.count(_ == x)
              val c2 = ix2.count(_ == x)
              jointype match {
                case InnerJoin => 
                  val m1 = c1 * c2                  
                  val m2 = c2 * c1
                    lft.toArray.count(_ == x) ==m1  && rgt.toArray.count(_ == x) == m2
                case LeftJoin => 
                  val m1 = c1 * math.max(1,c2)                  
                  val m2 = c2 * math.max(0,c1)                  
                    lft.toArray.count(_ == x) ==m1  && rgt.toArray.count(_ == x) == m2
                case RightJoin => 
                   val m1 = c1 * math.max(0,c2)                  
                  val m2 = c2 * math.max(1,c1)                  
                    lft.toArray.count(_ == x) ==m1  && rgt.toArray.count(_ == x) == m2
                case OuterJoin =>
                  val m1 = c1 * math.max(1,c2)                  
                  val m2 = c2 * math.max(1,c1)                  
                    lft.toArray.count(_ == x) ==m1  && rgt.toArray.count(_ == x) == m2
              }

              
              
            }
          }

          if (!pass) {
            println((ix1,ix2,lft,rgt,jointype))
          }
          
          pass.must_==(true)
          
        }
        all.foldLeft(true)((acc, v) => acc && v.isSuccess)
      }
    }
    "index joins work String" in {
      forAll { (ix1: Vector[String], ix2: Vector[String]) =>
        val all = Seq(
          LeftJoin,
          RightJoin,
          OuterJoin,
          InnerJoin
        ) map { jointype =>
          val res = (new ra3.join.JoinerImplAny[String](_ == null)).join(IndexS(ix1*),IndexS(ix2*),jointype) 
          import org.saddle.*
          val lix = ix1.toVec
          val rix = ix2.toVec
          val lft = res.lTake.map(x => lix.take(x)).getOrElse(lix)
          val rgt = res.rTake.map(x => rix.take(x)).getOrElse(rix)

          

          val pass = lft.toSeq.zip(rgt.toSeq).forall{ case (a,b) => 
            if (ix1.contains(b) && ix2.contains(a)) a == b 
            else if (a == null) ix2.contains(b) && !(b == null)
            else if (b == null) ix1.contains(a) && !(a == null)
            else ???
          }.&&{
            val all = (ix1 ++ ix2).distinct.filterNot(_ == null)
            all.forall{x =>

              val c1 =  ix1.count(_ == x)
              val c2 = ix2.count(_ == x)
              jointype match {
                case InnerJoin => 
                  val m1 = c1 * c2                  
                  val m2 = c2 * c1
                    lft.toArray.count(_ == x) ==m1  && rgt.toArray.count(_ == x) == m2
                case LeftJoin => 
                  val m1 = c1 * math.max(1,c2)                  
                  val m2 = c2 * math.max(0,c1)                  
                    lft.toArray.count(_ == x) ==m1  && rgt.toArray.count(_ == x) == m2
                case RightJoin => 
                   val m1 = c1 * math.max(0,c2)                  
                  val m2 = c2 * math.max(1,c1)                  
                    lft.toArray.count(_ == x) ==m1  && rgt.toArray.count(_ == x) == m2
                case OuterJoin =>
                  val m1 = c1 * math.max(1,c2)                  
                  val m2 = c2 * math.max(1,c1)                  
                    lft.toArray.count(_ == x) ==m1  && rgt.toArray.count(_ == x) == m2
              }

              
              
            }
          }

          
          
          pass.must_==(true)
          
        }
        all.foldLeft(true)((acc, v) => acc && v.isSuccess)
      }
    }

  }

}

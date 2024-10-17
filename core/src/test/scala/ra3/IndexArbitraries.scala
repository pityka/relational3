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

import org.scalacheck.Gen

object IndexArbitraries {

  // Generates int index with duplicates

  def indexIntWithDups: Gen[Vector[Int]] =
    for {
      l <- Gen.choose(0, 20)
      lst <- Gen.listOfN(l, Gen.chooseNum(0, l))
    } yield (lst ++ List(Int.MinValue, Int.MinValue)).toVector
  def indexLongWithDups: Gen[Vector[Long]] =
    for {
      l <- Gen.choose(0, 20)
      lst <- Gen.listOfN(l, Gen.chooseNum(0, l))
    } yield (lst.map(_.toLong) ++ List(Long.MinValue, Long.MinValue)).toVector
  def indexDoubleWithDups: Gen[Vector[Double]] =
    for {
      l <- Gen.choose(0, 20)
      lst <- Gen.listOfN(l, Gen.chooseNum(0, l))
    } yield (lst.map(_.toDouble) ++ List(Double.NaN, Double.NaN)).toVector
  def indexStrWithDups: Gen[Vector[String]] =
    for {
      l <- Gen.choose(0, 20)
      lst <- Gen.listOfN(l, Gen.chooseNum(0, l))
    } yield (lst.map(_.toString) ++ List(null, null)).toVector
  def indexStrWithDupsNonnull: Gen[Vector[String]] =
    for {
      l <- Gen.choose(0, 20)
      lst <- Gen.listOfN(l, Gen.chooseNum(0, l))
    } yield (lst.map(_.toString)).toVector

}

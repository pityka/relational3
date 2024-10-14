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
  *   - removed factorized join and monotonic joins
  *   - adapted unique join methods to non unique cases
  *   - added handling of missing values
  */
package ra3.join
import ra3.join.locator.LocatorDouble
import ra3.join.MutableBuffer


/** Concrete implementation of Joiner instance which is specialized on basic
  * types.
  */
private[ra3] object JoinerImplDouble {

  /** Perform database joins
    *
    * @param left
    *   left index to join
    * @param right
    *   right index to join
    * @param how
    *   mode of operation: inner, left outer, right outer, full outer
    * @return
    */
  def join(
      left: LocatorDouble,
      right: LocatorDouble,
      how: JoinType
  ): ReIndexer = {

    how match {
      case InnerJoin => innerJoin(left, right)
      case OuterJoin => outerJoin(left, right)
      case LeftJoin  => leftJoin(left, right)
      case RightJoin => leftJoin(right, left).swap
    }

  }

  private def leftJoin(left: LocatorDouble, right: LocatorDouble): ReIndexer = {
    val ll = left.length
    val rightBuffer = MutableBuffer.emptyI(ll)
    val leftBuffer =  MutableBuffer.emptyI(ll)
    val leftKeys = left.allKeys
    var returnLeftBuffer = false
    var i = 0
    while (i < ll) {
      val otherVal = leftKeys(i)
      if (otherVal.isNaN) {
        rightBuffer.+=(-1)
        leftBuffer.+=(i)
      } else {
        val c = right.count(otherVal)
        if (c == 0) {
          rightBuffer.+=(-1)
          leftBuffer.+=(i)
        } else if (c == 1) {
          rightBuffer.+=(right.getFirst(otherVal))
          leftBuffer.+=(i)
        } else {
          returnLeftBuffer = true
          val rIdx = right.getAll(otherVal)
          var j = 0
          while (j < rIdx.length) {
            rightBuffer.+=(rIdx(j))
            leftBuffer.+=(i)
            j += 1
          }
        }
      }

      i += 1
    }
    ReIndexer(
      if (returnLeftBuffer) Some(leftBuffer.toArray) else None,
      Some(rightBuffer.toArray)
    )

  }

  def innerJoin(left: LocatorDouble, right: LocatorDouble): ReIndexer = {
    // want to scan over the smaller one; make left the smaller one
    val sizeHint = if (left.length > right.length) right.length else left.length

    val leftBuffer = MutableBuffer.emptyI(sizeHint)
    val rightBuffer = MutableBuffer.emptyI(sizeHint)

    val switchLR = left.length > right.length
    
    val (ltmp, rtmp) = if (switchLR) (right, left) else (left, right)
    val ltmpKeys =ltmp.allKeys 
    var i = 0
    while (i < ltmp.length) {
      val k = ltmpKeys(i)
      if (k.isNaN) {
        ()
      } else {
        val c = rtmp.count(k)
        if (c == 0) {
          ()
        } else if (c == 1) {
          rightBuffer.+=(rtmp.getFirst(k))
          leftBuffer.+=(i)
        } else {
          val rIdx = rtmp.getAll(k)
          var j = 0
          while (j < rIdx.length) {
            rightBuffer.+=(rIdx(j))
            leftBuffer.+=(i)
            j += 1
          }
        }

      }
      i += 1
    }
    val (lres, rres) =
      if (switchLR) (rightBuffer, leftBuffer) else (leftBuffer, rightBuffer)

    ReIndexer(Some(lres.toArray), Some(rres.toArray))
  }

  def outerJoin(left: LocatorDouble, right: LocatorDouble): ReIndexer = {
    // hits hashmap
    val szhint = left.length + right.length

    val lft = MutableBuffer.emptyI(szhint)
    val rgt = MutableBuffer.emptyI(szhint)

    val leftKeys = left.allKeys
    val rightKeys = right.allKeys
    var i = 0
    while (i < left.length) {
      val v = leftKeys(i)
      if (v.isNaN) {
        lft.+=(i)
        rgt.+=(-1)
      } else {
        val c = right.count(v)
        if (c == 0) {
          lft.+=(i)
          rgt.+=(-1)
        } else if (c == 1) {
          lft.+=(i)
          rgt.+=(right.getFirst(v))
        } else {

          val rIdx = right.getAll(v)
          var j = 0
          while (j < rIdx.length) {
            lft.+=(i)
            rgt.+=(rIdx(j))
            j += 1
          }
        }

      }

      i += 1
    }

    var j = 0
    while (j < right.length) {
      val v = rightKeys(j)

      if (v.isNaN) {
        rgt.+=(j)
        lft.+=(-1)
      } else {
        val c = left.count(v)
        if (c == 0) {
          rgt.+=(j)
          lft.+=(-1)
        }
      }
      j += 1
    }

    ReIndexer(Some(lft.toArray), Some(rgt.toArray))
  }

}

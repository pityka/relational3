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
package ra3.join.locator

import ra3.join.*

@scala.annotation.nowarn
private[ra3] class LocatorDouble(val allKeys: Array[Double]) {
  private val (cts, uniqueIdx) = ra3.hashtable.DoubleTable
    .buildWithUniques(allKeys, Array.ofDim[Int](allKeys.length))

  private val uniqueBuffer = uniqueIdx.map(allKeys)
  private var i = 0
  while (i < allKeys.length) {
    cts.mutate(allKeys(i), _ + 1)
    i += 1
  }
  private val map = ra3.hashtable.DoubleTable
    .build(allKeys, null)

  def contains(key: Double): Boolean = map.contains(key)
  def get(key: Double): Int = map.lookupIdx(key)
  def getAll(key: Double): Array[Int] = if (map.contains(key))
    map.lookupAllIdx(key)
  else Locator.emptyArray

  def count(key: Double): Int = {
    val c = cts.lookupIdx(key)
    if (c == -1) 0 else cts.payload(c)
  }

  def uniqueKeys: Array[Double] = uniqueBuffer.toArray
  def length: Int = allKeys.length
  def getFirst(key: Double): Int = get(key)
}

private[ra3] object LocatorDouble {
  def fromKeys(
      keys: Array[Double]
  ): LocatorDouble = {
    LocatorDouble(keys)

  }
}

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
import ra3.join.MutableBuffer
import ra3.join.util._
  


private[ra3] class LocatorDouble(val allKeys: Array[Double]) {
  private val uniqueBuffer = MutableBuffer.emptyD
  private val map = new DoubleMap
  private val cts = new DoubleMap
  private val mapAll = new DoubleBufferMap


  def getAll(key: Double): Array[Int] = if (mapAll.contains(key)) mapAll.get(key).toArray else Locator.emptyArray

  def length: Int = allKeys.length
  def contains(key: Double): Boolean = map.contains(key)
  def get(key: Double): Int = if (map.contains(key)) map.get(key) else -1 
  def getFirst(key: Double): Int = get(key)
  def put(key: Double, value: Int) = if (!contains(key)) {
    map.update(key, value)
    mapAll.update(key,value)
    uniqueBuffer.+=(key)
  }
  def put2(key: Double, value: Int) = {
    mapAll.update(key,value)
  }
  def count(key: Double): Int = if (cts.contains(key)) cts.get(key) else 0 
  def inc(key: Double): Int = {
    val u = count(key)
    cts.update(key, u + 1)
    u
  }
  def uniqueKeys: Array[Double] = uniqueBuffer.toArray
  def counts: Array[Int] = {
    val res = Array.ofDim[Int](uniqueKeys.length)
    var i = 0
    uniqueKeys.foreach { key =>
      res(i) = count(key)
      i += 1
    }
    res
  }
}

private[ra3] object LocatorDouble {
  def fromKeys(
      keys: Array[Double]
  ): LocatorDouble = {
    val map =  LocatorDouble(keys)
    var i = 0
    while (i < keys.length) {
      val k = keys(i)
      if (map.inc(k) == 0) {
        map.put(k, i)
      } else {
        map.put2(k,i)
      }
      i += 1
    }
    map
  }
}
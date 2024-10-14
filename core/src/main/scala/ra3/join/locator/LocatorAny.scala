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
import scala.reflect.ClassTag


@scala.annotation.nowarn
private[ra3] class LocatorAny[T:ClassTag](val allKeys: Array[T])  {
  private val uniqueBuffer = MutableBuffer.emptyG[T]
  private val map = new scala.collection.mutable.HashMap[T, Int]
  private val mapAll = new scala.collection.mutable.HashMap[T, MBufferInt]
  private val cts = new scala.collection.mutable.HashMap[T, Int]

  def contains(key: T): Boolean = map.contains(key)
  def get(key: T): Int = map.get(key).getOrElse(-1)
  def getAll(key: T): Array[Int] = if (mapAll.contains(key)) mapAll(key).toArray 
  else Locator.emptyArray

  def put(key: T, value: Int) = if (!contains(key)) {
    map.update(key, value)
    if (mapAll.contains(key)) {
      mapAll(key).+=(value)
    } else {
      val b= MutableBuffer.emptyI
      b.+=(value)
      mapAll.update(key,b)
    }
    uniqueBuffer.+=(key)
  }
  def put2(key: T, value: Int) = {
      mapAll(key).+=(value)
    
  }
  def count(key: T): Int = cts.get(key).getOrElse(0)
  def inc(key: T): Int = {
    val u = count(key)
    cts.update(key, u + 1)
    u
  }
  def uniqueKeys: Array[T] = uniqueBuffer.toArray
  def counts: Array[Int] = {
    val iter = uniqueKeys.iterator
    val res = Array.ofDim[Int](uniqueKeys.length)
    var i = 0
    while (iter.hasNext) {
      res(i) = count(iter.next())
      i += 1
    }
    res
  }
  def length: Int = allKeys.length
  def getFirst(key: T): Int = get(key)
}

private[ra3] object LocatorAny {
  def fromKeys[T:ClassTag](
      keys: Array[T]
  ): LocatorAny[T] = {
    val map =  LocatorAny[T](keys)
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
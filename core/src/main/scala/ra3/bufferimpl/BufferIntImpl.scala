package ra3.bufferimpl
import ra3.*
private[ra3] trait BufferIntImpl { self: BufferInt =>
  def elementwise_abs: BufferInt
  def elementwise_not: BufferInt
  def elementwise_isMissing: BufferInt
  def elementwise_containedIn(i: Set[Int]): BufferInt

  def elementwise_*(other: BufferDouble): BufferDouble
  def elementwise_+(other: BufferDouble): BufferDouble

  def elementwise_*(other: BufferInt): BufferInt
  def elementwise_+(other: BufferInt): BufferInt

  def elementwise_&&(other: BufferType): BufferInt
  def elementwise_||(other: BufferType): BufferInt

  def elementwise_eq(other: BufferType): BufferInt
  def elementwise_eq(other: Int): BufferInt
  def elementwise_gt(other: BufferType): BufferInt
  def elementwise_gt(other: Int): BufferInt
  def elementwise_gteq(other: BufferType): BufferInt
  def elementwise_gteq(other: Int): BufferInt
  def elementwise_lt(other: BufferType): BufferInt
  def elementwise_lt(other: Int): BufferInt
  def elementwise_lteq(other: BufferType): BufferInt
  def elementwise_lteq(other: Int): BufferInt
  def elementwise_neq(other: BufferType): BufferInt
  def elementwise_neq(other: Int): BufferInt

  def elementwise_eq(other: BufferDouble): BufferInt
  def elementwise_gt(other: BufferDouble): BufferInt
  def elementwise_gteq(other: BufferDouble): BufferInt
  def elementwise_lt(other: BufferDouble): BufferInt
  def elementwise_lteq(other: BufferDouble): BufferInt
  def elementwise_neq(other: BufferDouble): BufferInt

  def elementwise_toDouble: BufferDouble
  def elementwise_toLong: BufferLong
  def elementwise_printf(s: String): BufferString

  def minInGroups(partitionMap: BufferInt, numGroups: Int): BufferType
  def maxInGroups(partitionMap: BufferInt, numGroups: Int): BufferType
  def hasMissingInGroup(partitionMap: BufferInt, numGroups: Int): BufferInt
  def countInGroups(partitionMap: BufferInt, numGroups: Int): BufferType
  def countDistinctInGroups(partitionMap: BufferInt, numGroups: Int): BufferInt
  def meanInGroups(partitionMap: BufferInt, numGroups: Int): BufferDouble
  def allInGroups(partitionMap: BufferInt, numGroups: Int): BufferInt
  def anyInGroups(partitionMap: BufferInt, numGroups: Int): BufferInt
  def noneInGroups(partitionMap: BufferInt, numGroups: Int): BufferInt

  def sumGroups(partitionMap: BufferInt, numGroups: Int): BufferType
  def makeStatistic(): StatisticInt

  def elementwise_choose(t: BufferInt, f: BufferInt): BufferInt
  def elementwise_choose(t: Int, f: BufferInt): BufferInt
  def elementwise_choose(t: BufferInt, f: Int): BufferInt
  def elementwise_choose(t: Int, f: Int): BufferInt

  def elementwise_choose(t: BufferDouble, f: BufferDouble): BufferDouble
  def elementwise_choose(t: Double, f: BufferDouble): BufferDouble
  def elementwise_choose(t: BufferDouble, f: Double): BufferDouble
  def elementwise_choose(t: Double, f: Double): BufferDouble

  def elementwise_choose(t: BufferLong, f: BufferLong): BufferLong
  def elementwise_choose(t: Long, f: BufferLong): BufferLong
  def elementwise_choose(t: BufferLong, f: Long): BufferLong
  def elementwise_choose(t: Long, f: Long): BufferLong

  def elementwise_choose(t: BufferString, f: BufferString): BufferString
  def elementwise_choose(t: String, f: BufferString): BufferString
  def elementwise_choose(t: BufferString, f: String): BufferString
  def elementwise_choose(t: String, f: String): BufferString

  def elementwise_choose(t: BufferInstant, f: BufferInstant): BufferInstant
  def elementwise_choose(t: Long, f: BufferInstant): BufferInstant
  def elementwise_choose(t: BufferInstant, f: Long): BufferInstant
  def elementwise_choose_inst(t: Long, f: Long): BufferInstant
}

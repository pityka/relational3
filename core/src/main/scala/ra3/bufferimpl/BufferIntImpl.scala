package ra3.bufferimpl
import ra3._
private[ra3] trait BufferIntImpl { self: BufferInt =>
  def elementwise_*=(other: BufferType): Unit
  def elementwise_*(other: BufferDouble): BufferDouble
  def elementwise_+=(other: BufferType): Unit
  def elementwise_+(other: BufferDouble): BufferDouble


  def elementwise_&&(other: BufferType): BufferInt
  def elementwise_||(other: BufferType): BufferInt


  def elementwise_eq(other: BufferType): BufferInt
  def elementwise_gt(other: BufferType): BufferInt
  def elementwise_gteq(other: BufferType): BufferInt
  def elementwise_lt(other: BufferType): BufferInt
  def elementwise_lteq(other: BufferType): BufferInt
  def elementwise_neq(other: BufferType): BufferInt

  def elementwise_eq(other: BufferDouble): BufferInt
  def elementwise_gt(other: BufferDouble): BufferInt
  def elementwise_gteq(other: BufferDouble): BufferInt
  def elementwise_lt(other: BufferDouble): BufferInt
  def elementwise_lteq(other: BufferDouble): BufferInt
  def elementwise_neq(other: BufferDouble): BufferInt

  def elementwise_toDouble: BufferDouble
  def elementwise_toLong: BufferLong
}

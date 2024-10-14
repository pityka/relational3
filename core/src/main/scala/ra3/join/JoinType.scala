package ra3.join

private[ra3] sealed trait JoinType 
private[ra3] case object InnerJoin extends JoinType
private[ra3] case object LeftJoin extends JoinType
private[ra3] case object RightJoin extends JoinType
private[ra3] case object OuterJoin extends JoinType
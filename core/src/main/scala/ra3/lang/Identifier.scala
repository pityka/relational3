package ra3.lang

private[ra3] class Identifier[T0](id: Expr { type T = T0 }) {
  def apply[T1](
      body: Expr { type T = T0 } => Expr { type T = T1 }
  ): Expr { type T = T1 } =
    local(id)(body)
}

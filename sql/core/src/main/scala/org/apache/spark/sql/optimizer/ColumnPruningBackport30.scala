package org.apache.spark.sql.optimizer

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.expressions.{Alias, And, AttributeMap, AttributeReference, AttributeSet, Expression, ExpressionSet, ExtractValue, Generator, PredicateHelper, PythonUDF, SubqueryExpression}
import org.apache.spark.sql.catalyst.plans.{ExistenceJoin, FullOuter, InnerLike, LeftAnti, LeftExistence, LeftOuter, LeftSemi, NaturalJoin, RightOuter, UsingJoin}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, AppendColumns, DeserializeToObject, Distinct, EventTimeWatermark, Expand, Filter, FlatMapGroupsInPandas, Generate, Join, LeafNode, LogicalPlan, Pivot, Project, Repartition, RepartitionByExpression, ScriptTransformation, SerializeFromObject, SetOperation, Sort, UnaryNode, Union, Window}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.internal.SQLConf

/**
 * Attempts to eliminate the reading of unneeded columns from the query plan.
 *
 * Since adding Project before Filter conflicts with PushPredicatesThroughProject, this rule will
 * remove the Project p2 in the following pattern:
 *
 *   p1 @ Project(_, Filter(_, p2 @ Project(_, child))) if p2.outputSet.subsetOf(p2.inputSet)
 *
 * p2 is usually inserted by this rule and useless, p1 could prune the columns anyway.
 */
object ColumnPruningBackport30 extends Rule[LogicalPlan] {

  def apply(plan: LogicalPlan): LogicalPlan = removeProjectBeforeFilter(plan transform {
    // Prunes the unused columns from project list of Project/Aggregate/Expand
    case p @ Project(_, p2: Project) if !p2.outputSet.subsetOf(p.references) =>
      p.copy(child = p2.copy(projectList = p2.projectList.filter(p.references.contains)))
    case p @ Project(_, a: Aggregate) if !a.outputSet.subsetOf(p.references) =>
      p.copy(
        child = a.copy(aggregateExpressions = a.aggregateExpressions.filter(p.references.contains)))
    case a @ Project(_, e @ Expand(_, _, grandChild)) if !e.outputSet.subsetOf(a.references) =>
      val newOutput = e.output.filter(a.references.contains(_))
      val newProjects = e.projections.map { proj =>
        proj.zip(e.output).filter { case (_, a) =>
          newOutput.contains(a)
        }.unzip._1
      }
      a.copy(child = Expand(newProjects, newOutput, grandChild))

    // Prunes the unused columns from child of `DeserializeToObject`
    case d @ DeserializeToObject(_, _, child) if !child.outputSet.subsetOf(d.references) =>
      d.copy(child = prunedChild(child, d.references))

    // Prunes the unused columns from child of Aggregate/Expand/Generate/ScriptTransformation
    case a @ Aggregate(_, _, child) if !child.outputSet.subsetOf(a.references) =>
      a.copy(child = prunedChild(child, a.references))
    case f @ FlatMapGroupsInPandas(_, _, _, child) if !child.outputSet.subsetOf(f.references) =>
      f.copy(child = prunedChild(child, f.references))
    case e @ Expand(_, _, child) if !child.outputSet.subsetOf(e.references) =>
      e.copy(child = prunedChild(child, e.references))
    case s @ ScriptTransformation(_, _, _, child, _)
      if !child.outputSet.subsetOf(s.references) =>
      s.copy(child = prunedChild(child, s.references))

    // prune unrequired references
    case p @ Project(_, g: Generate) if p.references != g.outputSet =>
      val requiredAttrs = p.references -- g.producedAttributes ++ g.generator.references
      val newChild = prunedChild(g.child, requiredAttrs)
      val unrequired = g.generator.references -- p.references
      val unrequiredIndices = newChild.output.zipWithIndex.filter(t => unrequired.contains(t._1))
        .map(_._2)
      p.copy(child = g.copy(child = newChild, unrequiredChildIndex = unrequiredIndices))

    // prune unrequired nested fields
    case p @ Project(projectList, g: Generate) if SQLConf.get.nestedPruningOnExpressions &&
      NestedColumnAliasing.canPruneGenerator(g.generator) =>
      val exprsToPrune = projectList ++ g.generator.children
      NestedColumnAliasing.getAliasSubMap(exprsToPrune, g.qualifiedGeneratorOutput).map {
        case (nestedFieldToAlias, attrToAliases) =>
          val newGenerator = g.generator.transform {
            case f: ExtractValue if nestedFieldToAlias.contains(f) =>
              nestedFieldToAlias(f).toAttribute
          }.asInstanceOf[Generator]

          // Defer updating `Generate.unrequiredChildIndex` to next round of `ColumnPruning`.
          val newGenerate = g.copy(generator = newGenerator)

          val newChild = NestedColumnAliasing.replaceChildrenWithAliases(newGenerate, attrToAliases)

          Project(NestedColumnAliasing.getNewProjectList(projectList, nestedFieldToAlias), newChild)
      }.getOrElse(p)

    // Eliminate unneeded attributes from right side of a Left Existence Join.
    case j @ Join(_, right, LeftExistence(_), _) =>
      j.copy(right = prunedChild(right, j.references))

    // all the columns will be used to compare, so we can't prune them
    case p @ Project(_, _: SetOperation) => p
    case p @ Project(_, _: Distinct) => p
    // Eliminate unneeded attributes from children of Union.
    case p @ Project(_, u: Union) =>
      if (!u.outputSet.subsetOf(p.references)) {
        val firstChild = u.children.head
        val newOutput = prunedChild(firstChild, p.references).output
        // pruning the columns of all children based on the pruned first child.
        val newChildren = u.children.map { p =>
          val selected = p.output.zipWithIndex.filter { case (a, i) =>
            newOutput.contains(firstChild.output(i))
          }.map(_._1)
          Project(selected, p)
        }
        p.copy(child = u.withNewChildren(newChildren))
      } else {
        p
      }

    // Prune unnecessary window expressions
    case p @ Project(_, w: Window) if !w.windowOutputSet.subsetOf(p.references) =>
      p.copy(child = w.copy(
        windowExpressions = w.windowExpressions.filter(p.references.contains)))

    // Can't prune the columns on LeafNode
    case p @ Project(_, _: LeafNode) => p

    case p @ NestedColumnAliasing(nestedFieldToAlias, attrToAliases) =>
      NestedColumnAliasing.replaceToAliases(p, nestedFieldToAlias, attrToAliases)

    // for all other logical plans that inherits the output from it's children
    // Project over project is handled by the first case, skip it here.
    case p @ Project(_, child) if !child.isInstanceOf[Project] =>
      val required = child.references ++ p.references
      if (!child.inputSet.subsetOf(required)) {
        val newChildren = child.children.map(c => prunedChild(c, required))
        p.copy(child = child.withNewChildren(newChildren))
      } else {
        p
      }
  })

  /** Applies a projection only when the child is producing unnecessary attributes */
  private def prunedChild(c: LogicalPlan, allReferences: AttributeSet) =
    if (!c.outputSet.subsetOf(allReferences)) {
      Project(c.output.filter(allReferences.contains), c)
    } else {
      c
    }

  /**
   * The Project before Filter is not necessary but conflict with PushPredicatesThroughProject,
   * so remove it. Since the Projects have been added top-down, we need to remove in bottom-up
   * order, otherwise lower Projects can be missed.
   */
  private def removeProjectBeforeFilter(plan: LogicalPlan): LogicalPlan = plan transformUp {
    case p1 @ Project(_, f @ Filter(_, p2 @ Project(_, child)))
      if p2.outputSet.subsetOf(child.outputSet) &&
        // We only remove attribute-only project.
        p2.projectList.forall(_.isInstanceOf[AttributeReference]) =>
      p1.copy(child = f.copy(child = child))
  }
}

case class ColumnPruningJ1Rule(spark: SparkSession) extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = ColumnPruningBackport30.apply(plan)
}

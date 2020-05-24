/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.datasources

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LeafNode, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.ProjectionOverSchema
import org.apache.spark.sql.execution.datasources.orc.OrcFileFormat
import org.apache.spark.sql.execution.datasources.parquet.{ParquetFileFormat, SparkToParquetSchemaConverter}
import org.apache.spark.sql.expressions.SelectedField
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{ArrayType, DataType, MapType, StructField, StructType}

/**
  * Prunes unnecessary physical columns given a [[PhysicalOperation]] over a data source relation.
  * By "physical column", we mean a column as defined in the data source format like Parquet format
  * or ORC format. For example, in Spark SQL, a root-level Parquet column corresponds to a SQL
  * column, and a nested Parquet column corresponds to a [[StructField]].
  */
object SchemaPruningBackport30 extends Rule[LogicalPlan] {
//  import org.apache.spark.sql.catalyst.expressions.SchemaPruning._

  /**
   * Filters the schema by the requested fields. For example, if the schema is struct<a:int, b:int>,
   * and given requested field are "a", the field "b" is pruned in the returned schema.
   * Note that schema field ordering at original schema is still preserved in pruned schema.
   */
  def pruneDataSchema(
      dataSchema: StructType,
      requestedRootFields: Seq[RootField]): StructType = {
    // Merge the requested root fields into a single schema. Note the ordering of the fields
    // in the resulting schema may differ from their ordering in the logical relation's
    // original schema
    val mergedSchema = requestedRootFields
      .map { case root: RootField => StructType(Array(root.field)) }
      .reduceLeft(_ merge _)
    val dataSchemaFieldNames = dataSchema.fieldNames.toSet
    val mergedDataSchema =
      StructType(mergedSchema.filter(f => dataSchemaFieldNames.contains(f.name)))
    // Sort the fields of mergedDataSchema according to their order in dataSchema,
    // recursively. This makes mergedDataSchema a pruned schema of dataSchema
    sortLeftFieldsByRight(mergedDataSchema, dataSchema).asInstanceOf[StructType]
  }

  /**
   * Sorts the fields and descendant fields of structs in left according to their order in
   * right. This function assumes that the fields of left are a subset of the fields of
   * right, recursively. That is, left is a "subschema" of right, ignoring order of
   * fields.
   */
  private def sortLeftFieldsByRight(left: DataType, right: DataType): DataType =
    (left, right) match {
      case (ArrayType(leftElementType, containsNull), ArrayType(rightElementType, _)) =>
        ArrayType(
          sortLeftFieldsByRight(leftElementType, rightElementType),
          containsNull)
      case (MapType(leftKeyType, leftValueType, containsNull),
          MapType(rightKeyType, rightValueType, _)) =>
        MapType(
          sortLeftFieldsByRight(leftKeyType, rightKeyType),
          sortLeftFieldsByRight(leftValueType, rightValueType),
          containsNull)
      case (leftStruct: StructType, rightStruct: StructType) =>
        val filteredRightFieldNames = rightStruct.fieldNames.filter(leftStruct.fieldNames.contains)
        val sortedLeftFields = filteredRightFieldNames.map { fieldName =>
          val leftFieldType = leftStruct(fieldName).dataType
          val rightFieldType = rightStruct(fieldName).dataType
          val sortedLeftFieldType = sortLeftFieldsByRight(leftFieldType, rightFieldType)
          StructField(fieldName, sortedLeftFieldType, nullable = leftStruct(fieldName).nullable)
        }
        StructType(sortedLeftFields)

      // backport 3.0 - new case condition
      case (leftStruct: StructType, rightStructType: ArrayType) =>
        val rightStruct = rightStructType.elementType.asInstanceOf[StructType]
        val filteredRightFieldNames = rightStruct.fieldNames.filter(leftStruct.fieldNames.contains)
        val sortedLeftFields = filteredRightFieldNames.map { fieldName =>
          val leftFieldType = leftStruct(fieldName).dataType
          val rightFieldType = rightStruct(fieldName).dataType
          val sortedLeftFieldType = sortLeftFieldsByRight(leftFieldType, rightFieldType)
          StructField(fieldName, sortedLeftFieldType, nullable = leftStruct(fieldName).nullable)
        }
        ArrayType(StructType(sortedLeftFields))
      case _ => left
    }

  /**
   * Returns the set of fields from projection and filtering predicates that the query plan needs.
   */
  def identifyRootFields(
      projects: Seq[NamedExpression],
      filters: Seq[Expression]): Seq[RootField] = {
    val projectionRootFields = projects.flatMap(getRootFields)
    val filterRootFields = filters.flatMap(getRootFields)

    // Kind of expressions don't need to access any fields of a root fields, e.g., `IsNotNull`.
    // For them, if there are any nested fields accessed in the query, we don't need to add root
    // field access of above expressions.
    // For example, for a query `SELECT name.first FROM contacts WHERE name IS NOT NULL`,
    // we don't need to read nested fields of `name` struct other than `first` field.
    val (rootFields, optRootFields) = (projectionRootFields ++ filterRootFields)
      .distinct.partition(!_.prunedIfAnyChildAccessed)

    optRootFields.filter { opt =>
      !rootFields.exists { root =>
        root.field.name == opt.field.name && {
          // Checking if current optional root field can be pruned.
          // For each required root field, we merge it with the optional root field:
          // 1. If this optional root field has nested fields and any nested field of it is used
          //    in the query, the merged field type must equal to the optional root field type.
          //    We can prune this optional root field. For example, for optional root field
          //    `struct<name:struct<middle:string,last:string>>`, if its field
          //    `struct<name:struct<last:string>>` is used, we don't need to add this optional
          //    root field.
          // 2. If this optional root field has no nested fields, the merged field type equals
          //    to the optional root field only if they are the same. If they are, we can prune
          //    this optional root field too.
          val rootFieldType = StructType(Array(root.field))
          val optFieldType = StructType(Array(opt.field))
          val merged = optFieldType.merge(rootFieldType)
          merged.sameType(optFieldType)
        }
      }
    } ++ rootFields
  }

  /**
   * Gets the root (aka top-level, no-parent) [[StructField]]s for the given [[Expression]].
   * When expr is an [[Attribute]], construct a field around it and indicate that that
   * field was derived from an attribute.
   */
  private def getRootFields(expr: Expression): Seq[RootField] = {
    expr match {
      case att: Attribute =>
        RootField(StructField(att.name, att.dataType, att.nullable), derivedFromAtt = true) :: Nil
      case SelectedField(field) => RootField(field, derivedFromAtt = false) :: Nil
      // Root field accesses by `IsNotNull` and `IsNull` are special cases as the expressions
      // don't actually use any nested fields. These root field accesses might be excluded later
      // if there are any nested fields accesses in the query plan.
      case IsNotNull(SelectedField(field)) =>
        RootField(field, derivedFromAtt = false, prunedIfAnyChildAccessed = true) :: Nil
      case IsNull(SelectedField(field)) =>
        RootField(field, derivedFromAtt = false, prunedIfAnyChildAccessed = true) :: Nil
      case IsNotNull(_: Attribute) | IsNull(_: Attribute) =>
        expr.children.flatMap(getRootFields).map(_.copy(prunedIfAnyChildAccessed = true))
      case _ =>
        expr.children.flatMap(getRootFields)
    }
  }

  /**
   * This represents a "root" schema field (aka top-level, no-parent). `field` is the
   * `StructField` for field name and datatype. `derivedFromAtt` indicates whether it
   * was derived from an attribute or had a proper child. `prunedIfAnyChildAccessed` means
   * whether this root field can be pruned if any of child field is used in the query.
   */
  case class RootField(field: StructField, derivedFromAtt: Boolean,
    prunedIfAnyChildAccessed: Boolean = false)

  override def apply(plan: LogicalPlan): LogicalPlan =
    if (SQLConf.get.nestedSchemaPruningEnabled) {
      val plan2 = apply0(plan)
      plan2
    } else {
      plan
    }

  private def apply0(plan: LogicalPlan): LogicalPlan = {

//    if(plan.isInstanceOf[Filter])
//      return plan

    plan transformDown {
      case op @ PhysicalOperation(projects, filters,
      l @ LogicalRelation(hadoopFsRelation: HadoopFsRelation, _, _, _))
        if canPruneRelation(hadoopFsRelation) =>

        val projectList : Seq[NamedExpression] =
          if (plan.isInstanceOf[Project]) {
            plan.asInstanceOf[Project].projectList
          } else {
            projects
          }

        val newPlan = prunePhysicalColumns(l.output, projects, filters, hadoopFsRelation.dataSchema,
          prunedDataSchema => {
            val prunedHadoopRelation =
              hadoopFsRelation.copy(dataSchema = prunedDataSchema)(hadoopFsRelation.sparkSession)
            buildPrunedRelation(l, prunedHadoopRelation)
          }, projectList).getOrElse(op)
        newPlan
    }
  }

  private def prunePhysicalColumns(output: Seq[AttributeReference], projects: Seq[NamedExpression],
                                   filters: Seq[Expression], dataSchema: StructType,
                                   leafNodeBuilder: StructType => LeafNode, projectList : Seq[NamedExpression]): Option[LogicalPlan] =
  {
    val (normalizedProjects, normalizedFilters) =
      normalizeAttributeRefNames(output, projects, filters)
    val (normalizedProjects2, normalizedFilters2) =
      normalizeAttributeRefNames(output, projectList, filters)

    val requestedRootFields = identifyRootFields(normalizedProjects, normalizedFilters)
    val requestedRootFields2 = identifyRootFields(normalizedProjects2, normalizedFilters)

    // If requestedRootFields includes a nested field, continue. Otherwise,
    // return op
    if (requestedRootFields.exists { root: RootField => !root.derivedFromAtt }) {
      val prunedDataSchema2 = pruneDataSchema(dataSchema, requestedRootFields2)
      val prunedDataSchema = pruneDataSchema(dataSchema, requestedRootFields)

      if (countLeaves(dataSchema) > countLeaves(prunedDataSchema2)) {
        val projectionOverSchema3 = ProjectionOverSchema(prunedDataSchema2)
        val prunedRelation3 = leafNodeBuilder(prunedDataSchema2)
        Some(buildNewProjection(normalizedProjects, normalizedFilters, prunedRelation3, projectionOverSchema3))
      }

      // If the data schema is different from the pruned data schema, continue. Otherwise,
      // return op. We effect this comparison by counting the number of "leaf" fields in
      // each schemata, assuming the fields in prunedDataSchema are a subset of the fields
      // in dataSchema.SchemaPruning
      else if (countLeaves(dataSchema) > countLeaves(prunedDataSchema)) {
        val prunedRelation = leafNodeBuilder(prunedDataSchema)
        val projectionOverSchema = ProjectionOverSchema(prunedDataSchema)

        Some(buildNewProjection(normalizedProjects, normalizedFilters, prunedRelation,
          projectionOverSchema))
      } else {
        None
      }
    } else {
      None
    }
  }


  /**
   * Checks to see if the given relation can be pruned. Currently we support Parquet and ORC v1.
   */
  private def canPruneRelation(fsRelation: HadoopFsRelation) =
    fsRelation.fileFormat.isInstanceOf[ParquetFileFormat] ||
      fsRelation.fileFormat.isInstanceOf[OrcFileFormat]

  /**
   * Normalizes the names of the attribute references in the given projects and filters to reflect
   * the names in the given logical relation. This makes it possible to compare attributes and
   * fields by name. Returns a tuple with the normalized projects and filters, respectively.
   */
  private def normalizeAttributeRefNames(
                                          output: Seq[AttributeReference],
                                          projects: Seq[NamedExpression],
                                          filters: Seq[Expression]): (Seq[NamedExpression], Seq[Expression]) = {
    val normalizedAttNameMap = output.map(att => (att.exprId, att.name)).toMap
    val normalizedProjects = projects.map(_.transform {
      case att: AttributeReference if normalizedAttNameMap.contains(att.exprId) =>
        att.withName(normalizedAttNameMap(att.exprId))
    }).map { case expr: NamedExpression => expr }
    val normalizedFilters = filters.map(_.transform {
      case att: AttributeReference if normalizedAttNameMap.contains(att.exprId) =>
        att.withName(normalizedAttNameMap(att.exprId))
    })
    (normalizedProjects, normalizedFilters)
  }

  /**
   * Builds the new output [[Project]] Spark SQL operator that has the `leafNode`.
   */
  private def buildNewProjection(
                                  projects: Seq[NamedExpression],
                                  filters: Seq[Expression],
                                  leafNode: LeafNode,
                                  projectionOverSchema: ProjectionOverSchema): Project = {
    // Construct a new target for our projection by rewriting and
    // including the original filters where available
    val projectionChild =
    if (filters.nonEmpty) {
      val projectedFilters = filters.map(_.transformDown {
        case projectionOverSchema(expr) => expr
      })
      val newFilterCondition = projectedFilters.reduce(And)
      Filter(newFilterCondition, leafNode)
    } else {
      leafNode
    }

    // Construct the new projections of our Project by
    // rewriting the original projections
    val newProjects = projects.map(_.transformDown {
      case projectionOverSchema(expr) => expr
    }).map { case expr: NamedExpression => expr }

    if (log.isDebugEnabled) {
      logDebug(s"New projects:\n${newProjects.map(_.treeString).mkString("\n")}")
    }

    Project(newProjects, projectionChild)
  }

  /**
   * Builds a pruned logical relation from the output of the output relation and the schema of the
   * pruned base relation.
   */
  private def buildPrunedRelation(
                                   outputRelation: LogicalRelation,
                                   prunedBaseRelation: HadoopFsRelation) = {
    val prunedOutput = getPrunedOutput(outputRelation.output, prunedBaseRelation.schema)
    outputRelation.copy(relation = prunedBaseRelation, output = prunedOutput)
  }

  // Prune the given output to make it consistent with `requiredSchema`.
  private def getPrunedOutput(
                               output: Seq[AttributeReference],
                               requiredSchema: StructType): Seq[AttributeReference] = {
    // We need to replace the expression ids of the pruned relation output attributes
    // with the expression ids of the original relation output attributes so that
    // references to the original relation's output are not broken
    val outputIdMap = output.map(att => (att.name, att.exprId)).toMap
    requiredSchema
      .toAttributes
      .map {
        case att if outputIdMap.contains(att.name) =>
          att.withExprId(outputIdMap(att.name))
        case att => att
      }
  }

  /**
   * Counts the "leaf" fields of the given dataType. Informally, this is the
   * number of fields of non-complex data type in the tree representation of
   * [[DataType]].
   */
  private def countLeaves(dataType: DataType): Int = {
    dataType match {
      case array: ArrayType => countLeaves(array.elementType)
      case map: MapType => countLeaves(map.keyType) + countLeaves(map.valueType)
      case struct: StructType =>
        struct.map(field => countLeaves(field.dataType)).sum
      case _ => 1
    }
  }


}

/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.filters;

import io.openlineage.spark.api.OpenLineageContext;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.sql.catalyst.plans.logical.Aggregate;
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.Project;
import org.apache.spark.sql.catalyst.plans.logical.Repartition;
import org.apache.spark.sql.execution.command.CreateDatabaseCommand;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static io.openlineage.spark.agent.filters.EventFilterUtils.getLogicalPlan;

/** If a root node of an Spark action is one of defined nodes, we should filter it */
public class SparkNodesFilter implements EventFilter {
  private final OpenLineageContext context;

  private final static List<String> filterNodes =
      Arrays.asList(
          "org.apache.spark.sql.catalyst.plans.logical.ShowTables",
          "org.apache.spark.sql.catalyst.plans.logical.CreateNamespace",
          "org.apache.spark.sql.catalyst.plans.logical.SetCatalogAndNamespace",
          Project.class.getCanonicalName(),
          Aggregate.class.getCanonicalName(),
          Repartition.class.getCanonicalName(),
          CreateDatabaseCommand.class.getCanonicalName(),
          LocalRelation.class.getCanonicalName());

  public SparkNodesFilter(OpenLineageContext context) {
    this.context = context;
  }

  @Override
  public boolean isDisabled(SparkListenerEvent event) {
    return getLogicalPlan(context)
        .filter(this::filterNode)
        .isPresent();
  }

  private boolean filterNode(LogicalPlan plan) {
    if (plan.isStreaming()) {
      return filterNodes.stream().
              filter(node -> !node.equals(Project.class.getCanonicalName())).
              collect(Collectors.toList()).
              contains(plan.getClass().getCanonicalName());
    }

    return filterNodes.contains(plan.getClass().getCanonicalName());
  }
}

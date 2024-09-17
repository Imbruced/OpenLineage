/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.api.AbstractQueryPlanInputDatasetBuilder;
import io.openlineage.spark.api.OpenLineageContext;
import java.util.Collections;
import java.util.List;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.Project;
import org.apache.spark.sql.catalyst.plans.logical.View;

public class ViewVisitor extends AbstractQueryPlanInputDatasetBuilder<View> {

  @Override
  public boolean isDefinedAtLogicalPlan(LogicalPlan x) {
    return x instanceof View && ((View) x).child() != null;
  }

  public ViewVisitor(OpenLineageContext context) {
    super(context, true);
  }

  @Override
  protected List<OpenLineage.InputDataset> apply(SparkListenerEvent event, View x) {
    LogicalPlan child = x.child();

    if (child instanceof Project) {
      return delegate(((Project) child).child(), event);
    }

    return Collections.emptyList();
  }
}

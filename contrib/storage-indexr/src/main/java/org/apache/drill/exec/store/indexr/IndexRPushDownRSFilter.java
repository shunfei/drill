/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.store.indexr;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rex.RexNode;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.planner.logical.DrillOptiq;
import org.apache.drill.exec.planner.logical.DrillParseContext;
import org.apache.drill.exec.planner.logical.RelOptHelper;
import org.apache.drill.exec.planner.physical.FilterPrel;
import org.apache.drill.exec.planner.physical.PrelUtil;
import org.apache.drill.exec.planner.physical.ProjectPrel;
import org.apache.drill.exec.planner.physical.ScanPrel;
import org.apache.drill.exec.store.StoragePluginOptimizerRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.indexr.segment.rc.RCOperator;

public class IndexRPushDownRSFilter {
  private static final Logger log = LoggerFactory.getLogger(IndexRPushDownRSFilter.class);

  private static void setRSFilter(RelOptRuleCall call, ScanPrel scan, RexNode condition) {
    GroupScan gs = scan.getGroupScan();
    if (gs == null || !(gs instanceof IndexRGroupScan)) {
      return;
    }

    IndexRGroupScan groupScan = (IndexRGroupScan) gs;
    if (groupScan.getScanSpec().getRSFilter() != null) {
      return;
    }

    LogicalExpression conditionExp = DrillOptiq.toDrill(new DrillParseContext(PrelUtil.getPlannerSettings(call.getPlanner())), scan, condition);
    RSFilterGenerator generator = new RSFilterGenerator(groupScan, conditionExp);
    RCOperator rsFilter = generator.rsFilter();
    log.debug("================= rsFilter:" + rsFilter);
    groupScan.getScanSpec().setRSFilter(rsFilter);
  }

  public static StoragePluginOptimizerRule MatchFilterScan = new StoragePluginOptimizerRule(RelOptHelper.some(FilterPrel.class, RelOptHelper.any(ScanPrel.class)), "IndexRFilterScan") {
    @Override
    public void onMatch(RelOptRuleCall call) {
      FilterPrel filter = (FilterPrel) call.rel(0);
      ScanPrel scan = (ScanPrel) call.rel(1);
      RexNode condition = filter.getCondition();

      setRSFilter(call, scan, condition);
    }
  };

  public static StoragePluginOptimizerRule MatchFilterProjectScan = new StoragePluginOptimizerRule(RelOptHelper.some(FilterPrel.class, RelOptHelper.some(ProjectPrel.class, RelOptHelper.any(ScanPrel.class))), "IndexRFilterProjectScan") {
    @Override
    public void onMatch(RelOptRuleCall call) {
      FilterPrel filter = (FilterPrel) call.rel(0);
      ProjectPrel project = (ProjectPrel) call.rel(1);
      ScanPrel scan = (ScanPrel) call.rel(2);
      RexNode condition = RelOptUtil.pushFilterPastProject(filter.getCondition(), project);

      setRSFilter(call, scan, condition);
    }
  };
}

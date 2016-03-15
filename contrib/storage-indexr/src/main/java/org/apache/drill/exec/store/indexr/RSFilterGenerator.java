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

import org.apache.drill.common.expression.BooleanOperator;
import org.apache.drill.common.expression.ExpressionStringBuilder;
import org.apache.drill.common.expression.FunctionCall;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.expression.visitors.AbstractExprVisitor;

import java.util.ArrayList;
import java.util.List;

import io.indexr.segment.rc.And;
import io.indexr.segment.rc.Attr;
import io.indexr.segment.rc.Equal;
import io.indexr.segment.rc.Greater;
import io.indexr.segment.rc.GreaterEqual;
import io.indexr.segment.rc.Less;
import io.indexr.segment.rc.LessEqual;
import io.indexr.segment.rc.Not;
import io.indexr.segment.rc.NotEqual;
import io.indexr.segment.rc.Or;
import io.indexr.segment.rc.RCOperator;
import io.indexr.segment.rc.UnknownOperator;

public class RSFilterGenerator extends AbstractExprVisitor<RCOperator, Void, RuntimeException> {
  private IndexRGroupScan groupScan;
  private LogicalExpression conditionExpr;

  private CmpOpProcessor processor = new CmpOpProcessor();

  public RSFilterGenerator(IndexRGroupScan groupScan, LogicalExpression conditionExpr) {
    this.groupScan = groupScan;
    this.conditionExpr = conditionExpr;
  }

  public RCOperator rsFilter() {
    RCOperator rsFilter = conditionExpr.accept(this, null);
    rsFilter = rsFilter.optimize();
    return rsFilter;
  }

  @Override
  public RCOperator visitUnknown(LogicalExpression e, Void value) throws RuntimeException {
    return new UnknownOperator(ExpressionStringBuilder.toString(e));
  }

  @Override
  public RCOperator visitBooleanOperator(BooleanOperator op, Void value) throws RuntimeException {
    List<LogicalExpression> args = op.args;
    String functionName = op.getName();
    List<RCOperator> children = new ArrayList<>(args.size());
    for (LogicalExpression expression : args) {
      children.add(expression.accept(this, null));
    }
    switch (functionName) {
      case "booleanAnd":
        return new And(children);
      case "booleanOr":
        return new Or(children);
      default:
        return new UnknownOperator(ExpressionStringBuilder.toString(op));
    }
  }

  @Override
  public RCOperator visitFunctionCall(FunctionCall call, Void value) throws RuntimeException {
    if ("not".equals(call.getName())) {
      return new Not(call.args.get(0).accept(this, null));
    }
    return createCmpOperator(call);
  }

  private RCOperator createCmpOperator(FunctionCall call) {
    if (!processor.process(call)) {
      return new UnknownOperator(ExpressionStringBuilder.toString(call));
    }
    RCOperator operator;
    String fieldName = processor.getPath().getAsUnescapedPath();
    switch (processor.getFunctionName()) {
      case "equal": {
        operator = new Equal(
            genAttr(processor.getPath()),
            processor.getNumValue(),
            processor.getStrValue());
        break;
      }
      case "not_equal": {
        operator = new NotEqual(
            genAttr(processor.getPath()),
            processor.getNumValue(),
            processor.getStrValue());
        break;
      }
      case "greater_than_or_equal_to": {
        operator = new GreaterEqual(
            genAttr(processor.getPath()),
            processor.getNumValue(),
            processor.getStrValue());
        break;
      }
      case "greater_than": {
        operator = new Greater(
            genAttr(processor.getPath()),
            processor.getNumValue(),
            processor.getStrValue());
        break;
      }
      case "less_than_or_equal_to": {
        operator = new LessEqual(
            genAttr(processor.getPath()),
            processor.getNumValue(),
            processor.getStrValue());
        break;
      }
      case "less_than": {
        operator = new Less(
            genAttr(processor.getPath()),
            processor.getNumValue(),
            processor.getStrValue());
        break;
      }
      default:
        operator = new UnknownOperator(ExpressionStringBuilder.toString(call));
        break;
    }
    if (processor.isSwitchDirection()) {
      operator = operator.switchDirection();
    }
    return operator;
  }

  private Attr genAttr(SchemaPath path) {
    return new Attr(DrillIndexRTable.toColName(groupScan.getScanSpec().getTableName(), path));
  }
}

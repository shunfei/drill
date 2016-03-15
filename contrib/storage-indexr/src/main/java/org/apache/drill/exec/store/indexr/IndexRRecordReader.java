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

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.store.AbstractRecordReader;
import org.apache.drill.exec.vector.ValueVector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import io.indexr.segment.ColumnSchema;
import io.indexr.segment.SegmentSchema;
import io.indexr.segment.pack.DataPack;
import io.indexr.util.Pair;

public abstract class IndexRRecordReader extends AbstractRecordReader {
  static final int MAX_ROW_COUNT_PER_STEP = DataPack.MAX_COUNT;
  private static final Logger log = LoggerFactory.getLogger(IndexRRecordReader.class);

  final String tableName;
  final SegmentSchema schema;

  ProjectedColumnInfo[] projectedColumnInfos;

  static class ProjectedColumnInfo {
    ColumnSchema columnSchema;
    ValueVector valueVector;

    public ProjectedColumnInfo(ColumnSchema columnSchema, ValueVector valueVector) {
      this.columnSchema = columnSchema;
      this.valueVector = valueVector;
    }
  }

  IndexRRecordReader(String tableName, //
                     SegmentSchema schema, //
                     List<SchemaPath> projectColumns) {
    this.tableName = tableName;
    this.schema = schema;

    setColumns(projectColumns);
  }

  @SuppressWarnings("unchecked")
  private ProjectedColumnInfo genPCI(ColumnSchema columnSchema, OutputMutator output) {
    TypeProtos.MinorType minorType = DrillIndexRTable.parseMinorType(columnSchema.dataType);
    TypeProtos.MajorType majorType = Types.required(minorType);
    MaterializedField field = MaterializedField.create(columnSchema.name, majorType);
    final Class<? extends ValueVector> clazz = (Class<? extends ValueVector>) TypeHelper.getValueVectorClass(minorType, majorType.getMode());
    ValueVector vector = null;
    try {
      vector = output.addField(field, clazz);
    } catch (SchemaChangeException e) {
      throw new RuntimeException(e);
    }
    vector.allocateNew();
    return new ProjectedColumnInfo(columnSchema, vector);
  }

  @Override
  @SuppressWarnings("unchecked")
  public void setup(OperatorContext context, OutputMutator output) throws ExecutionSetupException {
    List<ColumnSchema> schemas = schema.columns;
    if (isStarQuery()) {
      projectedColumnInfos = new ProjectedColumnInfo[schemas.size()];
      int columnId = 0;
      for (ColumnSchema cs : schemas) {
        projectedColumnInfos[columnId] = genPCI(cs, output);
        columnId++;
      }
    } else {
      projectedColumnInfos = new ProjectedColumnInfo[this.getColumns().size()];
      int count = 0;
      for (SchemaPath schemaPath : this.getColumns()) {
        Pair<ColumnSchema, Integer> p = DrillIndexRTable.mapColumn(tableName, schema, schemaPath);
        if (p == null) {
          throw new RuntimeException(String.format("Column not found! SchemaPath: %s, search segment schema: %s", schemaPath, schemas));
        }
        projectedColumnInfos[count] = genPCI(p.first, output);
        count++;
      }
    }
  }
}

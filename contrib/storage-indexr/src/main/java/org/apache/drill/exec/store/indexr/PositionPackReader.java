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

import com.google.common.base.Preconditions;

import org.apache.drill.exec.vector.BigIntVector;
import org.apache.drill.exec.vector.DateVector;
import org.apache.drill.exec.vector.Float4Vector;
import org.apache.drill.exec.vector.Float8Vector;
import org.apache.drill.exec.vector.IntVector;
import org.apache.drill.exec.vector.TimeStampVector;
import org.apache.drill.exec.vector.TimeVector;
import org.apache.drill.exec.vector.VarCharVector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;

import io.indexr.data.BytePiece;
import io.indexr.data.BytePieceSetter;
import io.indexr.data.DoubleSetter;
import io.indexr.data.FloatSetter;
import io.indexr.data.IntSetter;
import io.indexr.data.LongSetter;
import io.indexr.segment.Column;
import io.indexr.segment.ColumnSchema;
import io.indexr.segment.SQLType;
import io.indexr.segment.pack.DataPack;
import io.indexr.segment.pack.DataPackNode;
import io.indexr.segment.storage.CachedSegment;
import io.indexr.util.BitMap;
import io.indexr.util.MemoryUtil;

class PositionPackReader implements PackReader {
  private static final Logger logger = LoggerFactory.getLogger(DefaultPackReader.class);

  private CachedSegment segment;
  private ProjectedColumnInfo[] projectInfos;
  private int[] projColIds;
  private int[] projColInfoIndies;

  private int packId;
  private int packRowCount;
  private BitMap position;

  PositionPackReader(CachedSegment segment,
                     int packId,
                     ProjectedColumnInfo[] projectInfos,
                     int[] projColIds,
                     int[] projColInfoIndies,
                     BitMap position) throws IOException {
    int packRowCount = -1;
    int projectSizePerRow = 0;
    for (int projectId = 0; projectId < projectInfos.length; projectId++) {
      int columnId = projColIds[projectId];
      int projectIndex = projColInfoIndies[projectId];

      ColumnSchema cs = projectInfos[projectIndex].columnSchema;
      Column column = segment.column(columnId);
      DataPackNode dpn = column.dpn(packId);

      assert packRowCount == -1 || packRowCount == dpn.objCount();

      packRowCount = dpn.objCount();
      projectSizePerRow += Math.ceil(((double) dpn.objCount()) / packRowCount);
    }

    this.projectInfos = projectInfos;
    this.projColIds = projColIds;
    this.projColInfoIndies = projColInfoIndies;

    this.segment = segment;
    this.packId = packId;
    this.packRowCount = packRowCount;
    this.position = position;

    logger.debug(
        "packId:{}, packRowCount:{}, stepRowCount:{}, rowOffset:{}",
        packId, packRowCount, DataPack.MAX_COUNT, 0);
  }

  @Override
  public void clear() throws IOException {
    if (segment != null) {
      segment.free();
      segment = null;
      packId = -1;
      packRowCount = -1;
      position.free();
      position = null;
    }
  }

  @Override
  public boolean hasMore() {
    return segment != null;
  }

  @Override
  public int read() throws IOException {
    Preconditions.checkState(hasMore());

    int count = -1;
    for (int projectId = 0; projectId < projectInfos.length; projectId++) {
      int projectIndex = projColInfoIndies[projectId];
      ProjectedColumnInfo projectInfo = projectInfos[projectIndex];
      int columnId = projColIds[projectId];

      DataPack dataPack = segment.column(columnId).pack(packId);
      SQLType sqlType = projectInfo.columnSchema.getSqlType();
      count = doRead(dataPack, sqlType, projectInfo, position);
    }

    clear();

    return count;
  }

  private static int doRead(DataPack dataPack, SQLType sqlType, ProjectedColumnInfo projectInfo, BitMap position) {
    switch (sqlType) {
      case INT: {
        IntVector.Mutator mutator = (IntVector.Mutator) projectInfo.valueVector.getMutator();
        // Force the vector to allocate engough space.
        mutator.setSafe(dataPack.valueCount() - 1, 0);
        return dataPack.foreach(position, new IntSetter() {
          int index = 0;

          @Override
          public void set(int id, int value) {
            mutator.set(index++, value);
          }
        });
      }
      case BIGINT: {
        BigIntVector.Mutator mutator = (BigIntVector.Mutator) projectInfo.valueVector.getMutator();
        mutator.setSafe(dataPack.valueCount() - 1, 0);
        return dataPack.foreach(position, new LongSetter() {
          int index = 0;

          @Override
          public void set(int id, long value) {
            mutator.set(index++, value);
          }
        });
      }
      case FLOAT: {
        Float4Vector.Mutator mutator = (Float4Vector.Mutator) projectInfo.valueVector.getMutator();
        mutator.setSafe(dataPack.valueCount() - 1, 0);
        return dataPack.foreach(position, new FloatSetter() {
          int index = 0;

          @Override
          public void set(int id, float value) {
            mutator.set(index++, value);
          }
        });
      }
      case DOUBLE: {
        Float8Vector.Mutator mutator = (Float8Vector.Mutator) projectInfo.valueVector.getMutator();
        mutator.setSafe(dataPack.valueCount() - 1, 0);
        return dataPack.foreach(position, new DoubleSetter() {
          int index = 0;

          @Override
          public void set(int id, double value) {
            mutator.set(index++, value);
          }
        });
      }
      case DATE: {
        DateVector.Mutator mutator = (DateVector.Mutator) projectInfo.valueVector.getMutator();
        mutator.setSafe(dataPack.valueCount() - 1, 0);
        return dataPack.foreach(position, new LongSetter() {
          int index = 0;

          @Override
          public void set(int id, long value) {
            mutator.set(index++, value);
          }
        });
      }
      case TIME: {
        TimeVector.Mutator mutator = (TimeVector.Mutator) projectInfo.valueVector.getMutator();
        mutator.setSafe(dataPack.valueCount() - 1, 0);
        return dataPack.foreach(position, new IntSetter() {
          int index = 0;

          @Override
          public void set(int id, int value) {
            mutator.set(index++, value);
          }
        });
      }
      case DATETIME: {
        TimeStampVector.Mutator mutator = (TimeStampVector.Mutator) projectInfo.valueVector.getMutator();
        mutator.setSafe(dataPack.valueCount() - 1, 0);
        return dataPack.foreach(position, new LongSetter() {
          int index = 0;

          @Override
          public void set(int id, long value) {
            mutator.set(index++, value);
          }
        });
      }
      case VARCHAR: {
        ByteBuffer byteBuffer = MemoryUtil.getHollowDirectByteBuffer();
        VarCharVector.Mutator mutator = (VarCharVector.Mutator) projectInfo.valueVector.getMutator();
        return dataPack.foreach(position, new BytePieceSetter() {
          int index = 0;

          @Override
          public void set(int id, BytePiece bytes) {
            assert bytes.base == null;
            MemoryUtil.setByteBuffer(byteBuffer, bytes.addr, bytes.len, null);
            mutator.setSafe(index++, byteBuffer, 0, byteBuffer.remaining());
          }
        });
      }
      default:
        throw new IllegalStateException(String.format("Unsupported date type %s", projectInfo.columnSchema.getSqlType()));
    }
  }
}

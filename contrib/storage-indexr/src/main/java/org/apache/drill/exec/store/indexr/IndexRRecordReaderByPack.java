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

import org.apache.commons.io.IOUtils;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.vector.BaseDataValueVector;
import org.apache.drill.exec.vector.BigIntVector;
import org.apache.drill.exec.vector.Float4Vector;
import org.apache.drill.exec.vector.Float8Vector;
import org.apache.drill.exec.vector.IntVector;
import org.apache.drill.exec.vector.UInt4Vector;
import org.apache.drill.exec.vector.VarCharVector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.indexr.data.BytePiece;
import io.indexr.data.DoubleSetter;
import io.indexr.data.FloatSetter;
import io.indexr.data.IntSetter;
import io.indexr.data.LongSetter;
import io.indexr.io.ByteSlice;
import io.indexr.segment.ColumnType;
import io.indexr.segment.Segment;
import io.indexr.segment.SegmentSchema;
import io.indexr.segment.helper.SegmentOpener;
import io.indexr.segment.helper.SingleWork;
import io.indexr.segment.pack.DataPack;
import io.indexr.segment.pack.Version;
import io.indexr.util.MemoryUtil;
import io.netty.buffer.DrillBuf;

public class IndexRRecordReaderByPack extends IndexRRecordReader {
  private static final Logger log = LoggerFactory.getLogger(IndexRRecordReaderByPack.class);

  private SegmentOpener segmentOpener;
  private List<SingleWork> works;
  private int curStepId = 0;

  private Map<String, Segment> segmentMap;

  public IndexRRecordReaderByPack(String tableName,//
                                  SegmentSchema schema,//
                                  List<SchemaPath> projectColumns,//
                                  SegmentOpener segmentOpener,//
                                  List<SingleWork> works) {
    super(tableName, schema, projectColumns);
    this.segmentOpener = segmentOpener;
    this.works = works;
  }

  @Override
  public void setup(OperatorContext context, OutputMutator output) throws ExecutionSetupException {
    super.setup(context, output);
    this.segmentMap = new HashMap<>();
    try {
      for (SingleWork work : works) {
        if (!segmentMap.containsKey(work.segment())) {
          Segment segment = segmentOpener.open(work.segment());
          // Check segment column here.
          for (ProjectedColumnInfo info : projectedColumnInfos) {
            Integer columnId = DrillIndexRTable.mapColumn(info.columnSchema, segment.schema());
            if (columnId == null) {
              throw new IllegalStateException(
                  String.format("segment[%s]: column %s not found in %s",
                      segment.name(), info.columnSchema, segment.schema()));
            }
          }

          segmentMap.put(work.segment(), segment);
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public int next() {
    try {
      int read = -1;
      while (read < 0) {
        if (curStepId >= works.size()) {
          return 0;
        }

        SingleWork stepWork = works.get(curStepId);
        curStepId++;

        Segment segment = segmentMap.get(stepWork.segment());
        read = read(segment, stepWork.packId());
      }
      return read;
    } catch (Throwable t) {
      // No matter or what, don't thrown exception from here.
      // It will break the Drill algorithm and make system unpredictable.
      // I do think Drill should handle this...

      log.error("Read rows error, query may return incorrect result.", t);
      return 0;
    }
  }

  private int read(Segment segment, int packId) {
    int read = -1;
    for (ProjectedColumnInfo info : projectedColumnInfos) {
      Integer columnId = DrillIndexRTable.mapColumn(info.columnSchema, segment.schema());
      if (columnId == null) {
        log.error("segment[{}]: column {} not found in {}",
            segment.name(), info.columnSchema, segment.schema());
        return -1;
      }
      byte dataType = info.columnSchema.dataType;
      DataPack dataPack = null;
      try {
        dataPack = (DataPack) segment.column(columnId).pack(packId);
      } catch (IOException e) {
        log.error("open {}", segment.name(), e);
        return -1;
      }
      int count = dataPack.count();
      if (count == 0) {
        log.warn("segment[{}]: found empty pack, packId: [{}]", segment.name(), packId);
        return 0;
      }
      if (read == -1) {
        read = count;
      }
      assert read == count;

      if (dataPack.version() == Version.VERSION_0_ID) {
        switch (dataType) {
          case ColumnType.INT: {
            IntVector.Mutator mutator = (IntVector.Mutator) info.valueVector.getMutator();
            // Force the vector to allocate engough space.
            mutator.setSafe(count - 1, 0);
            dataPack.foreach(0, count, (IntSetter) mutator::set);
            break;
          }
          case ColumnType.LONG: {
            BigIntVector.Mutator mutator = (BigIntVector.Mutator) info.valueVector.getMutator();
            mutator.setSafe(count - 1, 0);
            dataPack.foreach(0, count, (LongSetter) mutator::set);
            break;
          }
          case ColumnType.FLOAT: {
            Float4Vector.Mutator mutator = (Float4Vector.Mutator) info.valueVector.getMutator();
            mutator.setSafe(count - 1, 0);
            dataPack.foreach(0, count, (FloatSetter) mutator::set);
            break;
          }
          case ColumnType.DOUBLE: {
            Float8Vector.Mutator mutator = (Float8Vector.Mutator) info.valueVector.getMutator();
            mutator.setSafe(count - 1, 0);
            dataPack.foreach(0, count, (DoubleSetter) mutator::set);
            break;
          }
          case ColumnType.STRING: {
            ByteBuffer byteBuffer = MemoryUtil.getHollowDirectByteBuffer();
            VarCharVector.Mutator mutator = (VarCharVector.Mutator) info.valueVector.getMutator();
            dataPack.foreach(0, count, (int id, BytePiece bytes) -> {
              assert bytes.base == null;
              MemoryUtil.setByteBuffer(byteBuffer, bytes.addr, bytes.len, null);
              mutator.setSafe(id, byteBuffer, 0, byteBuffer.remaining());
            });
            break;
          }
          default:
            throw new IllegalStateException(String.format("Unsupported date type %s", info.columnSchema.dataType));
        }
      } else {
        // Start from v1, we directly copy the memory into vector, to avoid the traversing cost.

        if (dataType == ColumnType.STRING) {
          VarCharVector vector = (VarCharVector) info.valueVector;
          UInt4Vector offsetVector = vector.getOffsetVector();

          ByteSlice packData = dataPack.data();
          int indexSize = (count + 1) << 2;
          int strDataSize = packData.size() - indexSize;

          dataPack.stringValueAt(0);
          dataPack.stringValueAt(count - 1);

          packData.getInt(1);

          // Expand the offset vector if needed.
          offsetVector.getMutator().setSafe(count, 0);
          // Expand the data vector if needed.
          while (vector.getByteCapacity() < strDataSize) {
            vector.reAlloc();
          }
          Preconditions.checkState(vector.getByteCapacity() >= strDataSize, "Illegal drill vector buff capacity");

          DrillBuf offsetBuffer = offsetVector.getBuffer();
          DrillBuf vectorBuffer = vector.getBuffer();

          MemoryUtil.copyMemory(packData.address(), offsetBuffer.memoryAddress(), indexSize);
          MemoryUtil.copyMemory(packData.address() + indexSize, vectorBuffer.memoryAddress(), strDataSize);
        } else {
          BaseDataValueVector vector = (BaseDataValueVector) info.valueVector;

          // Expand the vector if needed.
          switch (dataType) {
            case ColumnType.INT:
              ((IntVector.Mutator) vector.getMutator()).setSafe(count - 1, 0);
              break;
            case ColumnType.LONG:
              ((BigIntVector.Mutator) vector.getMutator()).setSafe(count - 1, 0);
              break;
            case ColumnType.FLOAT:
              ((Float4Vector.Mutator) vector.getMutator()).setSafe(count - 1, 0);
              break;
            case ColumnType.DOUBLE:
              ((Float8Vector.Mutator) vector.getMutator()).setSafe(count - 1, 0);
              break;
            default:
              throw new IllegalStateException(String.format("Unsupported date type %s", info.columnSchema.dataType));
          }

          DrillBuf vectorBuffer = vector.getBuffer();
          ByteSlice packData = dataPack.data();

          Preconditions.checkState((count << ColumnType.numTypeShift(dataType)) == packData.size(), "Illegal pack size");
          Preconditions.checkState(vectorBuffer.capacity() >= packData.size(), "Illegal drill vector buff capacity");

          MemoryUtil.copyMemory(packData.address(), vectorBuffer.memoryAddress(), packData.size());
        }
      }
    }
    return read;
  }

  @Override
  public void close() throws Exception {
    if (segmentMap != null) {
      segmentMap.values().forEach(IOUtils::closeQuietly);
      segmentMap = null;
      segmentOpener = null;
      works = null;
    }
  }
}

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

import org.apache.commons.io.IOUtils;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

import io.indexr.segment.ColumnSchema;
import io.indexr.segment.Segment;
import io.indexr.segment.SegmentSchema;
import io.indexr.segment.helper.SegmentOpener;
import io.indexr.segment.helper.SingleWork;
import io.indexr.segment.rc.RCOperator;
import io.indexr.segment.storage.CachedSegment;
import io.indexr.util.BitMap;

public class IndexRRecordReaderByPack extends IndexRRecordReader {
  private static final Logger log = LoggerFactory.getLogger(IndexRRecordReaderByPack.class);

  private RCOperator rsFilter;
  private int curStepId = 0;

  private long setupTimePoint = 0;
  private long lmCheckTime = 0;

  private Segment curSegment;

  // We use two arrays here to make sure the scan operation is always forward in file.

  // The column id of current selecting segment.
  private int[] curSegProjColIds;
  // The index of the projectColumnInfos according to current segment.
  private int[] curSegProjColInfoIndies;

  private PackReader packReader;

  public IndexRRecordReaderByPack(String tableName,//
                                  SegmentSchema schema,//
                                  List<SchemaPath> projectColumns,//
                                  SegmentOpener segmentOpener,//
                                  RCOperator rsFilter,//
                                  List<SingleWork> works) {
    super(tableName, schema, segmentOpener, projectColumns, works);
    this.segmentOpener = segmentOpener;
    this.rsFilter = rsFilter;
  }

  @Override
  public void setup(OperatorContext context, OutputMutator output) throws ExecutionSetupException {
    setupTimePoint = System.currentTimeMillis();
    super.setup(context, output);
    curSegProjColIds = new int[projectColumnInfos.length];
    curSegProjColInfoIndies = new int[projectColumnInfos.length];
  }

  @Override
  public int next() {
    int read = -1;
    while (read <= 0) {
      try {
        if (packReader != null && packReader.hasMore()) {
          read = packReader.read();
        } else {
          if (curStepId >= works.size()) {
            return 0;
          }
          SingleWork stepWork = works.get(curStepId);
          curStepId++;

          int packId = stepWork.packId();
          Segment segment;
          if (curSegment != null
              && curSegment.name().equals(stepWork.segment())) {
            segment = curSegment;
          } else {
            IOUtils.closeQuietly(curSegment);

            segment = segmentOpener.open(stepWork.segment());
            setProjectColumnIds(segment);

            curSegment = segment;
          }
          CachedSegment cachedSegment = new CachedSegment(segment);
          BitMap position = beforeRead(cachedSegment, packId);
          if (position == BitMap.NONE) {
            continue;
          } else if (position == BitMap.ALL || position == BitMap.SOME) {
            packReader = new DefaultPackReader(cachedSegment, packId, projectColumnInfos, curSegProjColIds, curSegProjColInfoIndies);
          } else {
            packReader = new PositionPackReader(cachedSegment, packId, projectColumnInfos, curSegProjColIds, curSegProjColInfoIndies, position);
          }
        }
      } catch (Throwable t) {
        // No matter or what, don't thrown exception from here.
        // It will break the Drill algorithm and make system unpredictable.
        // I do think Drill should handle this...

        log.error("Read rows error, query may return incorrect result.", t);
        read = 0;
      }
    }
    return read;
  }

  private void setProjectColumnIds(Segment segment) {
    // Set the project columns to the real columnIds.
    List<ColumnSchema> columns = segment.schema().getColumns();
    int projectId = 0;
    for (int colId = 0; colId < columns.size(); colId++) {
      ColumnSchema sc = columns.get(colId);
      for (int index = 0; index < projectColumnInfos.length; index++) {
        ColumnSchema column = projectColumnInfos[index].columnSchema;
        if (sc.getName().equalsIgnoreCase(column.getName())
            && sc.getDataType() == column.getDataType()) {
          curSegProjColIds[projectId] = colId;
          curSegProjColInfoIndies[projectId] = index;
          projectId++;
          break;
        }
      }
    }

    if (projectId != projectColumnInfos.length) {
      // Find what is missing and make exception
      for (int i = 0; i < projectColumnInfos.length; i++) {
        ColumnSchema column = projectColumnInfos[i].columnSchema;
        Integer columnId = DrillIndexRTable.mapColumn(column, segment.schema());
        if (columnId == null) {
          throw new IllegalStateException(String.format("segment[%s]: column %s not found in %s",
              segment.name(), column, segment.schema()));
        }
      }

      // Why we are here?
      throw new RuntimeException(String.format("segment[%s]: column illegal", segment.name()));
    }
  }

  /**
   * This method check whether those rows in packId possibly contains any rows we interested.
   *
   * @return the positions of avaliable rows.
   */
  private BitMap beforeRead(CachedSegment segment, int packId) throws IOException {
    List<ColumnSchema> schemas = segment.schema().getColumns();

    // Set the attrs to the real columnIds.
    if (rsFilter != null) {
      rsFilter.materialize(schemas);
    }

    if (rsFilter == null) {
      return BitMap.SOME;
    }

    long time2 = System.currentTimeMillis();
    BitMap position = rsFilter.exactCheckOnRow(segment, packId);
    lmCheckTime += System.currentTimeMillis() - time2;
    if (position == BitMap.NONE) {
      log.debug("ignore (LM) segment: {}, pack: {}", segment.name(), packId);
    } else {
      log.debug("hit (LM) segment: {}, packId: {}", segment.name(), packId);
    }

    return position;
  }

  @Override
  public void close() throws Exception {
    super.close();
    if (packReader != null) {
      packReader.clear();
      packReader = null;
    }
    if (curSegment != null) {
      IOUtils.closeQuietly(curSegment);
      curSegment = null;
    }
    long now = System.currentTimeMillis();
    log.debug("cost: total: {}ms, lmCheck: {}ms", now - setupTimePoint, lmCheckTime);
  }
}

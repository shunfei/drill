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
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;

import com.carrotsearch.hppc.ObjectLongHashMap;
import com.carrotsearch.hppc.cursors.ObjectLongCursor;

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.physical.EndpointAffinity;
import org.apache.drill.exec.proto.CoordinationProtos;
import org.apache.drill.exec.store.AbstractRecordReader;
import org.apache.drill.exec.store.schedule.EndpointByteMap;
import org.apache.drill.exec.store.schedule.EndpointByteMapImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import io.indexr.segment.InfoSegment;
import io.indexr.segment.RSValue;
import io.indexr.segment.SegmentFd;
import io.indexr.segment.SegmentSchema;
import io.indexr.segment.helper.RangeWork;
import io.indexr.segment.helper.SingleWork;
import io.indexr.segment.pack.DataPack;
import io.indexr.segment.rc.RCOperator;
import io.indexr.server.HybridTable;
import io.indexr.server.TablePool;
import io.indexr.util.Trick;

public class ScanWrokProvider {
  private static final Logger logger = LoggerFactory.getLogger(ScanWrokProvider.class);
  private static final double RT_COST_RATE = 3.5;
  private static final int MIN_PACK_SPLIT_STEP = 10;
  private static final int RESERVE_NODE_THRESHOLD = 4;
  private static final Comparator<EndpointAffinity> eaDescCmp = (ea1, ea2) -> Double.compare(ea2.getAffinity(), ea1.getAffinity());

  private static final Cache<CacheKey, Works> workCache = CacheBuilder.newBuilder()
      .initialCapacity(1024)
      .expireAfterAccess(5, TimeUnit.MINUTES)
      .maximumSize(2048)
      .build();
  private static final Cache<CacheKey, Stat> statCache = CacheBuilder.newBuilder()
      .initialCapacity(1024)
      .expireAfterAccess(5, TimeUnit.MINUTES)
      .maximumSize(2048)
      .build();

  private static class CacheKey {
    private final String scanId;
    private final IndexRScanSpec scanSpec;
    private final long limitScanRows;

    public CacheKey(String scanId, IndexRScanSpec scanSpec, long limitScanRows) {
      this.scanId = scanId;
      this.scanSpec = scanSpec;
      this.limitScanRows = limitScanRows;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      CacheKey cacheKey = (CacheKey) o;

      if (scanId != null ? !scanId.equals(cacheKey.scanId) : cacheKey.scanId != null) return false;
      if (limitScanRows != cacheKey.limitScanRows) return false;
      return scanSpec != null ? scanSpec.equals(cacheKey.scanSpec) : cacheKey.scanSpec == null;
    }

    @Override
    public int hashCode() {
      int result = scanId != null ? scanId.hashCode() : 0;
      result = 31 * result + (scanSpec != null ? scanSpec.hashCode() : 0);
      result = 31 * result + (int) (limitScanRows ^ (limitScanRows >>> 32));
      return result;
    }
  }

  public static class Stat {
    public final long scanRowCount;
    public final int maxPw;

    public Stat(long scanRowCount, int maxPw) {
      this.scanRowCount = scanRowCount;
      this.maxPw = maxPw;
    }
  }

  public static class Works {
    public final int minPw;
    public final int maxPw;
    public final List<ScanCompleteWork> historyWorks;
    public final ListMultimap<CoordinationProtos.DrillbitEndpoint, RangeWork> realtimeWorks;
    public final List<EndpointAffinity> endpointAffinities;

    public Works(int minPw,
                 int maxPw,
                 List<ScanCompleteWork> historyWorks,
                 ListMultimap<CoordinationProtos.DrillbitEndpoint, RangeWork> realtimeWorks,
                 List<EndpointAffinity> endpointAffinities) {
      this.minPw = minPw;
      this.maxPw = maxPw;
      this.historyWorks = historyWorks;
      this.realtimeWorks = realtimeWorks;
      this.endpointAffinities = endpointAffinities;
    }
  }

  public static class FragmentAssignment {
    int fragmentCount;
    int fragmentIndex;
    List<RangeWork> endpointWorks;

    public FragmentAssignment(int fragmentCount, int fragmentIndex, List<RangeWork> endpointWorks) {
      this.fragmentCount = fragmentCount;
      this.fragmentIndex = fragmentIndex;
      this.endpointWorks = endpointWorks;
    }

    @Override
    public String toString() {
      return "FragmentAssignment{" +
          "fragmentCount=" + fragmentCount +
          ", fragmentIndex=" + fragmentIndex +
          ", endpointWorks=" + endpointWorks +
          '}';
    }
  }


  public static Stat getStat(IndexRStoragePlugin plugin,
                             IndexRScanSpec scanSpec,
                             String scanId,
                             long limitScanRows,
                             List<SchemaPath> columns) {
    CacheKey key = new CacheKey(scanId, scanSpec, limitScanRows);
    try {
      return statCache.get(key, () -> calStat(plugin, scanSpec, scanId, limitScanRows, columns));
    } catch (ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  private static Stat calStat(IndexRStoragePlugin plugin,
                              IndexRScanSpec scanSpec,
                              String scanId,
                              long limitScanRows,
                              List<SchemaPath> columns) throws IOException {
    TablePool tablePool = plugin.indexRNode().getTablePool();
    HybridTable table = tablePool.get(scanSpec.getTableName());
    SegmentSchema schema = table.schema().schema;

    List<SegmentFd> allSegments = table.segmentPool().all();

    RCOperator rsFilter = scanSpec.getRSFilter();
    long totalRowCount = 0;
    long validRowCount = 0;
    int validPackCount = 0;
    for (SegmentFd fd : allSegments) {
      InfoSegment infoSegment = fd.info();
      totalRowCount += infoSegment.rowCount();
      if (rsFilter == null || rsFilter.roughCheckOnColumn(infoSegment) != RSValue.None) {
        if (infoSegment.isRealtime()) {
          validPackCount += DataPack.rowCountToPackCount(infoSegment.rowCount());
        } else {
          validPackCount += infoSegment.packCount();
        }
        validRowCount += infoSegment.rowCount();
      } else {
        logger.debug("rs filter ignore segment {}", infoSegment.name());
      }

      if (validRowCount >= limitScanRows) {
        break;
      }
    }

    long statScanRowCount = validRowCount;
    if (rsFilter != null
        && validRowCount == totalRowCount
        && validRowCount > 0) {
      // We definitely want the scan node with rcFilter to win in query plan competition,
      // so that we trick the planner with less scan rows.
      // The rsFilter is good, even if it looks useless in this level, i.e. roughCheckOnColumn.
      // It could make different in next level, i.e. roughCheckOnPack.
      statScanRowCount = validRowCount - 1;
    }

    // estimated maxPw.
    int maxPw = Math.min(Math.max(2, validPackCount / MIN_PACK_SPLIT_STEP), confMaxPw(plugin));

    logger.debug("=============== calScanRowCount totalRowCount:{}, validRowCount:{}, validPackCount:{}, statScanRowCount:{}, maxPw: {}",
        totalRowCount, validRowCount, validPackCount, statScanRowCount, maxPw);

    return new Stat(statScanRowCount, maxPw);
  }

  public static Works getScanWorks(boolean must,
                                   IndexRStoragePlugin plugin,
                                   IndexRScanSpec scanSpec,
                                   String scanId,
                                   long limitScanRows,
                                   List<SchemaPath> columns) {
    CacheKey key = new CacheKey(scanId, scanSpec, limitScanRows);
    try {
      return must
          ? workCache.get(key, () -> calScanWorks(plugin, scanSpec, scanId, limitScanRows, columns))
          : workCache.getIfPresent(key);
    } catch (ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  private static Works calScanWorks(IndexRStoragePlugin plugin,
                                    IndexRScanSpec scanSpec,
                                    String scanId,
                                    long limitScanRows,
                                    List<SchemaPath> columns) throws IOException {
    TablePool tablePool = plugin.indexRNode().getTablePool();
    HybridTable table = tablePool.get(scanSpec.getTableName());
    SegmentSchema schema = table.schema().schema;
    int confMaxPw = confMaxPw(plugin);

    List<SegmentFd> allSegments = table.segmentPool().all();

    RCOperator rsFilter = scanSpec.getRSFilter();
    long totalRowCount = 0;
    long validRowCount = 0;
    int validPackCount = 0;
    List<InfoSegment> usedSegments = new ArrayList<>(Math.max(allSegments.size() / 2, 100));
    for (SegmentFd fd : allSegments) {
      InfoSegment infoSegment = fd.info();
      totalRowCount += infoSegment.rowCount();
      if (rsFilter == null || rsFilter.roughCheckOnColumn(infoSegment) != RSValue.None) {
        if (infoSegment.isRealtime()) {
          validPackCount += DataPack.rowCountToPackCount(infoSegment.rowCount());
        } else {
          validPackCount += infoSegment.packCount();
        }
        usedSegments.add(infoSegment);
        validRowCount += infoSegment.rowCount();
      } else {
        logger.debug("rs filter ignore segment {}", infoSegment.name());
      }

      if (validRowCount >= limitScanRows) {
        break;
      }
    }

    if (logger.isInfoEnabled()) {
      double passRate = totalRowCount == 0 ? 0.0 : ((double) validRowCount) / totalRowCount;
      passRate = Math.min(passRate, 1.0);
      logger.info("=============== calScanWorks limitScanRows: {}, Pass rate: {}, scan row: {}",
          limitScanRows,
          String.format("%.2f%%", (float) (passRate * 100)),
          validRowCount);
    }

    Map<String, CoordinationProtos.DrillbitEndpoint> hostEndpointMap = new HashMap<>();
    for (CoordinationProtos.DrillbitEndpoint endpoint : plugin.context().getBits()) {
      hostEndpointMap.put(endpoint.getAddress(), endpoint);
    }

    boolean smallFetch = usedSegments.size() > 0
        && limitScanRows <= usedSegments.get(0).rowCount()
        && limitScanRows <= MIN_PACK_SPLIT_STEP * DataPack.MAX_COUNT;

    int packSplitStep = Math.max(validPackCount / confMaxPw, MIN_PACK_SPLIT_STEP);
    if (smallFetch) {
      // Only the first work will be used. We minimize the scan rows.
      packSplitStep = Math.min((int) (limitScanRows / DataPack.MAX_COUNT + 1), packSplitStep);
    }
    int colCount = colCount(table, columns);

    boolean isCompress = plugin.getConfig().isCompress();
    double hisByteCostPerRow = DrillIndexRTable.byteCostPerRow(table, columns, isCompress);
    double rtByteCostPerRow = hisByteCostPerRow * RT_COST_RATE;

    List<ScanCompleteWork> historyWorks = new ArrayList<>(1024);
    ListMultimap<CoordinationProtos.DrillbitEndpoint, RangeWork> realtimeWorks = ArrayListMultimap.create();
    ObjectLongHashMap<CoordinationProtos.DrillbitEndpoint> affinities = new ObjectLongHashMap<>();

    for (InfoSegment segment : usedSegments) {
      List<String> hosts = table.segmentLocality().getHosts(segment.name(), segment.isRealtime());
      if (segment.isRealtime()) {
        // Realtime segments.
        assert hosts.size() == 1;
        long bytes = (long) (rtByteCostPerRow * segment.rowCount());
        CoordinationProtos.DrillbitEndpoint endpoint = hostEndpointMap.get(hosts.get(0));
        if (endpoint == null) {
          // Looks like this endpoint is down, the realtime segment on it cannot reach right now, let's move on.
          continue;
        }

        affinities.putOrAdd(endpoint, bytes, bytes);
        realtimeWorks.put(endpoint, new SingleWork(segment.name(), -1));

        if (smallFetch) {
          break;
        }
      } else {
        // Historical segments.
        int segPackCount = segment.packCount();
        int startPackId = 0;
        while (startPackId < segPackCount) {
          int endPackId = Math.min(startPackId + packSplitStep, segPackCount);
          int scanPackCount = endPackId - startPackId;

          long rowCount = scanPackCount * DataPack.MAX_COUNT;
          long bytes = (long) (hisByteCostPerRow * rowCount);
          EndpointByteMap endpointByteMap = new EndpointByteMapImpl();
          for (String host : hosts) {
            CoordinationProtos.DrillbitEndpoint endpoint = hostEndpointMap.get(host);
            // endpoint may not found in this host, could be shutdown or not installed.
            if (endpoint != null) {
              endpointByteMap.add(endpoint, bytes);
              affinities.putOrAdd(endpoint, bytes, bytes);
            }
          }
          historyWorks.add(new ScanCompleteWork(segment.name(), startPackId, endPackId, bytes, endpointByteMap));

          startPackId = endPackId;

          if (smallFetch) {
            Preconditions.checkState(rowCount >= limitScanRows);
            break;
          }
        }
      }
    }

    List<EndpointAffinity> endpointAffinities = new ArrayList<>(affinities.size());
    for (ObjectLongCursor<CoordinationProtos.DrillbitEndpoint> cursor : affinities) {
      endpointAffinities.add(new EndpointAffinity(cursor.key, cursor.value));
    }

    // sort it in desc.
    endpointAffinities.sort(eaDescCmp);
    int lastRTE = Trick.indexLast(endpointAffinities, ea -> realtimeWorks.containsKey(ea.getEndpoint()));

    int minPw = Math.max(1, lastRTE + 1);
    //int maxPw = Math.min(historyWorks.size() + realtimeWorks.size(), confMaxPw);
    int maxPw = Math.max(Math.min(validPackCount / MIN_PACK_SPLIT_STEP, confMaxPw), minPw);

    logger.debug("=============== calScanWorks minPw {}", minPw);
    logger.debug("=============== calScanWorks maxPw {}", maxPw);
    logger.debug("=============== calScanWorks historyWorks {}", historyWorks);
    logger.debug("=============== calScanWorks realtimeWorks {}", realtimeWorks);
    logger.debug("=============== calScanWorks endpointAffinities {}", endpointAffinities);

    return new Works(minPw, maxPw, historyWorks, realtimeWorks, endpointAffinities);
  }

  private static int colCount(HybridTable table, List<SchemaPath> columns) {
    int colCount = 0;
    if (columns == null) {
      colCount = 20;
    } else if (AbstractRecordReader.isStarQuery(columns)) {
      colCount = table.schema().schema.columns.size();
    } else {
      colCount = columns.size();
    }
    return colCount;
  }

  private static int confMaxPw(IndexRStoragePlugin plugin) {
    IndexRStoragePluginConfig config = plugin.getConfig();
    int confPerNode = Math.max(config.getMaxScanThreadsPerNode(), 1);
    int endpointCount = plugin.context().getBits().size();
    if (endpointCount >= RESERVE_NODE_THRESHOLD) {
      return Math.max((int) (endpointCount * confPerNode * (1f - config.getResourceReserveRate())), 1);
    } else {
      return endpointCount * confPerNode;
    }
  }
}

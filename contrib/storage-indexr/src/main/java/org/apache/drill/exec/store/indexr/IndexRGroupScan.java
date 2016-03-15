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
import com.google.common.base.Supplier;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;

import com.carrotsearch.hppc.ObjectLongOpenHashMap;
import com.carrotsearch.hppc.cursors.ObjectLongCursor;
import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.physical.EndpointAffinity;
import org.apache.drill.exec.physical.PhysicalOperatorSetupException;
import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.ScanStats;
import org.apache.drill.exec.physical.base.SubScan;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.store.AbstractRecordReader;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.store.schedule.AssignmentCreator;
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

@JsonTypeName("indexr-scan")
public class IndexRGroupScan extends AbstractGroupScan {
  private static final Logger logger = LoggerFactory.getLogger(IndexRGroupScan.class);
  private static final double RT_COST_RATE = 3.5;
  private static final Comparator<EndpointAffinity> eaDescCmp = (ea1, ea2) -> Double.compare(ea2.getAffinity(), ea1.getAffinity());

  private final IndexRStoragePlugin plugin;
  private final IndexRScanSpec scanSpec;
  private final String scanId;
  private List<SchemaPath> columns;

  private Integer minPw;
  private List<ScanCompleteWork> historyWorks;
  private ListMultimap<DrillbitEndpoint, RangeWork> realtimeWorks;
  private List<EndpointAffinity> endpointAffinities;

  private Map<Integer, FragmentAssignment> assignments;

  @JsonCreator
  public IndexRGroupScan(@JsonProperty("indexrScanSpec") IndexRScanSpec scanSpec,//
                         @JsonProperty("storage") IndexRStoragePluginConfig storagePluginConfig,//
                         @JsonProperty("columns") List<SchemaPath> columns,//
                         @JsonProperty("scanId") String scanId,//
                         @JacksonInject StoragePluginRegistry pluginRegistry//
  ) throws IOException, ExecutionSetupException {
    this((IndexRStoragePlugin) pluginRegistry.getPlugin(storagePluginConfig), scanSpec, columns, scanId);
  }

  public IndexRGroupScan(IndexRStoragePlugin plugin, IndexRScanSpec scanSpec, List<SchemaPath> columns, String scanId) {
    super((String) null);
    this.plugin = plugin;
    this.scanSpec = scanSpec;
    this.columns = columns;
    this.scanId = scanId;
  }

  /**
   * Private constructor, used for cloning.
   */
  private IndexRGroupScan(IndexRGroupScan that) {
    super(that);
    this.scanId = that.scanId;
    this.columns = that.columns;
    this.scanSpec = that.scanSpec;
    this.plugin = that.plugin;
  }

  @Override
  public IndexRGroupScan clone(List<SchemaPath> columns) {
    IndexRGroupScan newScan = new IndexRGroupScan(this);
    newScan.columns = columns;
    return newScan;
  }

  private int getMinPw() {
    return withCalScanWorks(() -> minPw);
  }

  private List<ScanCompleteWork> getHistoryWorks() {
    return withCalScanWorks(() -> historyWorks);
  }

  private ListMultimap<DrillbitEndpoint, RangeWork> getRealtimeWorks() {
    return withCalScanWorks(() -> realtimeWorks);
  }

  private List<EndpointAffinity> getAffinities() {
    return withCalScanWorks(() -> endpointAffinities);
  }

  private <T> T withCalScanWorks(Supplier<T> s) {
    try {
      if (s.get() == null) {
        calScanWorks();
      }
      return s.get();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private void calScanWorks() throws Exception {
    boolean isAllColumn = AbstractRecordReader.isStarQuery(columns);
    TablePool tablePool = plugin.indexRNode().getTablePool();
    HybridTable table = tablePool.get(scanSpec.getTableName());
    SegmentSchema schema = table.schema().schema;

    List<SegmentFd> allSegments = table.segmentPool().all();

    RCOperator filter = scanSpec.getRSFilter();
    long totalRowCount = 0;
    long validRowCount = 0;
    int validPackCount = 0;
    List<InfoSegment> usedSegments = new ArrayList<>(Math.max(allSegments.size() / 2, 48));
    for (SegmentFd fd : allSegments) {
      InfoSegment infoSegment = fd.info();
      totalRowCount += infoSegment.rowCount();
      if (filter == null || filter.roughCheckOnColumn(infoSegment) != RSValue.None) {
        if (infoSegment.isRealtime()) {
          validPackCount = DataPack.rowCountToPackCount(infoSegment.rowCount());
        } else {
          validPackCount = infoSegment.packCount();
        }
        usedSegments.add(infoSegment);
        validRowCount += infoSegment.rowCount();
      } else {
        logger.debug("rs filter ignore segment {}", infoSegment.name());
      }
    }

    if (logger.isInfoEnabled()) {
      double passRate = totalRowCount == 0 ? 0.0 : ((double) validRowCount) / totalRowCount;
      passRate = Math.min(passRate, 1.0);
      logger.info("Pass rate: {}, scan row: {}", String.format("%.2f%%", (float) (passRate * 100)), validRowCount);
    }

    Map<String, DrillbitEndpoint> hostEndpointMap = new HashMap<>();
    for (DrillbitEndpoint endpoint : plugin.context().getBits()) {
      hostEndpointMap.put(endpoint.getAddress(), endpoint);
    }

    int splitStep = validPackCount > 1000 ? validPackCount / 1000 : 1;
    int colCount = colCount(table);

    boolean isCompress = getStorageConfig().isCompress();
    double hisByteCostPerRow = DrillIndexRTable.byteCostPerRow(table, columns, isCompress);
    double rtByteCostPerRow = hisByteCostPerRow * RT_COST_RATE;

    List<ScanCompleteWork> historyWorks = new ArrayList<>(1024);
    ListMultimap<DrillbitEndpoint, RangeWork> realtimeWorks = ArrayListMultimap.create();
    ObjectLongOpenHashMap<DrillbitEndpoint> affinities = new ObjectLongOpenHashMap<DrillbitEndpoint>();

    for (InfoSegment segment : usedSegments) {
      List<String> hosts = table.segmentLocality().getHosts(segment.name(), segment.isRealtime());
      if (segment.isRealtime()) {
        // Realtime segments.
        assert hosts.size() == 1;
        long bytes = (long) (rtByteCostPerRow * segment.rowCount());
        DrillbitEndpoint endpoint = hostEndpointMap.get(hosts.get(0));
        if (endpoint == null) {
          // Looks like this endpoint is down, the realtime segment on it cannot reach right now, let's move on.
          continue;
        }

        affinities.putOrAdd(endpoint, bytes, bytes);
        realtimeWorks.put(endpoint, new SingleWork(segment.name(), -1));
      } else {
        // Historical segments.
        int segPackCount = segment.packCount();
        int startPackId = 0;
        while (startPackId < segPackCount) {
          int endPackId = Math.min(startPackId + splitStep, segPackCount);
          int scanPackCount = endPackId - startPackId;

          long rowCount = scanPackCount * DataPack.MAX_COUNT;
          long bytes = (long) (hisByteCostPerRow * rowCount);
          EndpointByteMap endpointByteMap = new EndpointByteMapImpl();
          for (String host : hosts) {
            DrillbitEndpoint endpoint = hostEndpointMap.get(host);
            // endpoint may not found in this host, could be shutdown or not installed.
            if (endpoint != null) {
              endpointByteMap.add(endpoint, bytes);
              affinities.putOrAdd(endpoint, bytes, bytes);
            }
          }
          historyWorks.add(new ScanCompleteWork(segment.name(), startPackId, endPackId, bytes, endpointByteMap));

          startPackId = endPackId;
        }
      }
    }

    List<EndpointAffinity> endpointAffinities = new ArrayList<>(affinities.size());
    for (ObjectLongCursor<DrillbitEndpoint> cursor : affinities) {
      endpointAffinities.add(new EndpointAffinity(cursor.key, cursor.value));
    }

    // sort it in desc.
    endpointAffinities.sort(eaDescCmp);
    int lastRTE = Trick.indexLast(endpointAffinities, ea -> realtimeWorks.containsKey(ea.getEndpoint()));

    this.minPw = Math.max(1, lastRTE + 1);
    this.historyWorks = historyWorks;
    this.realtimeWorks = realtimeWorks;
    this.endpointAffinities = endpointAffinities;

    logger.debug("=============== historyWorks {}", historyWorks);
    logger.debug("=============== realtimeWorks {}", realtimeWorks);
  }

  private int colCount(HybridTable table) {
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

  private long rowCount(HybridTable table) throws Exception {
    long rowCount = 0;
    RCOperator filter = scanSpec.getRSFilter();
    List<SegmentFd> segmentFds = table.segmentPool().all();
    logger.debug("segmentFds: {}", segmentFds.size());
    for (SegmentFd fd : segmentFds) {
      InfoSegment infoSegment = fd.info();
      if (filter == null || filter.roughCheckOnColumn(infoSegment) != RSValue.None) {
        rowCount += infoSegment.rowCount();
      }
    }
    return rowCount;
  }

  @JsonIgnore
  public IndexRStoragePlugin getStoragePlugin() {
    return plugin;
  }

  @JsonProperty("storage")
  public IndexRStoragePluginConfig getStorageConfig() {
    return plugin.getConfig();
  }

  @JsonProperty("columns")
  public List<SchemaPath> getColumns() {
    return columns;
  }

  @JsonProperty("indexrScanSpec")
  public IndexRScanSpec getScanSpec() {
    return scanSpec;
  }

  @JsonProperty("scanId")
  public String getScanId() {
    return scanId;
  }

  @Override
  @JsonIgnore
  public boolean canPushdownProjects(List<SchemaPath> columns) {
    return true;
  }

  @Override
  public int getMaxParallelizationWidth() {
    int perNode = Math.max(plugin.getConfig().getMaxScanThreadsPerNode(), 1);
    return plugin.context().getBits().size() * perNode;
  }

  @Override
  public int getMinParallelizationWidth() {
    try {
      int min = Math.max(1, getMinPw());
      logger.debug("=============== getMinParallelizationWidth {}", min);
      return min;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public String getDigest() {
    return toString();
  }

  @Override
  public String toString() {
    return String.format("IndexRGroupScan@%s{Spec=%s, columns=%s}",
        Integer.toHexString(super.hashCode()),
        scanSpec,
        columns);
  }

  private Long rowCount;

  @Override
  public ScanStats getScanStats() {
    try {
      HybridTable table = plugin.indexRNode().getTablePool().get(scanSpec.getTableName());
      if (rowCount == null) {
        rowCount = rowCount(table);
      }
      logger.debug("=============== getScanStats, rowCount: {}", rowCount);
      if (rowCount <= 100000 && table.segmentPool().realtimeHosts().size() > 0) {
        // We must make the planner use exchange which can spreads the query fragments among nodes.
        // Otherwise realtime segments won't be able to query.
        // We keep the scan rows over a threshold to acheive this.
        long useRowCount = 100000;
        return new ScanStats(ScanStats.GroupScanProperty.NO_EXACT_ROW_COUNT, useRowCount, 1, useRowCount * colCount(table));
      } else {
        return new ScanStats(ScanStats.GroupScanProperty.EXACT_ROW_COUNT, rowCount, 1, rowCount * colCount(table));
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) throws ExecutionSetupException {
    Preconditions.checkArgument(children.isEmpty());
    return new IndexRGroupScan(this);
  }

  @Override
  public List<EndpointAffinity> getOperatorAffinity() {
    return getAffinities();
  }

  @Override
  public void applyAssignments(List<DrillbitEndpoint> endpoints) throws PhysicalOperatorSetupException {
    try {
      List<ScanCompleteWork> historyWorks = getHistoryWorks();
      ListMultimap<DrillbitEndpoint, RangeWork> realtimeWorks = getRealtimeWorks();
      for (DrillbitEndpoint endpoint : realtimeWorks.keySet()) {
        if (!endpoints.contains(endpoint)) {
          String errorMsg = String.format(//
              "Realtime works on %s cannot be assigned, because thery are not in work list %s.",//
              endpoint.getAddress(),//
              Lists.transform(endpoints, DrillbitEndpoint::getAddress));
          throw new IllegalStateException(errorMsg);
        }
      }

      ListMultimap<DrillbitEndpoint, RangeWork> endpointToWorks = ArrayListMultimap.create();
      ListMultimap<DrillbitEndpoint, Integer> endpointToMinoFragmentId = ArrayListMultimap.create();

      // Put history works.
      ListMultimap<Integer, ScanCompleteWork> fakeAssignments = AssignmentCreator.getMappings(endpoints, historyWorks, plugin.context());
      for (int id = 0; id < endpoints.size(); id++) {
        DrillbitEndpoint endpoint = endpoints.get(id);

        endpointToWorks.putAll(endpoint, fakeAssignments.get(id));
        endpointToMinoFragmentId.put(endpoint, id);
      }

      // Put reatlime works.
      for (DrillbitEndpoint endpoint : realtimeWorks.keySet()) {
        endpointToWorks.putAll(endpoint, realtimeWorks.get(endpoint));
      }

      HashMap<Integer, FragmentAssignment> assignments = new HashMap<>();

      for (int id = 0; id < endpoints.size(); id++) {
        DrillbitEndpoint endpoint = endpoints.get(id);

        List<RangeWork> works = endpointToWorks.get(endpoint);
        List<Integer> fragments = endpointToMinoFragmentId.get(endpoint);

        works = RangeWork.compact(new ArrayList<>(works));
        assignments.put(id, new FragmentAssignment(fragments.size(), fragments.indexOf(id), works));
      }

      this.assignments = assignments;

      logger.debug("=====================  applyAssignments endpointToWorks: {}", endpointToWorks);
    } catch (Exception e) {
      throw new PhysicalOperatorSetupException(e);
    }
  }

  @Override
  public SubScan getSpecificScan(int minorFragmentId) throws ExecutionSetupException {
    FragmentAssignment assign = assignments.get(minorFragmentId);

    IndexRSubScanSpec subScanSpec = new IndexRSubScanSpec(//
        scanId,//
        scanSpec.getTableName(),//
        assign.fragmentCount,//
        assign.fragmentIndex, //
        assign.endpointWorks,//
        scanSpec.getRSFilter());
    return new IndexRSubScan(plugin, subScanSpec, columns);
  }

  private static class FragmentAssignment {
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
}

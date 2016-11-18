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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

import org.apache.drill.common.logical.StoragePluginConfigBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@JsonTypeName(IndexRStoragePluginConfig.NAME)
public class IndexRStoragePluginConfig extends StoragePluginConfigBase {
  private static final Logger log = LoggerFactory.getLogger(IndexRStoragePluginConfig.class);

  public static final String NAME = "indexr";

  private final int maxScanThreadsPerNode;
  private final float resourceReserveRate;
  private final boolean enableRSFilter;
  private final boolean enableMemCache;
  private final float singleQueryMemCacheThreshold;
  private final boolean isCompress;

  @JsonCreator
  public IndexRStoragePluginConfig(@JsonProperty("maxScanThreadsPerNode") Integer maxScanThreadsPerNode,//
                                   @JsonProperty("resourceReserveRate") Float resourceReserveRate,//
                                   @JsonProperty("enableRSFilter") Boolean enableRSFilter,//
                                   @JsonProperty("enableMemCache") Boolean enableMemCache,//
                                   @JsonProperty("singleQueryMemCacheThreshold") Float singleQueryMemCacheThreshold,//
                                   @JsonProperty("isCompress") Boolean isCompress) {
    this.maxScanThreadsPerNode = maxScanThreadsPerNode != null && maxScanThreadsPerNode > 0 ? maxScanThreadsPerNode : 10;
    this.resourceReserveRate = resourceReserveRate != null && resourceReserveRate > 0 ? resourceReserveRate : 0.35f;
    this.enableRSFilter = enableRSFilter != null ? enableRSFilter : true;

    this.enableMemCache = enableMemCache != null ? enableMemCache : true;
    this.singleQueryMemCacheThreshold = singleQueryMemCacheThreshold != null ? singleQueryMemCacheThreshold : 0.2f;
    this.isCompress = isCompress != null ? isCompress : true;
  }

  @JsonProperty("maxScanThreadsPerNode")
  public int getMaxScanThreadsPerNode() {
    return maxScanThreadsPerNode;
  }

  @JsonProperty("resourceReserveRate")
  public float getResourceReserveRate() {
    return resourceReserveRate;
  }

  @JsonProperty("enableRSFilter")
  public boolean isEnableRSFilter() {
    return enableRSFilter;
  }

  @JsonProperty("enableMemCache")
  public boolean isEnableMemCache() {
    return enableMemCache;
  }

  @JsonProperty("singleQueryMemCacheThreshold")
  public float getSingleQueryMemCacheThreshold() {
    return singleQueryMemCacheThreshold;
  }

  @JsonProperty("isCompress")
  public boolean isCompress() {
    return isCompress;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    IndexRStoragePluginConfig that = (IndexRStoragePluginConfig) o;

    if (maxScanThreadsPerNode != that.maxScanThreadsPerNode) return false;
    if (Float.compare(that.resourceReserveRate, resourceReserveRate) != 0) return false;
    if (enableRSFilter != that.enableRSFilter) return false;
    if (enableMemCache != that.enableMemCache) return false;
    if (Float.compare(that.singleQueryMemCacheThreshold, singleQueryMemCacheThreshold) != 0)
      return false;
    return isCompress == that.isCompress;

  }

  @Override
  public int hashCode() {
    int result = maxScanThreadsPerNode;
    result = 31 * result + (resourceReserveRate != +0.0f ? Float.floatToIntBits(resourceReserveRate) : 0);
    result = 31 * result + (enableRSFilter ? 1 : 0);
    result = 31 * result + (enableMemCache ? 1 : 0);
    result = 31 * result + (singleQueryMemCacheThreshold != +0.0f ? Float.floatToIntBits(singleQueryMemCacheThreshold) : 0);
    result = 31 * result + (isCompress ? 1 : 0);
    return result;
  }
}

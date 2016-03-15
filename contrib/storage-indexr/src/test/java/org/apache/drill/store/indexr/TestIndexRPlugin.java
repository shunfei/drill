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
package org.apache.drill.store.indexr;

import org.apache.drill.BaseTestQuery;
import org.junit.Ignore;
import org.junit.Test;

@Ignore("requires indexr node setting up")
public class TestIndexRPlugin extends BaseTestQuery {

  @Test
  public void testIndexr() throws Exception {
//    Thread.sleep(5000);

//    test("use indexr;");
//    test("show tables;");
//    test("describe campaign");

    // 19 is the limit for drill transform in into hashjoin!

    String sql = "select A.user_id, sum(A.clicks), sum(B.impressions) as aa from indexr.campaign as A join indexr.campaign as B on A.channel_id = B.channel_id " +
      "where " +
      "A.campaign_id in (0, 51409, 54638,31460, 50688, 51087, 55) and (B.user_id > 10000 or B.spot_id < 100000) " +
      "group by A.user_id order by aa desc limit 100";

    //sql = "select user_id, sum(clicks), sum(impressions) as aa from indexr.campaign where " +
    //  "campaign_id in (0, 51409, 54638,31460, 50688, 51087, 55) or user_id > 10000 " +
    //  "group by user_id order by aa desc limit 10";
    test("explain plan for " + sql);
//    test(sql);
  }

}

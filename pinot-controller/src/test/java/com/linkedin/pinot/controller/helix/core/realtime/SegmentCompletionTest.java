/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.linkedin.pinot.controller.helix.core.realtime;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import org.apache.helix.HelixManager;
import org.apache.helix.ZNRecord;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import com.linkedin.pinot.common.metadata.segment.LLCRealtimeSegmentZKMetadata;
import com.linkedin.pinot.common.protocols.SegmentCompletionProtocol;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.common.utils.LLCSegmentName;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class SegmentCompletionTest {

  private MockPinotLLCRealtimeSegmentManager segmentManager;
  private MockSegmentCompletionManager segmentCompletionMgr;
  private Map<String, Object> fsmMap;
  private String segmentNameStr;
  private final String s1 = "S1";
  private final String s2 = "S2";
  private final String s3 = "S3";

  private final long s1Offset = 20L;
  private final long s2Offset = 40L;
  private final long s3Offset = 30L;

  @BeforeMethod
  public void testCaseSetup() throws Exception {
    testCaseSetup(true);
  }

  public void testCaseSetup(boolean isLeader) throws Exception {
    segmentManager = new MockPinotLLCRealtimeSegmentManager();
    final int partitionId = 23;
    final int seqId = 12;
    final long now = System.currentTimeMillis();
    final String tableName = "someTable";
    final LLCSegmentName segmentName = new LLCSegmentName(tableName, partitionId, seqId, now);
    segmentNameStr = segmentName.getSegmentName();
    final LLCRealtimeSegmentZKMetadata metadata = new LLCRealtimeSegmentZKMetadata();
    metadata.setStatus(CommonConstants.Segment.Realtime.Status.IN_PROGRESS);
    metadata.setNumReplicas(3);
    segmentManager._segmentMetadata = metadata;

    segmentCompletionMgr = new MockSegmentCompletionManager(segmentManager, isLeader);
    segmentManager._segmentCompletionMgr = segmentCompletionMgr;

    Field fsmMapField = SegmentCompletionManager.class.getDeclaredField("_fsmMap");
    fsmMapField.setAccessible(true);
    fsmMap = (Map<String, Object>)fsmMapField.get(segmentCompletionMgr);
  }

  // Simulate a new controller taking over with an empty completion manager object,
  // but segment metadata is fine in zk
  private void replaceSegmentCompletionManager() throws Exception {
    long oldSecs = segmentCompletionMgr._secconds;
    segmentCompletionMgr = new MockSegmentCompletionManager(segmentManager, true);
    segmentCompletionMgr._secconds = oldSecs;
    Field fsmMapField = SegmentCompletionManager.class.getDeclaredField("_fsmMap");
    fsmMapField.setAccessible(true);
    fsmMap = (Map<String, Object>)fsmMapField.get(segmentCompletionMgr);
  }

  @Test
  public void testHappyPath() throws Exception {
    SegmentCompletionProtocol.Response response;
    // s1 sends offset of 20, gets HOLD at t = 5s;
    segmentCompletionMgr._secconds = 5;
    response = segmentCompletionMgr.segmentConsumed(segmentNameStr, s1, s1Offset);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.HOLD);
    // s2 sends offset of 40, gets HOLD
    segmentCompletionMgr._secconds += 1;
    response = segmentCompletionMgr.segmentConsumed(segmentNameStr, s2, s2Offset);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.HOLD);
    // s3 sends offset of 30, gets catchup to 40
    segmentCompletionMgr._secconds += 1;
    response = segmentCompletionMgr.segmentConsumed(segmentNameStr, s3, s3Offset);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.CATCH_UP);
    Assert.assertEquals(response.getOffset(), s2Offset);
    // Now s1 comes back, and is asked to catchup.
    segmentCompletionMgr._secconds += 1;
    response = segmentCompletionMgr.segmentConsumed(segmentNameStr, s1, s1Offset);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.CATCH_UP);
    // s2 is asked to commit.
    segmentCompletionMgr._secconds += 1;
    response = segmentCompletionMgr.segmentConsumed(segmentNameStr, s2, s2Offset);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.COMMIT);
    // s3 comes back with new caught up offset, it should get a HOLD, since commit is not done yet.
    segmentCompletionMgr._secconds += 1;
    response = segmentCompletionMgr.segmentConsumed(segmentNameStr, s3, s2Offset);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.HOLD);
    // s2 executes a succesful commit
    segmentCompletionMgr._secconds += 1;
    response = segmentCompletionMgr.segmentCommitStart(segmentNameStr, s2, s2Offset);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.COMMIT_CONTINUE);

    segmentCompletionMgr._secconds += 5;
    response = segmentCompletionMgr.segmentCommitEnd(segmentNameStr, s2, s2Offset, true);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.COMMIT_SUCCESS);

    // Now the FSM should have disappeared from the map
    Assert.assertFalse(fsmMap.containsKey(segmentNameStr));

    // Now if s3 or s1 come back, they are asked to keep the segment they have.
    segmentCompletionMgr._secconds += 1;
    response = segmentCompletionMgr.segmentConsumed(segmentNameStr, s2, s2Offset);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.KEEP);

    // And the FSM should be removed.
    Assert.assertFalse(fsmMap.containsKey(segmentNameStr));
  }

  @Test
  public void testDelayedServer() throws Exception {
    SegmentCompletionProtocol.Response response;
    // s1 sends offset of 20, gets HOLD at t = 5s;
    final int startTimeSecs = 5;
    segmentCompletionMgr._secconds = startTimeSecs;
    response = segmentCompletionMgr.segmentConsumed(segmentNameStr, s1, s1Offset);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.HOLD);
    // s2 sends offset of 40, gets HOLD
    segmentCompletionMgr._secconds += 1;
    response = segmentCompletionMgr.segmentConsumed(segmentNameStr, s2, s2Offset);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.HOLD);
    // Now s1 comes back again, and is asked to hold
    segmentCompletionMgr._secconds += 1;
    response = segmentCompletionMgr.segmentConsumed(segmentNameStr, s1, s1Offset);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.HOLD);
    // s2 is asked to commit.
    segmentCompletionMgr._secconds += SegmentCompletionProtocol.MAX_HOLD_TIME_MS/1000;
    response = segmentCompletionMgr.segmentConsumed(segmentNameStr, s2, s2Offset);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.COMMIT);

    // Now s3 comes up with a better offset, but we ask it to hold, since the committer has not committed yet.
    segmentCompletionMgr._secconds += 1;
    response = segmentCompletionMgr.segmentConsumed(segmentNameStr, s3, s2Offset + 10);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.HOLD);
    // s2 commits.
    segmentCompletionMgr._secconds += 1;
    response = segmentCompletionMgr.segmentCommitStart(segmentNameStr, s2, s2Offset);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.COMMIT_CONTINUE);
    segmentCompletionMgr._secconds += 5;
    response = segmentCompletionMgr.segmentCommitEnd(segmentNameStr, s2, s2Offset, true);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.COMMIT_SUCCESS);
    // Now the FSM should have disappeared from the map
    Assert.assertFalse(fsmMap.containsKey(segmentNameStr));

    // Now s3 comes back to get a discard.
    segmentCompletionMgr._secconds += 1;
    response = segmentCompletionMgr.segmentConsumed(segmentNameStr, s3, s2Offset + 10);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.DISCARD);
    // Now the FSM should have disappeared from the map
    Assert.assertFalse(fsmMap.containsKey(segmentNameStr));
  }

  // We test the case when the committer is asked to commit, but they never come back.
  @Test
  public void testCommitterFailure() throws Exception {
    SegmentCompletionProtocol.Response response;
    // s1 sends offset of 20, gets HOLD at t = 5s;
    final int startTimeSecs = 5;
    segmentCompletionMgr._secconds = startTimeSecs;
    response = segmentCompletionMgr.segmentConsumed(segmentNameStr, s1, s1Offset);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.HOLD);
    // s2 sends offset of 40, gets HOLD
    segmentCompletionMgr._secconds += 1;
    response = segmentCompletionMgr.segmentConsumed(segmentNameStr, s2, s2Offset);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.HOLD);
    // s3 is asked to hole
    segmentCompletionMgr._secconds += 1;
    response = segmentCompletionMgr.segmentConsumed(segmentNameStr, s3, s3Offset);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.CATCH_UP);
    Assert.assertEquals(response.getOffset(), s2Offset);
    // Now s2 is asked to commit.
    segmentCompletionMgr._secconds += 1;
    response = segmentCompletionMgr.segmentConsumed(segmentNameStr, s2, s2Offset);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.COMMIT);

    // Time passes, s2 never comes back.
    segmentCompletionMgr._secconds += SegmentCompletionProtocol.MAX_HOLD_TIME_MS/1000;

    // But since s1 and s3 are in HOLDING state, they should come back again. s1 is asked to catchup
    response = segmentCompletionMgr.segmentConsumed(segmentNameStr, s1, s1Offset);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.CATCH_UP);

    // Max time to commit passes
    segmentCompletionMgr._secconds += 6 * SegmentCompletionProtocol.MAX_HOLD_TIME_MS/1000;

    // s1 comes back with the updated offset, since it was asked to catch up.
    // The FSM will be aborted, and destroyed ...
    response = segmentCompletionMgr.segmentConsumed(segmentNameStr, s1, s2Offset);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.HOLD);

    Assert.assertFalse(fsmMap.containsKey(segmentNameStr));

    // s1 comes back again, a new FSM created
    segmentCompletionMgr._secconds += 1;
    response = segmentCompletionMgr.segmentConsumed(segmentNameStr, s1, s2Offset);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.HOLD);

    // s3 comes back
    segmentCompletionMgr._secconds += 1;
    response = segmentCompletionMgr.segmentConsumed(segmentNameStr, s3, s2Offset);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.HOLD);

    // And winner chosen when the last one does not come back at all
    segmentCompletionMgr._secconds += 5;
    response = segmentCompletionMgr.segmentConsumed(segmentNameStr, s3, s2Offset);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.COMMIT);

    // The FSM is still present to complete the happy path as before.
    Assert.assertTrue(fsmMap.containsKey(segmentNameStr));
  }

  @Test
  public void testControllerFailureDuringCommit() throws Exception {
    SegmentCompletionProtocol.Response response;
    // s1 sends offset of 20, gets HOLD at t = 5s;
    segmentCompletionMgr._secconds = 5;
    response = segmentCompletionMgr.segmentConsumed(segmentNameStr, s1, s1Offset);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.HOLD);
    // s2 sends offset of 40, gets HOLD
    segmentCompletionMgr._secconds += 1;
    response = segmentCompletionMgr.segmentConsumed(segmentNameStr, s2, s2Offset);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.HOLD);
    // s3 sends offset of 30, gets catchup to 40
    segmentCompletionMgr._secconds += 1;
    response = segmentCompletionMgr.segmentConsumed(segmentNameStr, s3, s3Offset);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.CATCH_UP);
    Assert.assertEquals(response.getOffset(), s2Offset);
    // Now s1 comes back, and is asked to catchup.
    segmentCompletionMgr._secconds += 1;
    response = segmentCompletionMgr.segmentConsumed(segmentNameStr, s1, s1Offset);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.CATCH_UP);
    // s2 is asked to commit.
    segmentCompletionMgr._secconds += 1;
    response = segmentCompletionMgr.segmentConsumed(segmentNameStr, s2, s2Offset);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.COMMIT);

    // Now the controller fails, and a new one takes over, with no knowledge of what was done before.
    replaceSegmentCompletionManager();

    // s3 comes back with the correct offset but is asked to hold.
    response = segmentCompletionMgr.segmentConsumed(segmentNameStr, s3, s2Offset);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.HOLD);

    // s1 comes back, and still asked to hold.
    response = segmentCompletionMgr.segmentConsumed(segmentNameStr, s1, s2Offset);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.HOLD);

    // s2 has no idea the controller failed, so it comes back with a commit,but the controller asks it to hold,
    // (essentially a commit failure)
    segmentCompletionMgr._secconds += 1;
    response = segmentCompletionMgr.segmentCommitStart(segmentNameStr, s2, s2Offset);
    Assert.assertTrue(response.getStatus().equals(SegmentCompletionProtocol.ControllerResponseStatus.HOLD));

    // So s2 goes back into HOLDING state. s1 and s3 are already holding, so now it will get COMMIT back.
    response = segmentCompletionMgr.segmentConsumed(segmentNameStr, s2, s2Offset);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.COMMIT);
  }

  @Test
  public void testNotLeader() throws Exception {
    testCaseSetup(false);
    SegmentCompletionProtocol.Response response;

    response = segmentCompletionMgr.segmentConsumed(segmentNameStr, s1, s1Offset);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.NOT_LEADER);

    response = segmentCompletionMgr.segmentCommitStart(segmentNameStr, s1, s1Offset);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.NOT_LEADER);
  }

  private static HelixManager createMockHelixManager(boolean isLeader) {
    HelixManager helixManager = mock(HelixManager.class);
    when(helixManager.isLeader()).thenReturn(isLeader);
    return helixManager;
  }

  public static class MockPinotLLCRealtimeSegmentManager extends PinotLLCRealtimeSegmentManager {
    public static final String clusterName = "someCluster";
    public LLCRealtimeSegmentZKMetadata _segmentMetadata;
    public MockSegmentCompletionManager _segmentCompletionMgr;

    protected MockPinotLLCRealtimeSegmentManager() {
      super(null, clusterName, null, null, null);
    }

    @Override
    public LLCRealtimeSegmentZKMetadata getRealtimeSegmentZKMetadata(String realtimeTableName, String segmentName) {
      return _segmentMetadata;
    }

    @Override
    public boolean commitSegment(String rawTableName, String committingSegmentName, long nextOffset) {
      _segmentMetadata.setStatus(CommonConstants.Segment.Realtime.Status.DONE);
      _segmentMetadata.setEndOffset(nextOffset);
      _segmentMetadata.setEndTime(_segmentCompletionMgr.getCurrentTimeMs());
      return true;
    }

    @Override
    protected void writeSegmentsToPropertyStore(List<String> paths, List<ZNRecord> records) {
      _segmentMetadata = new LLCRealtimeSegmentZKMetadata(records.get(0));  // Updated record that we are writing to ZK
    }
  }

  public static class MockSegmentCompletionManager extends SegmentCompletionManager {
    public long _secconds;
    protected MockSegmentCompletionManager(PinotLLCRealtimeSegmentManager segmentManager, boolean isLeader) {
      super(createMockHelixManager(isLeader), segmentManager);
    }
    @Override
    protected long getCurrentTimeMs() {
      return _secconds * 1000L;
    }
  }
}

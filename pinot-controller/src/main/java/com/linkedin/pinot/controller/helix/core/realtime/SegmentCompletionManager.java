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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.helix.HelixManager;
import org.apache.helix.ZNRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.linkedin.pinot.common.metadata.segment.LLCRealtimeSegmentZKMetadata;
import com.linkedin.pinot.common.protocols.SegmentCompletionProtocol;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.common.utils.LLCSegmentName;


/**
 * This is a singleton class in the controller that drives the state machines for segments that are in the
 * committing stage.
 */
public class SegmentCompletionManager {
  // TODO Can we log using the segment name in the log message?
  public static Logger LOGGER = LoggerFactory.getLogger(SegmentCompletionManager.class);
  private enum State {
    HOLDING,          // the segment has started finalizing.
    COMMITTER_DECIDED, // We know who the committer will be, we will let them know next time they call segmentConsumed()
    COMMITTER_NOTIFIED, // we notified the committer to commit.
    COMMITTER_UPLOADING,  // committer is uploading.
    COMMITTING, // we are in the process of committing to zk
    COMMITTED,    // We already committed a segment.
    ABORTED,      // state machine is aborted. we will start a fresh one when the next segmentConsumed comes in.
  }

  private static SegmentCompletionManager _instance = null;

  private final HelixManager _helixManager;
  // A map that holds the FSM for each segment.
  private final Map<String, SegmentCompletionFSM> _fsmMap = new ConcurrentHashMap<>();
  private final PinotLLCRealtimeSegmentManager _segmentManager;

  // TODO keep some history of past committed segments so that we can avoid looking up PROPERTYSTORE if some server comes in late.

  protected SegmentCompletionManager(HelixManager helixManager, PinotLLCRealtimeSegmentManager segmentManager) {
    _helixManager = helixManager;
    _segmentManager = segmentManager;
  }

  public static SegmentCompletionManager create(HelixManager helixManager, PinotLLCRealtimeSegmentManager segmentManager) {
    if (_instance != null) {
      throw new RuntimeException("Cannot create multiple instances");
    }
    _instance = new SegmentCompletionManager(helixManager, segmentManager);
    return _instance;
  }

  public static SegmentCompletionManager getInstance() {
    if (_instance == null) {
      throw new RuntimeException("Not yet created");
    }
    return _instance;
  }

  protected long getCurrentTimeMs() {
    return System.currentTimeMillis();
  }

  // We need to make sure that we never create multiple FSMs for the same segment, so this method must be synchronized.
  private synchronized SegmentCompletionFSM lookupOrCreateFsm(final LLCSegmentName segmentName, String msgType,
      long offset) {
    final String segmentNameStr = segmentName.getSegmentName();
    SegmentCompletionFSM fsm = _fsmMap.get(segmentNameStr);
    if (fsm == null) {
      // Look up propertystore to see if this is a completed segment
      ZNRecord segment;
      try {
        // TODO if we keep a list of last few committed segments, we don't need to go to zk for this.
        LLCRealtimeSegmentZKMetadata segmentMetadata = _segmentManager.getRealtimeSegmentZKMetadata(
            segmentName.getTableName(), segmentName.getSegmentName());
        if (segmentMetadata.getStatus().equals(CommonConstants.Segment.Realtime.Status.DONE)) {
          // Best to go through the state machine for this case as well, so that all code regarding state handling is in one place
          // Also good for synchronization, because it is possible that multiple threads take this path, and we don't want
          // multiple instances of the FSM to be created for the same commit sequence at the same time.
          final long endOffset = segmentMetadata.getEndOffset();
          fsm = new SegmentCompletionFSM(_segmentManager, this, segmentName, segmentMetadata.getNumReplicas(), endOffset);
        } else {
          // Segment is finalizing, and this is the first one to respond. Create an entry
          fsm = new SegmentCompletionFSM(_segmentManager, this, segmentName, segmentMetadata.getNumReplicas());
        }
        LOGGER.info("Created FSM {}", fsm);
        _fsmMap.put(segmentNameStr, fsm);
      } catch (Exception e) {
        // Server gone wonky. Segment does not exist in propstore
        LOGGER.error("Exception reading segment read from propertystore {}", segmentNameStr, e);
        throw new RuntimeException("Segment read from propertystore " + segmentNameStr, e);
      }
    }
    return fsm;
  }

  public SegmentCompletionProtocol.Response segmentConsumed(final String segmentNameStr, final String instanceId, final long offset) {
    if (!_helixManager.isLeader()) {
      return SegmentCompletionProtocol.RESP_NOT_LEADER;
    }
    LLCSegmentName segmentName = new LLCSegmentName(segmentNameStr);
    SegmentCompletionFSM fsm = lookupOrCreateFsm(segmentName, SegmentCompletionProtocol.MSG_TYPE_CONSUMED, offset);
    SegmentCompletionProtocol.Response response = fsm.segmentConsumed(instanceId, offset);
    if (fsm.isDone()) {
      LOGGER.info("Removing FSM (if present):{}", fsm.toString());
      _fsmMap.remove(segmentNameStr);
    }
    return response;
  }

  public SegmentCompletionProtocol.Response segmentCommit(final String segmentNameStr, final String instanceId, final long offset) {
    if (!_helixManager.isLeader()) {
      return SegmentCompletionProtocol.RESP_NOT_LEADER;
    }
    LLCSegmentName segmentName = new LLCSegmentName(segmentNameStr);
    SegmentCompletionFSM fsm = lookupOrCreateFsm(segmentName, SegmentCompletionProtocol.MSG_TYPE_CONSUMED, offset);
    SegmentCompletionProtocol.Response response = fsm.segmentCommit(instanceId, offset);
    if (fsm.isDone()) {
      LOGGER.info("Removing FSM (if present):{}", fsm.toString());
      _fsmMap.remove(segmentNameStr);
    }
    return response;
  }

  private static class SegmentCompletionFSM {
    // We will have some variation between hosts, so we add 10% to the max hold time to pick a winner.
    // If there is more than 10% variation, then it is handled as an error case (i.e. the first few to
    // come in will have a winner, and the later ones will just download the segment)
    public static final long  MAX_TIME_TO_PICK_WINNER_MS =
      SegmentCompletionProtocol.MAX_HOLD_TIME_MS + (SegmentCompletionProtocol.MAX_HOLD_TIME_MS / 10);

    // Once we pick a winner, the winner may get notified in the next call, so add one hold time plus some.
    public static final long MAX_TIME_TO_NOTIFY_WINNER_MS = MAX_TIME_TO_PICK_WINNER_MS +
      SegmentCompletionProtocol.MAX_HOLD_TIME_MS + (SegmentCompletionProtocol.MAX_HOLD_TIME_MS / 10);

    // Once the winner is notified, the are expected to start right away. At this point, it is the segment commit
    // time that we need to consider.
    // We may need to add some time here to allow for getting the lock? For now 0
    // We may need to add some time for the committer come back to us? For now 0.
    public static final long MAX_TIME_ALLOWED_TO_COMMIT_MS = MAX_TIME_TO_NOTIFY_WINNER_MS + SegmentCompletionProtocol.MAX_SEGMENT_COMMIT_TIME_MS;

    public final Logger LOGGER;

    State _state = State.HOLDING;   // Always start off in HOLDING state.
    final long _startTime;
    private final LLCSegmentName _segmentName;
    private final int _numReplicas;
    private final Map<String, Long> _commitStateMap;
    private long _winningOffset = -1L;
    private String _winner;
    private final PinotLLCRealtimeSegmentManager _segmentManager;
    private final SegmentCompletionManager _segmentCompletionManager;

    public SegmentCompletionFSM(PinotLLCRealtimeSegmentManager segmentManager,
        SegmentCompletionManager segmentCompletionManager, LLCSegmentName segmentName, int numReplicas,
        long winningOffset) {
      // Constructor used when we get an event after a segment is committed.
      this(segmentManager, segmentCompletionManager, segmentName, numReplicas);
      _state = State.COMMITTED;
      _winningOffset = winningOffset;
      _winner = "UNKNOWN";
    }

    @Override
      public String toString() {
        return "{" + _segmentName.getSegmentName() + "," + _state + "," + _startTime + "," + _winner + "," + _winningOffset + "}";
      }

    public SegmentCompletionFSM(PinotLLCRealtimeSegmentManager segmentManager,
        SegmentCompletionManager segmentCompletionManager, LLCSegmentName segmentName, int numReplicas) {
      _segmentName = segmentName;
      _numReplicas = numReplicas;
      _segmentManager = segmentManager;
      _commitStateMap = new HashMap<>(_numReplicas);
      _segmentCompletionManager = segmentCompletionManager;
      _startTime = _segmentCompletionManager.getCurrentTimeMs();
      LOGGER = LoggerFactory.getLogger("SegmentFinalizerFSM_"  + segmentName.getSegmentName());
    }

    public boolean isDone() {
      return _state.equals(State.COMMITTED) || _state.equals(State.ABORTED);
    }

    /*
     * We just heard from a server that it is in HOLDING state, and is reporting the offset
     * that the server is at. Since multiple servers can come in at the same time for this segment,
     * we need to synchronize on the FSM to handle the messages. The processing time itself is small,
     * so we should be OK with this synchronization.
     */
    public SegmentCompletionProtocol.Response segmentConsumed(String instanceId, long offset) {
      final long now = _segmentCompletionManager.getCurrentTimeMs();
      // We can synchronize the entire block for the SegmentConsumed message.
      synchronized (this) {
        LOGGER.info("Processing segmentConsumed({}, {})", instanceId, offset);
        _commitStateMap.put(instanceId, offset);
        SegmentCompletionProtocol.Response response = null;
        switch (_state) {
          // If we have waited "enough", or we have all replicas reported, then we should pick the winner.
          // Otherwise, we ask the server that is reporting to come back again later until one of these conditions hold.
          case HOLDING:
            return HOLDING__consumed(instanceId, offset, now);

          // We have already decided who the committer is, but have not let them know yet. If this is the committer that
          // we decided, then respond back with COMMIT. Otherwise, if the offset is smaller, respond back with a CATCHUP.
          // Otherwise, just have the server HOLD. Since the segment is not committed yet, we cannot ask them to KEEP or
          // DISCARD etc. If the committer fails for any reason, this one may become the new committer!
          case COMMITTER_DECIDED: // This must be a retransmit
            if (offset > _winningOffset) {
              abortAndReturnHold(now, instanceId, offset);
            }
            return COMMITTER_DECIDED__consumed(instanceId, offset, now);

          case COMMITTER_NOTIFIED:
            return COMMITTER_NOTIFIED__consumed(instanceId, offset, now);

          case COMMITTER_UPLOADING:
            return COMMITTER_UPLOADING__consumed(instanceId, offset, now);

          case COMMITTING:
            return COMMITTING__consumed(instanceId, offset, now);

          case COMMITTED:
            return COMMITTED__consumed(instanceId, offset);

          case ABORTED:
            // FSM has been aborted, just return HOLD
            LOGGER.info("{}:ABORT for instance={} offset={}", _state, instanceId, offset);
            response = new SegmentCompletionProtocol.Response(SegmentCompletionProtocol.ControllerResponseStatus.HOLD, offset);
            return response;

          default:
            LOGGER.info("{}:FAILED for instance={} offset={}", _state, instanceId, offset);
            return SegmentCompletionProtocol.RESP_FAILED;
        }
      }
    }

    private SegmentCompletionProtocol.Response hold(String instanceId, long offset) {
      LOGGER.info("{}:HOLD for instance={} offset={}", _state, instanceId, offset);
      return new SegmentCompletionProtocol.Response(SegmentCompletionProtocol.ControllerResponseStatus.HOLD, offset);
    }

    private SegmentCompletionProtocol.Response abortAndReturnHold(long now, String instanceId, long offset) {
      _state = State.ABORTED;
      LOGGER.warn("{}:Aborting FSM because it is too late instance={} offset={} now={} start={}", _state, instanceId,
          offset, now, _startTime);
      return new SegmentCompletionProtocol.Response(SegmentCompletionProtocol.ControllerResponseStatus.HOLD, offset);
    }

    private SegmentCompletionProtocol.Response abortIfTooLateAndReturnHold(long now, String instanceId, long offset) {
      if (now > _startTime + MAX_TIME_ALLOWED_TO_COMMIT_MS) {
        return abortAndReturnHold(now, instanceId, offset);
      }
      return null;
    }
    private SegmentCompletionProtocol.Response COMMITTED__consumed(String instanceId, long offset) {
      SegmentCompletionProtocol.Response
          response;// Server reporting an offset on an already completed segment. Depending on the offset, either KEEP or DISCARD.
      if (offset == _winningOffset) {
        // Need to return KEEP
        LOGGER.info("{}:KEEP for instance={} offset={}", _state, instanceId, offset);
        response = new SegmentCompletionProtocol.Response(SegmentCompletionProtocol.ControllerResponseStatus.KEEP, offset);
      } else {
        // Return DISCARD. It is hard to say how long the server will take to complete things.
        LOGGER.info("{}:DISCARD for instance={} offset={}", _state, instanceId, offset);
        response = SegmentCompletionProtocol.RESP_DISCARD;
      }
      return response;
    }

    private SegmentCompletionProtocol.Response COMMITTER_NOTIFIED__consumed(String instanceId, long offset, long now) {
      SegmentCompletionProtocol.Response response;
      // We have already picked a winner, and may or many not have heard from them.
      // Common case here is that another server is coming back to us with its offset. We either respond back with HOLD or CATCHUP.
      // It may be that we never heard from the committer, or the committer is taking too long to commit the segment.
      // In that case, we abort the FSM and start afresh (i.e, return HOLD).
      // If the winner is coming back again, then we have some more conditions to look at.
      response = abortIfTooLateAndReturnHold(now, instanceId, offset);
      if (response != null) {
        return response;
      }
      if (instanceId.equals(_winner)) {
        // Winner is coming back to with a HOLD. Perhaps the commit call failed in the winner for some reason
        // Allow them to be winner again.
        if (offset == _winningOffset) {
          LOGGER.info("{}:Commit for instance={} offset={}", _state, instanceId, offset);
          response = new SegmentCompletionProtocol.Response(SegmentCompletionProtocol.ControllerResponseStatus.COMMIT, offset);
        } else {
          // Something seriously wrong. Abort the transaction
          _state = State.ABORTED;
          response = SegmentCompletionProtocol.RESP_DISCARD;
          LOGGER.warn("{}:Abort for instance={} offset={}", _state, instanceId, offset);
        }
      } else {
        // Common case: A different instance is reporting.
        if (offset == _winningOffset) {
          // Wait until winner has posted the segment before asking this server to KEEP the segment.
          response = hold(instanceId, offset);
        } else if (offset < _winningOffset) {
          LOGGER.info("{}:CATCHUP for instance={} offset={}", _state, instanceId, offset);
          response = new SegmentCompletionProtocol.Response(SegmentCompletionProtocol.ControllerResponseStatus.CATCH_UP,
              _winningOffset);
        } else {
          // We have not yet committed, so ask the new responder to hold. They may be the new leader in case the
          // committer fails.
          response = hold(instanceId, offset);
        }
      }
      return response;
    }

    private SegmentCompletionProtocol.Response COMMITTER_UPLOADING__consumed(String instanceId, long offset, long now) {
      return common_consumed(instanceId, offset, now);
    }

    private SegmentCompletionProtocol.Response COMMITTING__consumed(String instanceId, long offset, long now) {
      return common_consumed(instanceId, offset, now);
    }

    // A common method when the state is > COMMITTER_NOTIFIED.
    private SegmentCompletionProtocol.Response common_consumed(String instanceId, long offset, long now) {
      SegmentCompletionProtocol.Response response;
      // We have already picked a winner, and may or many not have heard from them.
      // Common case here is that another server is coming back to us with its offset. We either respond back with HOLD or CATCHUP.
      // It may be that we never heard from the committer, or the committer is taking too long to commit the segment.
      // In that case, we abort the FSM and start afresh (i.e, return HOLD).
      // If the winner is coming back again, then we have some more conditions to look at.
      response = abortIfTooLateAndReturnHold(now, instanceId, offset);
      if (response != null) {
        return null;
      }
      if (instanceId.equals(_winner)) {
        // The winner is coming back to report its offset. Take a decision based on the offset reported, and whether we
        // already notified them
        // Winner is supposedly already in the commit call. Something wrong.
        LOGGER.warn("{}:Aborting FSM because winner is reporting a segment while it is also committing instance={} offset={} now={}",
            _state, instanceId, offset, now);
        // Ask them to hold, just in case the committer fails for some reason..
        return abortAndReturnHold(now, instanceId, offset);
      } else {
        // Common case: A different instance is reporting.
        if (offset == _winningOffset) {
          // Wait until winner has posted the segment before asking this server to KEEP the segment.
          response = hold(instanceId, offset);
        } else if (offset < _winningOffset) {
          LOGGER.info("{}:CATCHUP for instance={} offset={}", _state, instanceId, offset);
          response = new SegmentCompletionProtocol.Response(SegmentCompletionProtocol.ControllerResponseStatus.CATCH_UP,
              _winningOffset);
        } else {
          // We have not yet committed, so ask the new responder to hold. They may be the new leader in case the
          // committer fails.
          response = hold(instanceId, offset);
        }
      }
      return response;
    }

    private SegmentCompletionProtocol.Response COMMITTER_DECIDED__consumed(String instanceId, long offset, long now) {
      SegmentCompletionProtocol.Response response;
      if (_winner.equals(instanceId)) {
        if (_winningOffset == offset) {
          LOGGER.info("{}:Committer notified winner instance={} offset={}", _state, instanceId, offset);
          _state = State.COMMITTER_NOTIFIED;
          response = new SegmentCompletionProtocol.Response(SegmentCompletionProtocol.ControllerResponseStatus.COMMIT, offset);
        } else {
          // Winner coming back with a different offset.
          LOGGER.warn("{}:Winner coming back with different offset for instance={} offset={} prevWinnOffset={}",
              _state, instanceId, offset, _winningOffset);
          response = abortAndReturnHold(now, instanceId, offset);
        }
      } else  if (offset == _winningOffset) {
        // Wait until winner has posted the segment.
        response = hold(instanceId, offset);
      } else {
        LOGGER.info("{}:Cathing up for instance={}  offset={}", _state, instanceId, _winningOffset);
        response = new SegmentCompletionProtocol.Response(SegmentCompletionProtocol.ControllerResponseStatus.CATCH_UP, _winningOffset);
      }
      if (now > _startTime + MAX_TIME_TO_NOTIFY_WINNER_MS) {
        // Winner never got back to us. Abort the commit protocol and start afresh.
        // We can potentially optimize here to see if this instance has the highest so far, and re-elect them to
        // be winner, but for now, we will abort it and restart
        response = abortAndReturnHold(now, instanceId, offset);
      }
      return response;
    }

    private SegmentCompletionProtocol.Response HOLDING__consumed(String instanceId, long offset, long now) {
      SegmentCompletionProtocol.Response response;
      if (now > _startTime + MAX_TIME_TO_PICK_WINNER_MS || _commitStateMap.size() == _numReplicas) {
        LOGGER.info("{}:Picking winner time={} size={}", _state, now-_startTime, _commitStateMap.size());
        pickWinner(instanceId);
        if (_winner.equals(instanceId)) {
          LOGGER.info("{}:Committer notified winner instance={} offset={}", _state, instanceId, offset);
          _state = State.COMMITTER_NOTIFIED;
          response = new SegmentCompletionProtocol.Response(SegmentCompletionProtocol.ControllerResponseStatus.COMMIT, offset);
        } else {
          LOGGER.info("{}:Committer decided winner={} offset={}", _state, _winner, _winningOffset);
          _state = State.COMMITTER_DECIDED;
          response = new SegmentCompletionProtocol.Response(SegmentCompletionProtocol.ControllerResponseStatus.CATCH_UP, _winningOffset);
        }
      } else {
        response = new SegmentCompletionProtocol.Response(SegmentCompletionProtocol.ControllerResponseStatus.HOLD, offset);
        LOGGER.info("{}:Holding for instance={}  offset={}", _state, instanceId, offset);
      }
      return response;
    }

    /*
     * A server is trying to commit a segment. Hopefully, this is the winner that we picked.
     * We should be in the COMMITTER_NOTIFIED state at this point, anything else should be handled
     * mostly with a HOLD return and perhaps aborting the FSM.
     *
     * We need to synchronize the processing of the message on the FSM, but then we can (must) release
     * synchronization when the segment is being uploaded, and then re-acquire synchronization after
     * the upload.
     *
     * Of course, we need a state-check after we re-acquire.
     */
    public SegmentCompletionProtocol.Response segmentCommit(String instanceId, long offset) {
      long now = _segmentCompletionManager.getCurrentTimeMs();
      SegmentCompletionProtocol.Response response = null;
      // Pre-process, prepare for uploading segment.
        LOGGER.info("Processing segmentCommit({}, {})", instanceId, offset);
        switch (_state) {
          case HOLDING:
            return HOLDING__commit(instanceId, offset, now);
          case COMMITTER_DECIDED:
            return COMMITTER_DECIDED__commit(instanceId, offset, now);
          case COMMITTER_NOTIFIED:
            synchronized (this) {
              response = checkBadCommitRequest(instanceId, offset, now);
              if (response != null) {
                return response;
              }
              LOGGER.info("{}:Uploading for instance={} offset={}", _state, instanceId, offset);
              _state = State.COMMITTER_UPLOADING;
            }

            // Accept commit. Need to release lock below while getting the segment.
            boolean success = false;
            try {
              // TODO we need to keep a handle to the uploader so that we can stop it via the fsm.stop() call.
              success = saveTheSegment();
            } catch (Exception e) {
              LOGGER.error("Segment upload failed");
            }
            if (!success) {
              // Committer failed when posting the segment. Start over.
              _state = State.ABORTED;
              return SegmentCompletionProtocol.RESP_FAILED;
            }

            /*
             * Before we enter this code loop, it is possible that others threads have asked for this FSM, or perhaps
             * we just took too long and this FSM has already been aborted or committed by someone else.
             */
            synchronized (this) {
              response = updateZk(instanceId, offset);
              if (response != null) {
                return response;
              }
            }
            return new SegmentCompletionProtocol.Response(SegmentCompletionProtocol.ControllerResponseStatus.FAILED, -1L);

          case COMMITTER_UPLOADING:
            return COMMITTER_UPLOADING__commit(instanceId, offset, now);
          case COMMITTING:
            return COMMITTING__commit(instanceId, offset, now);
          case COMMITTED:
            return COMMITTED__commit(instanceId, offset);
          case ABORTED:
            LOGGER.info("{}:ABORT for instance={} offset={}", _state, instanceId, offset);
            return new SegmentCompletionProtocol.Response(SegmentCompletionProtocol.ControllerResponseStatus.HOLD, offset);
          default:
            LOGGER.info("{}:FAILED for instance={} offset={}", _state, instanceId, offset);
            return SegmentCompletionProtocol.RESP_FAILED;
        }
    }

    private SegmentCompletionProtocol.Response updateZk(String instanceId, long offset) {
      boolean success;
      if (!_state.equals(State.COMMITTER_UPLOADING)) {
        // State changed while we were out of sync. return a failed commit.
        LOGGER.warn("Segment done during upload: state={} segment={} winner={} winningOffset={}",
            _state, _segmentName.getSegmentName(), _winner, _winningOffset);
        return SegmentCompletionProtocol.RESP_FAILED;
      }
      LOGGER.info("Committing segment {} at offset {} winner {}", _segmentName.getSegmentName(), offset, instanceId);
      _state = State.COMMITTING;
      success = _segmentManager.commitSegment(_segmentName.getTableName(), _segmentName.getSegmentName(), _winningOffset);
      if (success) {
        _state = State.COMMITTED;
        LOGGER.info("Committed segment {} at offset {} winner {}", _segmentName.getSegmentName(), offset, instanceId);
        return SegmentCompletionProtocol.RESP_COMMIT_SUCCESS;
      }
      return null;
    }

    private synchronized  SegmentCompletionProtocol.Response COMMITTED__commit(String instanceId, long offset) {
      SegmentCompletionProtocol.Response response;
      if (offset == _winningOffset) {
        LOGGER.info("{}:KEEP for instance={} offset={}", _state, instanceId, offset);
        response = new SegmentCompletionProtocol.Response(SegmentCompletionProtocol.ControllerResponseStatus.KEEP, offset);
      } else {
        LOGGER.info("{}:DISCARD for instance={} offset={}", _state, instanceId, offset);
        response = SegmentCompletionProtocol.RESP_DISCARD;
      }
      return response;
    }

    private synchronized SegmentCompletionProtocol.Response COMMITTER_UPLOADING__commit(String instanceId, long offset,
        long now) {
      LOGGER.info("Processing segmentCommit({}, {})", instanceId, offset);
      return uploadingCommitting_commit(instanceId, offset, now);
    }

    private synchronized  SegmentCompletionProtocol.Response COMMITTING__commit(String instanceId, long offset,
        long now) {
      LOGGER.info("Processing segmentCommit({}, {})", instanceId, offset);
      return uploadingCommitting_commit(instanceId, offset, now);
    }

    private SegmentCompletionProtocol.Response uploadingCommitting_commit(String instanceId, long offset, long now) {
      SegmentCompletionProtocol.Response response = abortIfTooLateAndReturnHold(now, instanceId, offset);
      if (response != null) {
        return response;
      }
      // Another committer (or same) came in while one was uploading. Ask them to hold in case this one fails.
      return new SegmentCompletionProtocol.Response(SegmentCompletionProtocol.ControllerResponseStatus.HOLD, offset);
    }

    private SegmentCompletionProtocol.Response checkBadCommitRequest(String instanceId, long offset, long now) {
      SegmentCompletionProtocol.Response response = abortIfTooLateAndReturnHold(now, instanceId, offset);
      if (response != null) {
        return response;
      } else  if (instanceId.equals(_winner) && offset != _winningOffset) {
        // Hmm. Committer has been notified, but either a different one is committing, or offset is different
        return abortAndReturnHold(now, instanceId, offset);
      }
      return null;
    }

    private synchronized SegmentCompletionProtocol.Response HOLDING__commit(String instanceId, long offset, long now) {
      LOGGER.info("Processing segmentCommit({}, {})", instanceId, offset);
      return holdingDecided_commit(instanceId, offset, now);
    }

    private synchronized SegmentCompletionProtocol.Response COMMITTER_DECIDED__commit(String instanceId, long offset,
        long now) {
      LOGGER.info("Processing segmentCommit({}, {})", instanceId, offset);
      return holdingDecided_commit(instanceId, offset, now);
    }

    private SegmentCompletionProtocol.Response holdingDecided_commit(String instanceId, long offset, long now) {
      SegmentCompletionProtocol.Response response = abortIfTooLateAndReturnHold(now, instanceId, offset);
      if (response != null) {
        return response;
      }
      // We cannot get a commit if we are in this state, so ask them to hold. Maybe we are starting after a failover.
      // The server will re-send the segmentConsumed message.
      return hold(instanceId, offset);
    }

    // TODO TBD whether the segment is saved here or before entering the FSM.
    private boolean saveTheSegment() throws InterruptedException {
      // XXX: this can fail
      return true;
    }

    // Pick a winner, preferring this instance if tied for highest.
    // Side-effect: Sets the _winner and _winningOffset
    private void pickWinner(String preferredInstance) {
      long maxSoFar = -1;
      String winner = null;
      for (Map.Entry<String, Long> entry : _commitStateMap.entrySet()) {
        if (entry.getValue() > maxSoFar) {
          maxSoFar = entry.getValue();
          winner = entry.getKey();
        }
      }
      _winningOffset = maxSoFar;
      if (_commitStateMap.get(preferredInstance) == maxSoFar) {
        winner = preferredInstance;
      }
      _winner =  winner;
    }
  }
}

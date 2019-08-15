from collections import defaultdict

import math
import sys
from typing import Tuple, List, Dict, NamedTuple, Iterable

from sortedcontainers import SortedListWithKey

from common.exceptions import LogicError
from plenum.common.config_util import getConfig
from plenum.common.event_bus import InternalBus, ExternalBus
from plenum.common.messages.internal_messages import NeedMasterCatchup, NeedBackupCatchup, CheckpointStabilized
from plenum.common.messages.node_messages import Checkpoint, Ordered, CheckpointState
from plenum.common.metrics_collector import MetricsName, MetricsCollector, NullMetricsCollector
from plenum.common.stashing_router import StashingRouter
from plenum.common.util import updateNamedTuple, SortedDict, firstKey
from plenum.server.consensus.consensus_shared_data import ConsensusSharedData
from plenum.server.consensus.metrics_decorator import measure_consensus_time
from plenum.server.consensus.msg_validator import CheckpointMsgValidator
from plenum.server.database_manager import DatabaseManager
from plenum.server.replica_stasher import ReplicaStasher
from plenum.server.replica_validator_enums import DISCARD, PROCESS
from stp_core.common.log import getlogger


class CheckpointService:
    STASHED_CHECKPOINTS_BEFORE_CATCHUP = 1

    # TODO: Remove view_no from key after implementing INDY-1336
    CheckpointKey = NamedTuple('CheckpointKey',
                               [('view_no', int), ('pp_seq_no', int), ('digest', str)])

    def __init__(self, data: ConsensusSharedData, bus: InternalBus, network: ExternalBus,
                 stasher: StashingRouter, db_manager: DatabaseManager, old_stasher: ReplicaStasher,
                 metrics: MetricsCollector = NullMetricsCollector(),):
        self._data = data
        self._bus = bus
        self._network = network
        self._stasher = stasher
        self._validator = CheckpointMsgValidator(self._data)
        self._db_manager = db_manager
        self.metrics = metrics

        # Received checkpoints, mapping CheckpointKey -> List(node_alias)
        self._received_checkpoints = defaultdict(list)  # type: Dict[CheckpointService.CheckpointKey, List[str]]

        # Stashed checkpoints for each view. The key of the outermost
        # dictionary is the view_no, value being a dictionary with key as the
        # range of the checkpoint and its value again being a mapping between
        # senders and their sent checkpoint
        # Dict[view_no, Dict[(seqNoStart, seqNoEnd),  Dict[sender, Checkpoint]]]
        self._stashed_recvd_checkpoints = {}

        self._config = getConfig()
        self._logger = getlogger()

        self._old_stasher = old_stasher

        # self._stasher.subscribe(Checkpoint, self.process_checkpoint)
        # self._stasher.subscribe_to(network)
        #
        # self._bus.subscribe(Ordered, self.process_ordered)

    @property
    def view_no(self):
        return self._data.view_no

    @property
    def is_master(self):
        return self._data.is_master

    @property
    def last_ordered_3pc(self):
        return self._data.last_ordered_3pc

    @measure_consensus_time(MetricsName.PROCESS_CHECKPOINT_TIME,
                            MetricsName.BACKUP_PROCESS_CHECKPOINT_TIME)
    def process_checkpoint(self, msg: Checkpoint, sender: str) -> bool:
        """
        Process checkpoint messages
        :return: whether processed (True) or stashed (False)
        """
        self._logger.info('{} processing checkpoint {} from {}'.format(self, msg, sender))
        result, reason = self._validator.validate(msg)
        if result == DISCARD:
            self.discard(msg, reason, sender)
            return False
        elif result == PROCESS:
            return self._do_process_checkpoint(msg, sender)
        else:
            self._logger.debug("{} stashing checkpoint message {} with "
                               "the reason: {}".format(self, msg, reason))
            self._old_stasher.stash((msg, sender), result)
            return False

    def _do_process_checkpoint(self, msg: Checkpoint, sender: str) -> bool:
        """
        Process checkpoint messages

        :return: whether processed (True) or stashed (False)
        """
        key = self.CheckpointKey(view_no=msg.viewNo,
                                 pp_seq_no=msg.seqNoEnd,
                                 digest=msg.digest)

        self._received_checkpoints[key].append(sender)
        self._try_to_stabilize_checkpoint(key)
        self._start_catchup_if_needed(key)

        # TODO: Do we really need to return bool?
        return True

    def process_ordered(self, ordered: Ordered):
        for batch_id in reversed(self._data.preprepared):
            if batch_id.pp_seq_no == ordered.ppSeqNo:
                self._add_to_checkpoint(batch_id.pp_seq_no,
                                        batch_id.pp_digest,
                                        ordered.ledgerId,
                                        batch_id.view_no,
                                        ordered.auditTxnRootHash)
                return
        raise LogicError("CheckpointService | Can't process Ordered msg because "
                         "ppSeqNo {} not in preprepared".format(ordered.ppSeqNo))

    def _start_catchup_if_needed(self, key: CheckpointKey):
        if self._have_own_checkpoint(key):
            return

        unknown_stabilized = self._unknown_stabilized_checkpoints()
        checkpoints_lag = len(unknown_stabilized)
        if checkpoints_lag <= self.STASHED_CHECKPOINTS_BEFORE_CATCHUP:
            return

        last_key = sorted(unknown_stabilized, key=lambda v: (v.view_no, v.pp_seq_no))[-1]

        if self._data.is_master and not self._data.is_primary:
            # Old code was also moving watermarks BEFORE catchup, but seemingly
            # it doesn't make sense, so removed this code. Leaving this comment
            # just in case we run into problems with catch up.
            self._logger.display('{} has lagged for {} checkpoints so the catchup procedure starts'.
                                 format(self, checkpoints_lag))
            self._bus.send(NeedMasterCatchup())
        else:
            self._logger.info('{} has lagged for {} checkpoints so adjust last_ordered_3pc to {}, '
                              'shift watermarks and clean collections'.
                              format(self, checkpoints_lag, last_key.pp_seq_no))
            # Adjust last_ordered_3pc, shift watermarks, clean operational
            # collections and process stashed messages which now fit between
            # watermarks
            # TODO: Now checkpoints from future view are stashed, so last_key won't be from
            #  future view as well. However checkpoints are going to become view-agnostic,
            #  and at that point this code should be reconsidered.
            key_3pc = (self.view_no, last_key.pp_seq_no)
            self._bus.send(NeedBackupCatchup(inst_id=self._data.inst_id,
                                             caught_up_till_3pc=key_3pc))
            self.caught_up_till_3pc(key_3pc)

    def gc_before_new_view(self):
        self._reset_checkpoints()
        self._remove_stashed_checkpoints(till_3pc_key=(self.view_no, 0))

    def caught_up_till_3pc(self, caught_up_till_3pc):
        self._reset_checkpoints()
        self._remove_stashed_checkpoints(till_3pc_key=caught_up_till_3pc)
        self.update_watermark_from_3pc()

    def catchup_clear_for_backup(self):
        self._reset_checkpoints()
        self._remove_stashed_checkpoints()
        self.set_watermarks(low_watermark=0,
                            high_watermark=sys.maxsize)

    def _add_to_checkpoint(self, ppSeqNo, digest, ledger_id, view_no, audit_txn_root_hash):
        for (s, e) in self._checkpoint_state.keys():
            if s <= ppSeqNo <= e:
                state = self._checkpoint_state[s, e]  # type: CheckpointState
                state.digests.append(digest)
                state = updateNamedTuple(state, seqNo=ppSeqNo)
                self._checkpoint_state[s, e] = state
                break
        else:
            s, e = ppSeqNo, math.ceil(ppSeqNo / self._config.CHK_FREQ) * self._config.CHK_FREQ
            self._logger.debug("{} adding new checkpoint state for {}".format(self, (s, e)))
            state = CheckpointState(ppSeqNo, [digest, ], None, {}, False)
            self._checkpoint_state[s, e] = state

        if state.seqNo == e:
            if len(state.digests) == self._config.CHK_FREQ:
                self._do_checkpoint(state, s, e, ledger_id, view_no, audit_txn_root_hash)
            self._process_stashed_checkpoints((s, e), view_no)

    @measure_consensus_time(MetricsName.SEND_CHECKPOINT_TIME,
                            MetricsName.BACKUP_SEND_CHECKPOINT_TIME)
    def _do_checkpoint(self, state, s, e, ledger_id, view_no, audit_txn_root_hash):
        # TODO CheckpointState/Checkpoint is not a namedtuple anymore
        # 1. check if updateNamedTuple works for the new message type
        # 2. choose another name

        # TODO: This is hack of hacks, should be removed when refactoring is complete
        if not self.is_master and audit_txn_root_hash is None:
            audit_txn_root_hash = "7RJ5bkAKRy2CCvarRij2jiHC16SVPjHcrpVdNsboiQGv"

        state = updateNamedTuple(state,
                                 digest=audit_txn_root_hash,
                                 digests=[])
        self._checkpoint_state[s, e] = state
        self._logger.info("{} sending Checkpoint {} view {} checkpointState digest {}. Ledger {} "
                          "txn root hash {}. Committed state root hash {} Uncommitted state root hash {}".
                          format(self, (s, e), view_no, state.digest, ledger_id,
                                 self._db_manager.get_txn_root_hash(ledger_id),
                                 self._db_manager.get_state_root_hash(ledger_id,
                                                                      committed=True),
                                 self._db_manager.get_state_root_hash(ledger_id,
                                                                      committed=False)))
        checkpoint = Checkpoint(self._data.inst_id, view_no, s, e, state.digest)
        self._network.send(checkpoint)
        self._data.checkpoints.append(checkpoint)

    def _try_to_stabilize_checkpoint(self, key: CheckpointKey):
        if not self._have_quorum_on_received_checkpoint(key):
            return

        if not self._have_own_checkpoint(key):
            return

        self._mark_checkpoint_stable(key.pp_seq_no)

    def _mark_checkpoint_stable(self, pp_seq_no):
        self._data.stable_checkpoint = pp_seq_no

        self._data.checkpoints = \
            SortedListWithKey([c for c in self._data.checkpoints if c.seqNoEnd >= end_seq_no],
                              key=lambda checkpoint: checkpoint.seqNoEnd)

        self.set_watermarks(low_watermark=pp_seq_no)

        self._remove_stashed_checkpoints(till_3pc_key=(self.view_no, seq_no))
        self._bus.send(CheckpointStabilized(self._data.inst_id, (self.view_no, seq_no)))  # call OrderingService.l_gc()
        self._logger.info("{} marked stable checkpoint {}".format(self, (s, e)))

    def _stash_checkpoint(self, ck: Checkpoint, sender: str):
        self._logger.debug('{} stashing {} from {}'.format(self, ck, sender))
        seqNoStart, seqNoEnd = ck.seqNoStart, ck.seqNoEnd
        if ck.viewNo not in self._stashed_recvd_checkpoints:
            self._stashed_recvd_checkpoints[ck.viewNo] = {}
        stashed_for_view = self._stashed_recvd_checkpoints[ck.viewNo]
        if (seqNoStart, seqNoEnd) not in stashed_for_view:
            stashed_for_view[seqNoStart, seqNoEnd] = {}
        stashed_for_view[seqNoStart, seqNoEnd][sender] = ck

    def _stashed_checkpoints_with_quorum(self):
        end_pp_seq_numbers = []
        quorum = self._data.quorums.checkpoint
        for (_, seq_no_end), senders in self._stashed_recvd_checkpoints.get(
                self.view_no, {}).items():
            if quorum.is_reached(len(senders)):
                end_pp_seq_numbers.append(seq_no_end)
        return sorted(end_pp_seq_numbers)

    def _process_stashed_checkpoints(self, key, view_no):
        # Remove all checkpoints from previous views if any
        self._remove_stashed_checkpoints(till_3pc_key=(self.view_no, 0))

        if key not in self._stashed_recvd_checkpoints.get(view_no, {}):
            self._logger.trace("{} have no stashed checkpoints for {}")
            return

        # Get a snapshot of all the senders of stashed checkpoints for `key`
        senders = list(self._stashed_recvd_checkpoints[view_no][key].keys())
        total_processed = 0
        consumed = 0

        for sender in senders:
            # Check if the checkpoint from `sender` is still in
            # `stashed_recvd_checkpoints` because it might be removed from there
            # in case own checkpoint was stabilized when we were processing
            # stashed checkpoints from previous senders in this loop
            if view_no in self._stashed_recvd_checkpoints \
                    and key in self._stashed_recvd_checkpoints[view_no] \
                    and sender in self._stashed_recvd_checkpoints[view_no][key]:
                if self.process_checkpoint(
                        self._stashed_recvd_checkpoints[view_no][key].pop(sender),
                        sender):
                    consumed += 1
                # Note that if `process_checkpoint` returned False then the
                # checkpoint from `sender` was re-stashed back to
                # `stashed_recvd_checkpoints`
                total_processed += 1

        # If we have consumed stashed checkpoints for `key` from all the
        # senders then remove entries which have become empty
        if view_no in self._stashed_recvd_checkpoints \
                and key in self._stashed_recvd_checkpoints[view_no] \
                and len(self._stashed_recvd_checkpoints[view_no][key]) == 0:
            del self._stashed_recvd_checkpoints[view_no][key]
            if len(self._stashed_recvd_checkpoints[view_no]) == 0:
                del self._stashed_recvd_checkpoints[view_no]

        restashed = total_processed - consumed
        self._logger.info('{} processed {} stashed checkpoints for {}, '
                          '{} of them were stashed again'.
                          format(self, total_processed, key, restashed))

        return total_processed

    def reset_watermarks_before_new_view(self):
        # Reset any previous view watermarks since for view change to
        # successfully complete, the node must have reached the same state
        # as other nodes
        self.set_watermarks(low_watermark=0)

    def should_reset_watermarks_before_new_view(self):
        if self.view_no <= 0:
            return False
        if self.last_ordered_3pc[0] == self.view_no and self.last_ordered_3pc[1] > 0:
            return False
        return True

    def set_watermarks(self, low_watermark: int, high_watermark: int = None):
        self._data.low_watermark = low_watermark
        self._data.high_watermark = self._data.low_watermark + self._config.LOG_SIZE \
            if high_watermark is None else \
            high_watermark

        self._logger.info('{} set watermarks as {} {}'.format(self,
                                                              self._data.low_watermark,
                                                              self._data.high_watermark))
        self._old_stasher.unstash_watermarks()

    def update_watermark_from_3pc(self):
        last_ordered_3pc = self.last_ordered_3pc
        if (last_ordered_3pc is not None) and (last_ordered_3pc[0] == self.view_no):
            self._logger.info("update_watermark_from_3pc to {}".format(last_ordered_3pc))
            self.set_watermarks(last_ordered_3pc[1])
        else:
            self._logger.info("try to update_watermark_from_3pc but last_ordered_3pc is None")

    def _remove_stashed_checkpoints(self, till_3pc_key=None):
        """
        Remove stashed received checkpoints up to `till_3pc_key` if provided,
        otherwise remove all stashed received checkpoints
        """
        if till_3pc_key is None:
            self._stashed_recvd_checkpoints.clear()
            self._logger.info('{} removing all stashed checkpoints'.format(self))
            return

        for view_no in list(self._stashed_recvd_checkpoints.keys()):

            if view_no < till_3pc_key[0]:
                self._logger.info('{} removing stashed checkpoints for view {}'.format(self, view_no))
                del self._stashed_recvd_checkpoints[view_no]

            elif view_no == till_3pc_key[0]:
                for (s, e) in list(self._stashed_recvd_checkpoints[view_no].keys()):
                    if e <= till_3pc_key[1]:
                        self._logger.info('{} removing stashed checkpoints: '
                                          'viewNo={}, seqNoStart={}, seqNoEnd={}'.
                                          format(self, view_no, s, e))
                        del self._stashed_recvd_checkpoints[view_no][(s, e)]
                if len(self._stashed_recvd_checkpoints[view_no]) == 0:
                    del self._stashed_recvd_checkpoints[view_no]

    def _reset_checkpoints(self):
        # That function most probably redundant in PBFT approach,
        # because according to paper, checkpoints cleared only when next stabilized.
        # Avoid using it while implement other services.
        self._checkpoint_state.clear()
        self._data.checkpoints.clear()
        # TODO: change to = 1 in ViewChangeService integration.
        self._data.stable_checkpoint = 0

    def __str__(self) -> str:
        return "{}:{} - checkpoint_service".format(self._data.name, self._data.inst_id)

    # TODO: move to OrderingService as a handler for Cleanup messages
    # def _clear_batch_till_seq_no(self, seq_no):
    #     self._data.preprepared = [pp for pp in self._data.preprepared if pp.ppSeqNo >= seq_no]
    #     self._data.prepared = [p for p in self._data.prepared if p.ppSeqNo >= seq_no]

    def discard(self, msg, reason, sender):
        self._logger.trace("{} discard message {} from {} "
                           "with the reason: {}".format(self, msg, sender, reason))

    def _have_own_checkpoint(self, key: CheckpointKey) -> bool:
        own_checkpoints = self._data.checkpoints.irange_key(min_key=key.pp_seq_no, max_key=key.pp_seq_no)
        return any(cp.viewNo == key.view_no and cp.digest == key.digest for cp in own_checkpoints)

    def _have_quorum_on_received_checkpoint(self, key: CheckpointKey) -> bool:
        votes = self._received_checkpoints[key]
        return self._data.quorums.checkpoint.is_reached(len(votes))

    def _unknown_stabilized_checkpoints(self) -> List[CheckpointKey]:
        return [key for key in self._received_checkpoints
                if self._have_quorum_on_received_checkpoint(key) and not self._have_own_checkpoint(key)]

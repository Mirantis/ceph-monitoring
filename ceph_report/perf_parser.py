# from __future__ import print_function
# import os
# import json
# from typing import List, Dict, IO
#
# import numpy
#
# from cephlib.sensors_rpc_plugin import CephOp
# from cephlib.plot import auto_edges
#
#
# STAGES_INFO = [
#     ("queued_for_pg", "queued"),
#     ("reached_pg", "reached pg"),
#     ("started", "started"),
#     ("commit_queued_for_journal_write", "commit queued\nfor journal"),
#     ("waiting_for_subops", "waiting for subops"),
#     ("write_thread_in_journal_buffer", "in journal buffer"),
#     ("op_commit", "commit"),
#     ("op_applied", "applied"),
#     ("sub_op_applied", "subop applied"),
#     ("journaled_completion_queued", "journaled completion\nqueued"),
#     ("sub_op_commit_rec", "subop commit"),
#     ("sub_op_apply_rec", "subop apply"),
#     ("commit_sent", "commit send"),
#     ("done", "done"),
#     ('waiting_for_rw_locks', 'waiting_for_rw_locks')
# ]
#
#
# def txthist(values, bin_edges=None, hist_width=70):
#     if not bin_edges:
#         bin_edges = auto_edges(values)
#     hist, _ = numpy.histogram(values, bin_edges)
#     hist = hist.astype(numpy.float32) / numpy.amax(hist)
#     bins_labels = bin_edges[:-1]
#     res = ""
#     for vl, rel_count in zip(bins_labels, hist):
#         print(vl, rel_count)
#         res += " {:1.3e} |  ".format(vl) + '#' * int(hist_width * rel_count) + "\n"
#     return res
#
#
# def group_times(times, x_bins=25):
#     """ops must be sorted by initiated_at field"""
#     min_time = times[0]
#     bin_size = (times[-1] - min_time) / float(x_bins)
#     bin_idxs = ((times - min_time) / bin_size).astype(numpy.uint32)
#     bin_idxs[bin_idxs >= x_bins] = x_bins - 1
#
#     # now init_at array holds bin idx
#     idxs = numpy.arange(x_bins)
#     bin_starts = numpy.searchsorted(bin_idxs, idxs)
#     bin_ranges = list(zip(bin_starts[:-1], bin_starts[1:]))
#     bin_ranges.append((bin_ranges[-1][1], len(times)))
#
#     return bin_ranges
#
#
# OSD_OP_PRIMARY_STEPS = sorted([
#      "initiated",
#      "queued_for_pg",
#      "reached_pg",
#      "started",
#      "waiting_for_subops",
#      "commit_queued_for_journal_write",
#      "write_thread_in_journal_buffer",
#      "sub_op_commit_rec_0",
#      "journaled_completion_queued",
#      "op_commit",
#      "op_applied",
#      "sub_op_commit_rec_1",
#      "commit_sent",
#      "done",
# ])
#
#
# OSD_OP_PRIMARY_FSTEPS = [
#      "initiated",
#      "queued_for_pg",
#      "reached_pg",
#      "started",
#      "waiting_for_subops",
#      "commit_queued_for_journal_write",
#      "write_thread_in_journal_buffer",
#      "journaled_completion_queued",
#      "op_commit",
#      "op_applied",
#      "sub_op_commit_rec",
#      "commit_sent",
#      "done",
# ]
#
# OSD_OP_PRIMARY_STEPS_PARENT = {
#      "queued_for_pg": "initiated",
#      "reached_pg": "queued_for_pg",
#      "started": "reached_pg",
#      "waiting_for_subops": "started",
#      "commit_queued_for_journal_write": "waiting_for_subops",
#      "write_thread_in_journal_buffer": "commit_queued_for_journal_write",
#      "sub_op_commit_rec_0": "commit_queued_for_journal_write",
#      "journaled_completion_queued": "write_thread_in_journal_buffer",
#      "op_commit": "write_thread_in_journal_buffer",
#      "op_applied": "write_thread_in_journal_buffer",
#      "sub_op_commit_rec_1": "commit_queued_for_journal_write",
#      "commit_sent": "sub_op_commit_rec_1",
#      "done": "commit_sent"
# }
#
#
# STAGES_PRINTABLE_NAMES = dict(STAGES_INFO)
#
#
# def load_jsops_from_fd(fd: IO[bytes], ops: List[str], tp : str = 'osd_op') -> List[Dict[str, float]]:
#     return parse_ops_timings(json.load(fd), tp, ops)
#
#
# # ----------  OLD OPS CODE  --------------------------------------------------------------------------------------------
#
#
# def load_ops(files, tp='osd_op'):
#     ops = []
#     for name in files:
#         with open(name, 'rb') as fd:
#             for op in load_ops_from_fd(fd, tp=tp):
#                 ops.append(op)
#     return ops
#
#
# def load_ops_from_fd(fd: IO[bytes], size: int = None, tp : str = 'osd_op') -> List[CephOp]:
#     ops = []
#     op_idx = CEPH_OP_DESCR_IDX[tp]
#
#     if size is None:
#         cpos = fd.tell()
#         fd.seek(0, os.SEEK_END)
#         size = fd.tell() - cpos
#         fd.seek(cpos)
#
#     for op in CephSensor.unpack_historic_fd(fd, size):
#         if op.descr_idx == op_idx:
#             ops.append(op)
#
#     return ops
#
#
# def load_stage(ops: List[CephOp], stage_name: str, no_gaps: bool = True) -> numpy.ndarray:
#     step_idx = ALL_STAGES.index(stage_name)
#     values = numpy.array([op.stages[step_idx] for op in ops], dtype=numpy.int32)
#     assert not no_gaps or values.min() > 0, "Some ops has no {0!r} stage".format(stage_name)
#     return values
#
#
# def op2stages_map(op: CephOp, with_extra: bool = False) -> Dict[str, int]:
#     res = {name: op.stages[ALL_STAGES.index(name)] for name in ALL_STAGES}
#     res = {key: val for key, val in res.items() if val != 0}
#
#     if with_extra:
#         for idx, tm in enumerate(op.extra1):
#             res[sub_op_commit + str(idx)] = tm
#
#         for idx, tm in enumerate(op.extra2):
#             res[sub_op_applied + str(idx)] = tm
#
#     return res
#
#
# # ALL_STAGES_IN_ORDER = [json_name for json_name, _ in STAGES_INFO]
# # assert set(STAGES_PRINTABLE_NAMES) == set(ALL_STAGES), str(set(STAGES_PRINTABLE_NAMES).symmetric_difference(ALL_STAGES))
#
#
# def group_ops(ops):
#     assert ops
#     res = {stage: [] for stage in ALL_STAGES_IN_ORDER}
#     for op in ops:
#         for name, tm in zip(ALL_STAGES, op.stages):
#             if tm:
#                 res[name].append(tm)
#         res["subop_commit_rec"].extend(op.extra1)
#         res["subop_apply_rec"].extend(op.extra2)
#
#     return res
#
#

"""Execution layer (§9). Currently holds only the predicted-vs-realized ledger,
which is deliberately built *before* paper execution rather than after it:
without a per-trade record of what the cost model expected against what the
venue actually did, a losing paper run cannot be diagnosed. "The edge decayed"
and "the cost model was always wrong" produce the same equity curve.
"""

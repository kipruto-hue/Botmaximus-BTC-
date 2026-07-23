"""Risk core (Decision & Execution Master Prompt §4).

Hard code, deterministic, standalone. Everything that trades calls INTO this
package and obeys its verdict; nothing may import into it to alter limits.
No LLM, strategy, arbiter, or config path may raise a limit, remove a stop,
or disarm a kill at runtime (constitution §2.1).
"""
from botmaximus.risk.core import RiskCore
from botmaximus.risk.state import OrderIntent, Rejection, SizedOrder

__all__ = ["RiskCore", "OrderIntent", "SizedOrder", "Rejection"]

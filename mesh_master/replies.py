from dataclasses import dataclass
from typing import Optional


@dataclass
class PendingReply:
    text: str
    reason: str = "command"
    chunk_delay: Optional[float] = None
    pre_send_delay: Optional[float] = None

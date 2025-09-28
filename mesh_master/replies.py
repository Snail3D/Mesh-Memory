from dataclasses import dataclass
from typing import Optional


@dataclass
class PendingReply:
    text: str
    reason: str = "command"
    chunk_delay: Optional[float] = None
    pre_send_delay: Optional[float] = None
    follow_up_text: Optional[str] = None
    follow_up_delay: Optional[float] = None

from enum import Enum

class StatusEnum(str, Enum):
    SENT = 'sent'
    ANALYSE = 'analyse'
    REJECTED = 'rejected'
    ACCEPTED = 'accepted'

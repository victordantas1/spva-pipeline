from enum import Enum

class CategoryEnum(str, Enum):
    REMOTE="remote"
    ON_SITE="on-site"
    HYBRID="hybrid"
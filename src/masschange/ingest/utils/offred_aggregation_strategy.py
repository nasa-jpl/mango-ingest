
from enum import Enum
class OffredAggregationStrategy(Enum):
    NONE = "NONE"             # 1. Do not aggregate
    AFTER_EACH = "AFTER_EACH" # 2. Aggregate after each individual un-zipped file

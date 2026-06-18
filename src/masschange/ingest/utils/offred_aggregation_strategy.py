
from enum import Enum
class OffredAggregationStrategy(Enum):
    NONE = "NONE"             # 1. Do not aggregate
    AFTER_ALL = "AFTER_ALL"   # 2. Aggregate after all files in zip file are ingested
    AFTER_EACH = "AFTER_EACH" # 3. Aggregate after each individual un-zipped file

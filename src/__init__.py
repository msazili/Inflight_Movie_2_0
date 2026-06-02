from .utils import create_spark_session

from .etl.bronze import trigger_bronze
from .etl.silver import trigger_silver
from .etl.gold import trigger_gold

from .analysis import (
    analyze_top_genres,
    find_hidden_gems,
    select_inflight_catalog
)

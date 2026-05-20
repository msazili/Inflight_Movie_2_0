from .utils import create_spark_session

from .bronze import trigger_bronze
from .silver import trigger_silver
from .gold import trigger_gold

from .analysis import (
    analyze_top_genres,
    find_hidden_gems,
    select_inflight_catalog
)

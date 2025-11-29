import polars as pl

GIANT_ALL = (
    (pl.col("external_popularity_norm_cat").is_in(["Giant"])) &
    (pl.col("collaboration_intensity_norm_cat").is_in(["Giant"])) &
    (pl.col("workload_norm_cat").is_in(["Giant"])) &
    (pl.col("community_engagement_norm_cat").is_in(["Giant"]))
)

GIANT_POP_LOW_COLLAB = (
    (pl.col("external_popularity_norm_cat").is_in(["Giant"])) &
    (pl.col("workload_norm_cat").is_in(["Giant"])) &
    (pl.col("collaboration_intensity_norm_cat").is_in(["Low", "Zero"])) &
    (pl.col("community_engagement_norm_cat").is_in(["Low", "Zero"]))
)

GIANT_COLLAB_LOW_POP = (
    (pl.col("collaboration_intensity_norm_cat").is_in(["Giant"])) &
    (pl.col("community_engagement_norm_cat").is_in(["Giant"])) &
    (pl.col("external_popularity_norm_cat").is_in(["Low", "Zero"])) &
    (pl.col("workload_norm_cat").is_in(["Giant"]))
)

MID_STANDARD = (
    (pl.col("external_popularity_norm_cat").is_in(["Medium"])) &
    (pl.col("collaboration_intensity_norm_cat").is_in(["Medium"])) &
    (pl.col("workload_norm_cat").is_in(["Medium"])) &
    (pl.col("community_engagement_norm_cat").is_in(["Medium"]))
)

HIGH_PERFORMANCE_BALANCED = (
    (pl.col("external_popularity_norm_cat").is_in(["High", "Medium"])) &
    (pl.col("collaboration_intensity_norm_cat").is_in(["High", "Medium"])) &
    (pl.col("workload_norm_cat").is_in(["High", "Medium"])) &
    (pl.col("community_engagement_norm_cat").is_in(["High", "Medium"]))
)

ALL_ARCHETYPES = {
    
    "Giant_All": GIANT_ALL,
    "Giant_Pop_LowCollab": GIANT_POP_LOW_COLLAB,
    "Giant_Collab_LowPop": GIANT_COLLAB_LOW_POP,
    
    
    "High_Perf_Balanced": HIGH_PERFORMANCE_BALANCED, 
    "Mid_Standard": MID_STANDARD,                    
}
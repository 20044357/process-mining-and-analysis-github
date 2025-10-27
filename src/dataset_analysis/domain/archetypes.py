import polars as pl

# =====================
# ARCHETIPI
# =====================

GIGANTI_COLLABORATIVI_POPOLARI = (
    (pl.col("popolarita_esterna_norm_cat").is_in(["Giant"])) &
    (pl.col("intensita_collaborativa_norm_cat").is_in(["Giant"])) &
    (pl.col("volume_lavoro_norm_cat").is_in(["Giant"])) &
    (pl.col("engagement_community_norm_cat").is_in(["Giant"]))
)

GIGANTI_COLLABORATIVI_NON_POPOLARI = (
    (pl.col("popolarita_esterna_norm_cat").is_in(["Zero"])) &
    (pl.col("intensita_collaborativa_norm_cat").is_in(["Giant"])) &
    (pl.col("volume_lavoro_norm_cat").is_in(["Giant"])) &
    (pl.col("engagement_community_norm_cat").is_in(["Zero"]))
)

GIGANTI_POPOLARI_NON_COLLABORATIVI = (
    (pl.col("popolarita_esterna_norm_cat").is_in(["Giant"])) &
    (pl.col("intensita_collaborativa_norm_cat").is_in(["Zero", "Low"])) &
    (pl.col("volume_lavoro_norm_cat").is_in(["Zero", "Low"])) &
    (pl.col("engagement_community_norm_cat").is_in(["Zero", "Low"]))
)
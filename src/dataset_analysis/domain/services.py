from pathlib import Path
import random
import polars as pl
from typing import Dict, List, Optional, Tuple

from ..config import AnalysisConfig

"""
def stratify(metrics_path: str) -> Tuple[pl.DataFrame, Dict[str, float]]:

    Applica etichette di strato (1..8) a un DataFrame di metriche 'Gold Standard'
    in modalità eager (tutto in memoria). Sicuro per file singoli <1GB.
    
    Ritorna:
        - stratified_df: DataFrame con colonna 'strato_id'
        - soglie: dict con le mediane reali


    print("\n[Stratify] 🚀 Avvio stratificazione metriche GOLD (modalità eager)...")

    # 1️⃣ Legge il file (Parquet o CSV)
    df = pl.read_parquet(metrics_path) if metrics_path.endswith(".parquet") else pl.read_csv(metrics_path)
    print(f"[Stratify] 📦 Caricate {df.height:,} righe da {metrics_path}")

    # 2️⃣ Calcola mediane reali
    medians = df.select([
        pl.col("volume_lavoro").median().alias("med_volume_lavoro"),
        pl.col("intensita_collaborativa").median().alias("med_intensita_collaborativa"),
        pl.col("engagement_community").median().alias("med_engagement_community"),
    ]).row(0, named=True)

    soglie = {
        "med_volume_lavoro": float(medians["med_volume_lavoro"]),
        "med_intensita_collaborativa": float(medians["med_intensita_collaborativa"]),
        "med_engagement_community": float(medians["med_engagement_community"]),
    }

    print(f"[Stratify] ✅ Medianes calcolate: {soglie}")

    # 3️⃣ Applica formula dello strato (1 + 4v + 2c + 1e)
    stratified_df = (
        df
        .with_columns([
            (pl.col("volume_lavoro") > soglie["med_volume_lavoro"]).cast(pl.Int8).alias("volume_hi"),
            (pl.col("intensita_collaborativa") > soglie["med_intensita_collaborativa"]).cast(pl.Int8).alias("collab_hi"),
            (pl.col("engagement_community") > soglie["med_engagement_community"]).cast(pl.Int8).alias("engage_hi"),
        ])
        .with_columns(
            (
                pl.lit(1, dtype=pl.Int8)
                + pl.col("volume_hi") * 4
                + pl.col("collab_hi") * 2
                + pl.col("engage_hi") * 1
            ).alias("strato_id")
        )
        .drop(["volume_hi", "collab_hi", "engage_hi"])
    )

    print(f"[Stratify] 🎯 Stratificazione completata su {stratified_df.height:,} righe.")
    return stratified_df, soglie

def sample(stratified_df: pl.DataFrame, n_per_strato: int, seed: int) -> pl.DataFrame:
    
    Esegue un campionamento stratificato scegliendo le repo più attive
    per ogni strato, garantendo la riproducibilità.

    Se uno strato ha meno di 'n_per_strato' elementi, li prende tutti.
    Le repository vengono ordinate per 'volume_lavoro' (discendente),
    poi 'intensita_collaborativa' (discendente), 'engagement_community' (discendente),
    e infine 'repo_id' (ascendente) per garantire un ordine deterministico
    anche in caso di valori identici nelle metriche.

    Parametri
    ---------
    stratified_df : pl.DataFrame
        DataFrame etichettato con una colonna 'strato_id'.
    n_per_strato : int
        Numero desiderato di campioni da estrarre per ogni strato.
    seed : int # Il seed non sarà più usato per la casualità, ma lo manteniamo
               # nella firma della funzione per consistenza, nel caso volessi
               # reintrodurre un elemento casuale controllato in futuro.

    Ritorna
    -------
    pl.DataFrame : Un nuovo DataFrame contenente il campione bilanciato.
    
    print(f"[Stratify] Campionamento di {n_per_strato} repo per strato (criterio: maggiore attivita').")

    if stratified_df.is_empty() or "strato_id" not in stratified_df.columns:
        print("[Stratify] DataFrame stratificato vuoto o senza colonna 'strato_id'. Restituisco DataFrame vuoto.")
        return pl.DataFrame()

    # Applica l'ordinamento e prendi le prime N per ogni gruppo (strato)
    df_sample = (
        stratified_df
        .group_by("strato_id", maintain_order=True) # Raggruppa per strato
        .map_groups(
            lambda g: g.sort(
                ["volume_lavoro", "intensita_collaborativa", "engagement_community", "repo_id"], # Criteri di ordinamento
                descending=[True, True, True, False] # Ordine: decrescente per metriche, crescente per ID (tie-breaker)
            ).head(n_per_strato) # Prende le prime 'n_per_strato' dopo l'ordinamento
            # head(N) gestisce automaticamente i casi in cui g.height < N, prendendo tutti gli elementi.
        )
    )
    
    print(f"[Stratify] Campionamento completato. Estratte {len(df_sample)} repository.")
    return df_sample

def count_by_stratum(stratified_df: pl.DataFrame) -> pl.DataFrame:
    
    Calcola il conteggio e la percentuale di repository per ogni strato (da 1 a 8).
    
    if stratified_df.is_empty() or "strato_id" not in stratified_df.columns:
        return pl.DataFrame({"strato_id": list(range(1, 9)), "num_repo": [0] * 8, "perc_tot": [0.0] * 8})

    counts = stratified_df.group_by("strato_id").agg(pl.count().alias("num_repo"))
    
    all_strata = pl.DataFrame({"strato_id": list(range(1, 9))})
    
    counts = all_strata.join(counts, on="strato_id", how="left").with_columns(
        pl.col("num_repo").fill_null(0)
    )

    total_repos = stratified_df.height
    counts = counts.with_columns(
        (pl.col("num_repo") / total_repos * 100.0).round(2).alias("perc_tot") if total_repos > 0 else pl.lit(0.0)
    ).sort("strato_id")
    
    return counts"""

def count_by_stratum(stratified_df: pl.DataFrame) -> pl.DataFrame:
    """
    Conta il numero di repository per ogni strato 'Incubator' (stringhe multivariate).
    """
    if stratified_df.is_empty() or "strato_id" not in stratified_df.columns:
        return pl.DataFrame({"strato_id": [], "num_repo": [], "perc_tot": []})

    counts = stratified_df.group_by("strato_id").agg(pl.count().alias("num_repo"))
    total_repos = stratified_df.height

    return (
        counts.with_columns(
            (pl.col("num_repo") / total_repos * 100.0)
            .round(2)
            .alias("perc_tot")
        )
        .sort("num_repo", descending=True)
    )


def stratify(metrics_path: str, config: AnalysisConfig) -> Tuple[pl.DataFrame, Dict[str, Dict[str, float]]]:
    """
    Applica la stratificazione 'Incubator' a un file di metriche (Parquet o CSV).

    🔹 Carica il file da disco
    🔹 Calcola i quantili per le 8 metriche
    🔹 Crea categorie Zero/Basso/Medio/Alto/Gigante
    🔹 Aggiunge colonna 'strato_id'

    Parametri
    ----------
    metrics_path : str
        Percorso al file Parquet/CSV contenente le metriche.
    config : AnalysisConfig
        Oggetto di configurazione del progetto.

    Ritorna
    -------
    Tuple[
        pl.DataFrame,  # DataFrame con le colonne *_cat e 'strato_id'
        Dict[str, Dict[str, float]]  # Dizionario delle soglie calcolate
    ]
    """
    print("[Core] 🚀 Avvio stratificazione multivariata...")

    # 1️⃣ Caricamento file
    if metrics_path.endswith(".parquet"):
        metrics_df = pl.read_parquet(metrics_path)
    else:
        metrics_df = pl.read_csv(metrics_path)
    print(f"[Core] 📦 Caricate {metrics_df.height:,} righe da {metrics_path}")

    # 2️⃣ Metriche da stratificare (8 dimensioni)
    METRICS_TO_STRATIFY = [
        "volume_lavoro_cum", "intensita_collaborativa_cum", "engagement_community_cum", "popolarita_esterna_cum",
        "volume_lavoro_norm", "intensita_collaborativa_norm", "engagement_community_norm", "popolarita_esterna_norm",
    ]

    # 3️⃣ Calcolo dei quantili
    boundaries = {}
    for col in METRICS_TO_STRATIFY:
        active = metrics_df.filter(pl.col(col) > 0)
        if active.is_empty():
            boundaries[col] = {f"Q{int(q*100)}": 0.0 for q in config.QUANTILES}
        else:
            quantiles_result = active.select([
                pl.col(col).quantile(q).alias(f"Q{int(q*100)}") for q in config.QUANTILES
            ]).row(0, named=True)
            boundaries[col] = {k: float(v) for k, v in quantiles_result.items()}

    print("[Core] ✅ Soglie di quantile calcolate.")

    # 4️⃣ Funzione di categorizzazione
    def categorize(col: str):
        q = boundaries[col]
        q_keys = sorted(q.keys())
        return (
            pl.when(pl.col(col) == 0).then(pl.lit("Zero"))
            .when(pl.col(col) <= q[q_keys[0]]).then(pl.lit(config.QUANTILE_LABELS[0]))
            .when(pl.col(col) <= q[q_keys[1]]).then(pl.lit(config.QUANTILE_LABELS[1]))
            .when(pl.col(col) <= q[q_keys[2]]).then(pl.lit(config.QUANTILE_LABELS[2]))
            .otherwise(pl.lit(config.QUANTILE_LABELS[3]))
            .alias(f"{col}_cat")
        )

    # 5️⃣ Aggiunge categorie e crea strato composito
    df_strat = metrics_df.with_columns([categorize(col) for col in METRICS_TO_STRATIFY])
    df_strat = df_strat.with_columns(
        pl.concat_str(
            [pl.col(f"{c}_cat") for c in METRICS_TO_STRATIFY],
            separator="|"
        )
        .cast(pl.Utf8)  # forza tipo stringa
        .alias("strato_id")
    )

    print("[Core] 🎯 Stratificazione completata.")
    return df_strat, boundaries


def sample(stratified_df: pl.DataFrame, config: AnalysisConfig) -> pl.DataFrame:
    print(f"[Core] 🧪 Avvio campionamento ibrido con minimo di {config.MIN_SAMPLES_PER_STRATUM}...")

    if stratified_df.is_empty() or "strato_id" not in stratified_df.columns:
        print("[Core] ⚠️ Nessun dato valido per il campionamento.")
        return pl.DataFrame()

    # 🩹 Forza strato_id a stringa (Utf8)
    stratified_df = stratified_df.with_columns(pl.col("strato_id").cast(pl.Utf8))

    sampled_df = (
        stratified_df.group_by("strato_id")
        .map_groups(
            lambda g: g.sample(
                n=max(config.MIN_SAMPLES_PER_STRATUM, int(len(g) * config.PROPORTIONAL_SAMPLE_RATE)),
                with_replacement=False,
                seed=config.RANDOM_SEED,
            )
            if len(g) > config.MIN_SAMPLES_PER_STRATUM
            else g
        )
    )

    print(f"[Core] ✅ Campionamento completato. Estratte {len(sampled_df)} repository.")
    return sampled_df


import polars as pl
from typing import Dict
from ..config import AnalysisConfig


# ==============================
#  ARCHEDTYPES CLASSIFICATION
# ==============================

def get_stratum_archetype_name_corrected(strato_id: str) -> str:
    """Classifica uno strato in archetipi semantici, seguendo una gerarchia logica coerente."""
    if not strato_id or not isinstance(strato_id, str):
        return "Sconosciuto"

    cats = strato_id.split('|')
    if len(cats) != 8:
        return "ID Strato Malformato"

    vol_cum, collab_cum, engag_cum, pop_cum, vol_norm, collab_norm, engag_norm, pop_norm = cats

    # --- REGOLA 1: ÉLITE DEL MOMENTUM (VELOCITÀ ATTUALE) ---
    if collab_norm in ["Gigante", "Alto"]:
        if vol_norm in ["Gigante", "Alto"]:
            return "Stella Nascente Collaborativa"
        else:
            return "Hub di Discussione"

    if vol_norm in ["Gigante", "Alto"]:
        if collab_cum == "Zero" and engag_cum == "Zero":
            return "Produttore Solitario Prolifico"
        else:
            return "Fucina di Codice"

    if engag_norm in ["Gigante", "Alto"]:
        return "Progetto Popolare (Community Attiva, Codice Moderato)"

    # --- REGOLA 2: GIGANTI LEGACY ---
    if vol_cum == "Gigante" and collab_cum in ["Gigante", "Alto"]:
        return "Progetto Consolidato"
    if vol_cum == "Gigante":
        return "Grande Base di Codice (Bassa Attività)"

    # --- REGOLA 3: CORE ATTIVI ---
    if collab_cum not in ["Zero"] or engag_cum not in ["Zero"]:
        return "Progetto Collaborativo a Crescita Lenta"

    if pop_cum not in ["Zero"]:
        return "Progetto Vetrina (Solo Popolarità)"

    # --- REGOLA 4: SOLITARI ---
    if vol_cum not in ["Zero", "Basso"]:
        return "Sviluppo Attivo (Solitario)"
    if vol_cum in ["Basso"]:
        return "Progetto Abbozzato (Quasi Inattivo)"

    # --- REGOLA 5: INATTIVI ---
    return "Progetto Fantasma / Inattivo"



# ==============================
#  SUMMARIZE ARCHETYPES SERVICE
# ==============================

def summarize_archetypes(
    strata_distribution_path: str,
    config: AnalysisConfig,
    output_path: str | None = None
) -> pl.DataFrame:
    """
    Aggrega gli oltre 4.000 strati in macro-archetipi semantici.
    Ritorna un DataFrame con numero e percentuale di repository per archetipo.
    """
    print("[Core] 🧩 Analisi Archetipica: caricamento distribuzione strati...")
    df = pl.read_csv(strata_distribution_path)

    if "strato_id" not in df.columns or "num_repo" not in df.columns:
        raise ValueError("❌ Il file deve contenere le colonne 'strato_id' e 'num_repo'.")

    print(f"[Core] 📦 Caricate {df.height:,} righe da {strata_distribution_path}")

    # Applica la classificazione semantica corretta
    df = df.with_columns(
        pl.col("strato_id").map_elements(get_stratum_archetype_name_corrected, return_dtype=pl.Utf8).alias("archetipo_nome")
    )

    # Aggrega per archetipo
    summary = (
        df.group_by("archetipo_nome")
        .agg(pl.col("num_repo").sum().alias("numero_di_repo"))
        .sort("numero_di_repo", descending=True)
        .with_columns(
            (pl.col("numero_di_repo") / pl.col("numero_di_repo").sum() * 100).round(3).alias("percentuale_sul_totale")
        )
    )

    # Salvataggio opzionale
    if output_path:
        summary.write_csv(output_path)
        print(f"[Writer] ✅ File archetipi salvato in: {output_path}")

    return summary


def select_representative_repos(
    stratified_df: pl.DataFrame,
    config: AnalysisConfig,
    archetype_profiles: Dict[str, pl.Expr],
    sampling_metric: str = "volume_lavoro_cum"
) -> Dict[str, List[str]]:
    """
    Seleziona repository rappresentative per ogni archetipo.
    Per ogni combinazione unica di categorie (_cat), sceglie la repo con il valore più alto
    della metrica specificata (sampling_metric).
    
    Esempio:
        sampling_metric="volume_lavoro_cum"  → prende la repo con più lavoro prodotto.
        sampling_metric="intensita_collaborativa_cum"  → prende quella con più collaborazione.
    
    Stampa anche la combinazione di strati da cui proviene ogni repo campionata.
    """
    print(f"[Core] Avvio selezione rappresentativa per {len(archetype_profiles)} archetipi...")
    print(f"[Core] Criterio di selezione: '{sampling_metric}' (repo con valore più alto per combinazione)\n")

    sampled_repos = {}

    # Colonne di categoria che definiscono lo strato
    cat_cols = [
        "volume_lavoro_cum_cat",
        "intensita_collaborativa_cum_cat",
        "engagement_community_cum_cat",
        "popolarita_esterna_cum_cat",
        "volume_lavoro_norm_cat",
        "intensita_collaborativa_norm_cat",
        "engagement_community_norm_cat",
        "popolarita_esterna_norm_cat"
    ]

    for archetype_name, profile_expression in archetype_profiles.items():
        print(f"╔══════════════════════════════════════════════════════════════════════╗")
        print(f"  🔍  Archetipo: {archetype_name}")
        print(f"╚══════════════════════════════════════════════════════════════════════╝")

        repos_in_archetype = stratified_df.filter(profile_expression)
        n_repos = len(repos_in_archetype)
        print(f"[Core] → Repository trovate: {n_repos}")

        if n_repos == 0:
            print(f"[Core] ⚠ Nessuna repo corrispondente.\n")
            sampled_repos[archetype_name] = []
            continue

        # Calcola statistiche di età per ogni combinazione di categorie
        grouped = (
            repos_in_archetype
            .group_by(cat_cols)
            .agg([
                pl.col("age_in_days").mean().alias("age_in_days_media"),
                pl.col("age_in_days").median().alias("age_in_days_mediana"),
                pl.col("age_in_days").min().alias("age_in_days_min"),
                pl.col("age_in_days").max().alias("age_in_days_max")
            ])
            .sort("age_in_days_media", descending=True)
        )

        print(f"[Core] → Combinazioni _cat uniche trovate: {len(grouped)}\n")

        # Intestazione tabella
        header = (
            f"{'N°':<3} {'age_media':<10} {'age_mediana':<12} {'age_min':<8} {'age_max':<8} "
            f"{'volume_lavoro_cum_cat':<15} {'int_collab_cum_cat':<22} "
            f"{'eng_comm_cum_cat':<22} {'pop_ext_cum_cat':<18} "
            f"{'vol_norm_cat':<15} {'int_collab_norm_cat':<22} "
            f"{'eng_comm_norm_cat':<22} {'pop_ext_norm_cat':<18}"
        )
        print(header)
        print("-" * len(header))

        for i, row in enumerate(grouped.iter_rows(named=True), start=1):
            print(
                f"{i:<3} "
                f"{row['age_in_days_media']:<10.1f} {row['age_in_days_mediana']:<12.1f} "
                f"{row['age_in_days_min']:<8.0f} {row['age_in_days_max']:<8.0f} "
                f"{row['volume_lavoro_cum_cat']:<15} {row['intensita_collaborativa_cum_cat']:<22} "
                f"{row['engagement_community_cum_cat']:<22} {row['popolarita_esterna_cum_cat']:<18} "
                f"{row['volume_lavoro_norm_cat']:<15} {row['intensita_collaborativa_norm_cat']:<22} "
                f"{row['engagement_community_norm_cat']:<22} {row['popolarita_esterna_norm_cat']:<18}"
            )

        # 🔹 Seleziona UNA repo per ciascuna combinazione unica (_cat)
        # in base alla metrica di campionamento
        if sampling_metric not in repos_in_archetype.columns:
            print(f"[Core] ⚠ Colonna '{sampling_metric}' non trovata nel DataFrame.")
            sampled_repos[archetype_name] = []
            continue

        top_repos = (
            repos_in_archetype
            .sort(sampling_metric, descending=True)
            .unique(subset=cat_cols, keep="first")
            .with_columns([
                pl.concat_str([pl.col(c) for c in cat_cols], separator="|").alias("strato_id")
            ])
        )

        top_repo_ids = top_repos.get_column("repo_id").to_list()
        top_repo_rows = top_repos.select(["repo_id", "strato_id", sampling_metric]).iter_rows(named=True)
        sampled_repos[archetype_name] = top_repo_ids

        # Stampa riepilogo campionamento
        print(f"\n[Core] → Repository campionate per '{archetype_name}' (criterio: {sampling_metric}):")
        for row in top_repo_rows:
            print(f"     {row['repo_id']}  →  {row['strato_id']}  ({sampling_metric}={row[sampling_metric]})")

        print("\n────────────────────────────────────────────────────────────────────────────\n")

    print("[Core] ✅ Selezione completata.")
    return sampled_repos


"""
def select_representative_repos(
    stratified_path: str,
    config: AnalysisConfig,
    n_per_archetipo: int = 3,
    seed: int = 42
) -> Dict[str, List[str]]:
    
    Estrae un sottoinsieme rappresentativo di repository per ciascun archetipo semantico.

    Parametri
    ----------
    stratified_path : str
        Percorso al file 'repo_metrics_stratified.parquet'
    config : AnalysisConfig
        Oggetto di configurazione del progetto
    n_per_archetipo : int, default=3
        Numero massimo di repository da estrarre per ciascun archetipo
    seed : int, default=42
        Seed casuale per garantire riproducibilità

    Ritorna
    -------
    Dict[str, List[str]]
        Dizionario con la forma:
        {
            "Progetto Fantasma / Inattivo": ["repo1", "repo2", ...],
            "Stella Nascente Collaborativa": ["repo9", "repo12", ...],
            ...
        }
    
    print("[Core] 🧩 Estrazione di repository rappresentative per ciascun archetipo...")
    random.seed(seed)

    # 1️⃣ Caricamento del DataFrame stratificato
    path = Path(stratified_path)
    if not path.exists():
        raise FileNotFoundError(f"File non trovato: {path}")
    
    df = pl.read_parquet(path)
    print(f"[Core] 📦 Caricate {df.height:,} repository da {path.name}")

    # 2️⃣ Assegna a ogni repo il nome dell'archetipo
    df = df.with_columns(
        pl.col("strato_id")
        .map_elements(get_stratum_archetype_name_corrected, return_dtype=pl.Utf8)
        .alias("archetipo_nome")
    )

    # 3️⃣ Raggruppa per archetipo e raccogli gli ID
    archetype_groups = (
        df.group_by("archetipo_nome")
        .agg(pl.col("repo_id").alias("repo_ids"))
        .to_dict(as_series=False)
    )

    print(f"[Core] 📊 Trovati {len(archetype_groups['archetipo_nome'])} archetipi distinti.")

    # 4️⃣ Seleziona casualmente N repo per ciascun archetipo
    sampled_repos: Dict[str, List[str]] = {}
    for archetype, repos in zip(archetype_groups["archetipo_nome"], archetype_groups["repo_ids"]):
        if not repos:
            continue

        n_select = min(n_per_archetipo, len(repos))
        selected = random.sample(repos, n_select)
        sampled_repos[archetype] = selected
        print(f"   → {archetype}: {n_select} repo selezionate.")

    print(f"[Core] ✅ Estratte {len(sampled_repos)} categorie con fino a {n_per_archetipo} repo ciascuna.")
    return sampled_repos"""

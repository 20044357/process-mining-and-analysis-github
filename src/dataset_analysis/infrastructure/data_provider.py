import os
from typing import Optional, Set, List, Tuple
import polars as pl
from datetime import datetime, timedelta

from ..domain.interfaces import IDataProvider
from ..config import AnalysisConfig
from ..utils import scan_parquet_dataset_only
from ..domain.entities import RepositoryMetrics

class ParquetDataProvider(IDataProvider):
    """
    Implementazione di IDataProvider che legge i dati da file Parquet,
    utilizzando Polars per l'elaborazione efficiente e calcolando
    metriche di stratificazione "Gold Standard".
    """
    def __init__(self, config: AnalysisConfig):
        """
        Inizializza il provider con l'oggetto di configurazione completo.
        """
        self.config = config
        print(f"[DataProvider] Inizializzato. Scansionerà i dati da: {self.config.dataset_base_dir}")

    def _get_base_lazy_frame(self) -> pl.LazyFrame:
        """
        Crea un LazyFrame di base già filtrato per la finestra temporale definita
        nella configurazione. I file vengono selezionati a monte per anno/mese/giorno,
        evitando che Polars debba scansionare l’intero dataset.
        """
        lf = scan_parquet_dataset_only(
            base_dir=self.config.dataset_base_dir,
            start_date=self.config.analysis_start_date,
            end_date=self.config.analysis_end_date
        )

        print(f"[Config] Range temporale analizzato: {self.config.analysis_start_date} -> {self.config.analysis_end_date}")

        # Colonna repo_id in stringa UTF8 per coerenza
        lf = lf.with_columns(pl.col("repo_id").cast(pl.Utf8))

        return lf

    def get_eligible_repo_ids(self) -> Set[str]:
        """
        FASE 1A: Estrae e materializza in memoria gli ID delle repo nate nel periodo.
        Questa è l'unica operazione che richiede un picco di RAM per la lista di ID.
        """
        print("[DataProvider] Estrazione e materializzazione degli ID eleggibili...")
        try:
            dset = self._get_base_lazy_frame()
            filt = (pl.col("activity") == "CreateEvent") & (pl.col("create_ref_type") == "repository")
            table = dset.filter(filt).select("repo_id").unique().collect(engine="streaming")
            repo_ids = set(table["repo_id"].to_list())
            print(f"[DataProvider] Trovate e materializzate {len(repo_ids)} repository eleggibili.")
            return repo_ids
        except Exception as e:
            print(f"[ERROR] Errore durante l'estrazione degli ID: {e}")
            return set()

    def calculate_summary_metrics_from_file(self, repo_ids_path: str) -> List[RepositoryMetrics]:
        """
        FASE 1B (versione basica e purista):
        Calcola metriche di attività (num_push, num_watch, num_attori)
        leggendo gli ID da file in blocchi per limitare l’uso di RAM,
        ed esporta il risultato come lista di entità di dominio `RepositoryMetrics`.

        🔹 Identica nella logica a `calculate_summary_metrics`, ma:
        - legge gli ID da file in chunk
        - esclude i bot a monte
        - filtra le repo con >=2 push e >=2 attori
        - converte il risultato in entità di dominio
        """
        print(f"[DataProvider] Avvio calcolo metriche attività (basico, chunked) dal file: {repo_ids_path}...")
        all_chunks: List[pl.DataFrame] = []

        try:
            lf_base = self._get_base_lazy_frame()

            with open(repo_ids_path, "r", encoding="utf-8") as f:
                chunk_num = 1

                while True:
                    # Legge un blocco di ID repository
                    chunk_ids = [line.strip() for _, line in zip(range(self.config.chunk_size), f) if line.strip()]
                    if not chunk_ids:
                        break

                    print(f"  -> Processando blocco {chunk_num} ({len(chunk_ids)} repo)...")

                    repo_ids_lf_chunk = pl.LazyFrame({"repo_id": chunk_ids})

                    # Filtra dataset base per repo del blocco ed esclude bot
                    lf_chunk = (
                        lf_base
                        .join(repo_ids_lf_chunk, on="repo_id", how="semi")
                        .filter(pl.col("activity").is_in(["PushEvent", "WatchEvent"]))
                        .filter(~pl.col("actor_login").str.ends_with("[bot]"))
                    )

                    # Aggrega metriche per repo_id
                    metrics_lf_chunk = (
                        lf_chunk.group_by("repo_id").agg([
                            # Numero di Push
                            (
                                pl.when(pl.col("activity") == "PushEvent")
                                .then(1).otherwise(0)
                            ).sum().alias("num_push"),

                            # Numero di Watch (solo azione “started”)
                            (
                                pl.when(
                                    (pl.col("activity") == "WatchEvent") &
                                    (pl.col("action") == "started")
                                )
                                .then(1).otherwise(0)
                            ).sum().alias("num_watch"),

                            # Numero di attori umani distinti
                            pl.n_unique("actor_id").alias("num_attori"),
                        ])
                        # Filtro finale: almeno 2 push e 2 attori
                        .filter((pl.col("num_push") > 1) & (pl.col("num_attori") > 1))
                        .select(["repo_id", "num_push", "num_watch", "num_attori"])
                    )

                    df_chunk = metrics_lf_chunk.collect(engine="streaming")
                    if not df_chunk.is_empty():
                        all_chunks.append(df_chunk)

                    chunk_num += 1

            # 🔚 Post-elaborazione finale
            if not all_chunks:
                print("[DataProvider] Nessuna metrica calcolata (tutti i blocchi vuoti).")
                return []

            final_df = pl.concat(all_chunks, how="vertical")
            print(f"[DataProvider] Calcolo completato. {len(final_df)} repository valide trovate.")

            # --- Conversione in entità di dominio ---
            print("[DataProvider] Conversione del DataFrame in entità RepositoryMetrics...")
            list_of_dicts = final_df.to_dicts()

            try:
                domain_entities = [RepositoryMetrics(**d) for d in list_of_dicts]
                print(f"[DataProvider] Conversione completata: {len(domain_entities)} entità generate.")
                return domain_entities
            except TypeError as e:
                print(f"[ERROR] Errore di mappatura a RepositoryMetrics: {e}")
                print("Assicurati che i campi della dataclass corrispondano ai nomi delle colonne.")
                return []

        except Exception as e:
            print(f"[ERROR] Errore imprevisto durante il calcolo metriche da file: {e}")
            return []

    def calculate_summary_metrics_gold(self, repo_ids: set[str]) -> pl.LazyFrame:
        """
        Calcola e restituisce un LazyFrame con le metriche di stratificazione "Gold Standard".

        🔹 Ottimizzazioni:
        - Esclusione dei bot PRIMA dell'aggregazione (come in calculate_summary_metrics)
        - Aggiunta del filtro per escludere repo con <= 1 attore umano

        Metriche calcolate:
        - volume_lavoro: Somma dei commit nei push umani.
        - intensita_collaborativa: Somma pesata di PR, review e commenti umani.
        - engagement_community: Somma pesata di watch, issue e fork di tutti gli attori.
        """
        print(f"[DataProvider] Calcolo metriche 'Gold Standard' (lazy, bot esclusi) per {len(repo_ids)} repository...")

        try:
            if not repo_ids:
                print("[DataProvider] Set di repo_ids vuoto, nessun calcolo eseguito.")
                return pl.LazyFrame({
                    "repo_id": [], "volume_lavoro": [],
                    "intensita_collaborativa": [], "engagement_community": []
                })

            # 1️⃣ Dataset base filtrato per la finestra temporale
            lf_base = self._get_base_lazy_frame()

            # 2️⃣ Filtra solo le repository eleggibili
            repo_ids_lf = pl.LazyFrame({"repo_id": list(repo_ids)})
            lf_filtered = lf_base.join(repo_ids_lf, on="repo_id", how="semi")

            # 3️⃣ Escludi bot PRIMA dell’aggregazione (filtro diretto)
            lf_filtered = lf_filtered.filter(~pl.col("actor_login").str.ends_with("[bot]"))

            # 4️⃣ Aggregazione unica (metriche Gold + conteggio attori umani)
            metrics_lf = (
                lf_filtered.group_by("repo_id")
                .agg([
                    # Volume di lavoro (push umani)
                    (
                        pl.when(pl.col("activity") == "PushEvent")
                        .then(pl.col("push_size").fill_null(1))
                        .otherwise(0)
                    ).sum().alias("volume_lavoro"),

                    # Intensità collaborativa
                    (
                        pl.when((pl.col("activity") == "PullRequestEvent") &
                                (pl.col("action") == "opened")).then(3)
                        .when((pl.col("activity") == "PullRequestReviewEvent") &
                            (pl.col("action") == "created")).then(2)
                        .when((pl.col("activity") == "IssueCommentEvent") &
                            (pl.col("action") == "created")).then(1)
                        .otherwise(0)
                    ).sum().alias("intensita_collaborativa"),

                    # Engagement della community
                    (
                        pl.when((pl.col("activity") == "WatchEvent") &
                                (pl.col("action") == "started")).then(1)
                        .when((pl.col("activity") == "IssuesEvent") &
                            (pl.col("action") == "opened")).then(5)
                        .when(pl.col("activity") == "ForkEvent").then(2)
                        .otherwise(0)
                    ).sum().alias("engagement_community"),

                    # Numero di attori unici (umani)
                    pl.n_unique("actor_id").alias("num_attori"),
                ])
                # 5️⃣ Filtro finale: almeno 2 push-equivalenti e almeno 2 attori umani
                .filter((pl.col("volume_lavoro") > 1) & (pl.col("num_attori") > 1))
                .select(["repo_id", "volume_lavoro", "intensita_collaborativa", "engagement_community"])
            )

            print("[DataProvider] LazyFrame metriche 'Gold Standard' pronto (bot esclusi e con filtro attori).")
            return metrics_lf.sort(["engagement_community", "repo_id"], descending=[True, False])

        except Exception as e:
            print(f"[ERROR] Errore durante il calcolo metriche 'Gold Standard': {e}")
            return pl.LazyFrame({
                "repo_id": [], "volume_lavoro": [],
                "intensita_collaborativa": [], "engagement_community": []
            })
    
    def calculate_summary_metrics_golden_recipe(self, repo_ids: set[str]) -> pl.LazyFrame:
        """
        Calcola e restituisce un LazyFrame con le metriche di attività "base",
        usando la stessa nomenclatura delle metriche 'Gold Standard'.

        🔹 Colonne restituite:
            - repo_id
            - volume_lavoro              (ex num_push)
            - intensita_collaborativa    (ex num_attori, esclusi i watcher passivi)
            - engagement_community       (ex num_watch)

        🔹 Ottimizzazioni:
            - Esclusione dei bot (`actor_login` che termina con "[bot]`).
            - Esclusione dei watcher passivi solo dal conteggio attori.
            - Filtro finale: almeno 2 push e almeno 2 attori attivi distinti.
            - Interamente lazy / streaming-friendly.
        """
        print(f"[DataProvider] Calcolo metriche BASE (lazy, bot esclusi, watcher passivi esclusi dal conteggio attori) per {len(repo_ids)} repository...")

        try:
            # 🔸 Caso di input vuoto
            if not repo_ids:
                print("[DataProvider] Set di repo_ids vuoto, nessun calcolo eseguito.")
                return pl.DataFrame(
                    {
                        "repo_id": [],
                        "volume_lavoro": [],
                        "intensita_collaborativa": [],
                        "engagement_community": [],
                    }
                ).lazy()

            # 1️⃣ Dataset base filtrato per la finestra temporale
            lf = self._get_base_lazy_frame()

            # 2️⃣ Tieni solo eventi rilevanti (Push + Watch)
            lf = lf.filter(pl.col("activity").is_in(["PushEvent", "WatchEvent"]))

            # 3️⃣ Escludi i bot
            lf = lf.filter(~pl.col("actor_login").str.ends_with("[bot]").fill_null(False))

            # 4️⃣ Flag per distinguere eventi attivi (≠ WatchEvent)
            lf = lf.with_columns(
                (pl.col("activity") != "WatchEvent").alias("is_active_event")
            )

            # 5️⃣ Filtra solo le repository richieste
            repo_ids_lf = pl.DataFrame({"repo_id": list(repo_ids)}).lazy()
            lf = lf.join(repo_ids_lf, on="repo_id", how="semi")

            # 6️⃣ Aggregazione per repo_id
            metrics_lf = (
                lf.group_by("repo_id")
                .agg([
                    # Volume lavoro → numero di PushEvent
                    (pl.col("activity") == "PushEvent")
                    .sum()
                    .alias("volume_lavoro"),

                    # Engagement community → numero di WatchEvent 'started'
                    ((pl.col("activity") == "WatchEvent") & (pl.col("action") == "started"))
                    .sum()
                    .alias("engagement_community"),

                    # Intensità collaborativa → attori con almeno un evento attivo
                    pl.col("actor_id")
                    .filter(pl.col("is_active_event"))
                    .n_unique()
                    .alias("intensita_collaborativa"),
                ])
                # 7️⃣ Filtro finale: almeno 2 push e 2 attori attivi
                .filter(
                    (pl.col("volume_lavoro") > 1)
                    & (pl.col("intensita_collaborativa") > 1)
                )
                # 8️⃣ Colonne finali e ordinamento
                .select(
                    [
                        "repo_id",
                        "volume_lavoro",
                        "intensita_collaborativa",
                        "engagement_community",
                    ]
                )
                .sort(["engagement_community", "repo_id"], descending=[True, False])
            )

            print("[DataProvider] LazyFrame metriche BASE pronto (bot esclusi, watcher passivi esclusi dal conteggio attori).")
            return metrics_lf

        except Exception as e:
            print(f"[ERROR] Errore durante il calcolo metriche BASE: {e}")
            return pl.DataFrame(
                {
                    "repo_id": [],
                    "volume_lavoro": [],
                    "intensita_collaborativa": [],
                    "engagement_community": [],
                }
            ).lazy()

    def calculate_summary_metrics_with_age(self, repo_ids: set[str]) -> pl.LazyFrame:
        """
        Calcola e restituisce un LazyFrame con le metriche di attività "base"
        NORMALIZZATE per l'età della repository, eliminando il bias temporale.

        🔹 Colonne restituite:
            - repo_id
            - volume_lavoro              (push/giorno)
            - intensita_collaborativa    (attori/giorno)
            - engagement_community       (watch/giorno)

        🔹 Ottimizzazioni:
            - Esclusione dei bot (`actor_login` che termina con "[bot]`).
            - Esclusione dei watcher passivi solo dal conteggio attori.
            - Normalizzazione per età in giorni (tassi giornalieri).
            - Filtro finale: almeno 2 push e almeno 2 attori attivi distinti.
            - Interamente lazy / streaming-friendly.
        """
        print(f"[DataProvider] Calcolo metriche NORMALIZZATE PER ETÀ (lazy) per {len(repo_ids)} repository...")

        try:
            # 🔸 Caso di input vuoto
            if not repo_ids:
                print("[DataProvider] Set di repo_ids vuoto, nessun calcolo eseguito.")
                return pl.DataFrame(
                    {
                        "repo_id": [],
                        "volume_lavoro": [],
                        "intensita_collaborativa": [],
                        "engagement_community": [],
                    }
                ).lazy()

            # 1️⃣ Dataset base filtrato per la finestra temporale
            lf = self._get_base_lazy_frame()

            # 2️⃣ Tieni solo eventi rilevanti (Push + Watch)
            lf = lf.filter(pl.col("activity").is_in(["PushEvent", "WatchEvent", "CreateEvent"]))

            # 3️⃣ Escludi i bot
            lf = lf.filter(~pl.col("actor_login").str.ends_with("[bot]").fill_null(False))

            # 4️⃣ Flag per distinguere eventi attivi (≠ WatchEvent)
            lf = lf.with_columns(
                (pl.col("activity") != "WatchEvent").alias("is_active_event")
            )

            # 5️⃣ Filtra solo le repository richieste
            repo_ids_lf = pl.DataFrame({"repo_id": list(repo_ids)}).lazy()
            lf = lf.join(repo_ids_lf, on="repo_id", how="semi")

            # 6️⃣ Calcola la data di creazione di ogni repo (CreateEvent → repository)
            creation_dates_lf = (
                lf.filter(
                    (pl.col("activity") == "CreateEvent")
                    & (pl.col("create_ref_type") == "repository")
                )
                .select(["repo_id", "timestamp"])
                .rename({"timestamp": "creation_date"})
                .unique(subset=["repo_id"], keep="first")
            )

            # 7️⃣ Aggregazione per repo_id (conteggi cumulativi)
            metrics_cum_lf = (
                lf.group_by("repo_id")
                .agg([
                    (pl.col("activity") == "PushEvent").sum().alias("volume_lavoro_cum"),
                    ((pl.col("activity") == "WatchEvent") & (pl.col("action") == "started"))
                    .sum()
                    .alias("engagement_community_cum"),
                    pl.col("actor_id")
                    .filter(pl.col("is_active_event"))
                    .n_unique()
                    .alias("intensita_collaborativa_cum"),
                ])
                # Filtro di vitalità minimo
                .filter(
                    (pl.col("volume_lavoro_cum") > 1)
                    & (pl.col("intensita_collaborativa_cum") > 1)
                )
            )

            # 8️⃣ Unisci alle date di creazione per calcolare l'età
            metrics_with_age_lf = metrics_cum_lf.join(
                creation_dates_lf, on="repo_id", how="inner"
            )

            end_dt = datetime.fromisoformat(self.config.analysis_end_date.replace("Z", "")).date()

            metrics_norm_lf = (
                metrics_with_age_lf
                .with_columns(
                    (
                        (pl.lit(end_dt) - pl.col("creation_date").dt.date())
                        .dt.total_days()
                        .add(1.0)
                        .alias("age_in_days")
                    )
                )
                .filter(pl.col("age_in_days") >= 1)
                .with_columns([
                    # Tassi giornalieri normalizzati per età
                    (pl.col("volume_lavoro_cum") / pl.col("age_in_days")).alias("volume_lavoro"),
                    (pl.col("intensita_collaborativa_cum") / pl.col("age_in_days")).alias("intensita_collaborativa"),
                    (pl.col("engagement_community_cum") / pl.col("age_in_days")).alias("engagement_community"),
                ])
            )

            # 🔟 Colonne finali e ordinamento
            metrics_final_lf = (
                metrics_norm_lf
                .select([
                    "repo_id",
                    "volume_lavoro",
                    "intensita_collaborativa",
                    "engagement_community",
                ])
                .sort(["engagement_community", "repo_id"], descending=[True, False])
            )

            print("[DataProvider] LazyFrame metriche NORMALIZZATE PER ETÀ pronto (bot esclusi, tassi giornalieri calcolati).")
            return metrics_final_lf

        except Exception as e:
            print(f"[ERROR] Errore durante il calcolo metriche NORMALIZZATE PER ETÀ: {e}")
            return pl.DataFrame(
                {
                    "repo_id": [],
                    "volume_lavoro": [],
                    "intensita_collaborativa": [],
                    "engagement_community": [],
                }
            ).lazy()
        
    def calculate_summary_metrics(self, repo_ids: set[str]) -> pl.LazyFrame:
        """
        Calcola e restituisce un LazyFrame con le metriche di attività
        secondo la 'Golden Recipe' (cumulative + normalizzate per età).

        🔹 Colonne restituite:
            - repo_id
            - volume_lavoro_cum              → Somma dei push_size nei PushEvent
            - intensita_collaborativa_cum    → Pesi: PR=3, Review=2, Commento=1
            - engagement_community_cum       → Pesi: Issue=5, Fork=2
            - popolarita_esterna_cum         → Pesi: WatchEvent=1
            - age_in_days                    → Giorni trascorsi dalla creazione
            - *_norm                         → Metriche normalizzate per età (valori/giorno)

        🔹 Ottimizzazioni:
            - Esclusione bot (`actor_login` termina con "[bot]"`)
            - Esclusione repo senza evento di creazione
            - Interamente lazy / streaming-friendly
        """
        print(f"[DataProvider] Calcolo metriche GOLDEN (cumulative + normalizzate) per {len(repo_ids)} repository...")

        try:
            # 🔸 Caso di input vuoto
            if not repo_ids:
                print("[DataProvider] Set di repo_ids vuoto, nessun calcolo eseguito.")
                return pl.DataFrame(
                    {
                        "repo_id": [],
                        "volume_lavoro_cum": [],
                        "intensita_collaborativa_cum": [],
                        "engagement_community_cum": [],
                        "popolarita_esterna_cum": [],
                        "creation_date": [],
                        "age_in_days": [],
                        "volume_lavoro_norm": [],
                        "intensita_collaborativa_norm": [],
                        "engagement_community_norm": [],
                        "popolarita_esterna_norm": [],
                    }
                ).lazy()

            # 1️⃣ Dataset base filtrato per la finestra temporale
            lf = self._get_base_lazy_frame()

            # 2️⃣ Escludi bot
            lf = lf.filter(~pl.col("actor_login").str.ends_with("[bot]").fill_null(False))

            # 3️⃣ Filtra solo le repository richieste
            repo_ids_lf = pl.DataFrame({"repo_id": list(repo_ids)}).lazy()
            lf = lf.join(repo_ids_lf, on="repo_id", how="semi")

            # 4️⃣ Calcolo data di creazione (CreateEvent → repository)
            creation_lf = (
                lf.filter(
                    (pl.col("activity") == "CreateEvent")
                    & (pl.col("create_ref_type") == "repository")
                )
                .group_by("repo_id")
                .agg(pl.col("timestamp").min().alias("creation_date"))
            )

            # 5️⃣ Calcolo metriche cumulative GOLDEN
            metrics_lf = (
                lf.group_by("repo_id")
                .agg([
                    # --- Metrica 1: Volume di Lavoro
                    pl.col("push_size")
                    .filter(pl.col("activity") == "PushEvent")
                    .sum()
                    .alias("volume_lavoro_cum"),

                    # --- Metrica 2: Intensità Collaborativa (pesi 3-2-1)
                    (
                        pl.when((pl.col("activity") == "PullRequestEvent") & (pl.col("action") == "opened")).then(3)
                        .when((pl.col("activity") == "PullRequestReviewEvent") & (pl.col("action") == "created")).then(2)
                        .when(pl.col("activity").is_in(["IssueCommentEvent", "PullRequestReviewCommentEvent"]) & (pl.col("action") == "created")).then(1)
                        .otherwise(0)
                    ).sum().alias("intensita_collaborativa_cum"),

                    # --- Metrica 3: Engagement Community (pesi 5-2)
                    (
                        pl.when((pl.col("activity") == "IssuesEvent") & (pl.col("action") == "opened")).then(5)
                        .when(pl.col("activity") == "ForkEvent").then(2)
                        .otherwise(0)
                    ).sum().alias("engagement_community_cum"),

                    # --- Metrica 4: Popolarità Esterna
                    (
                        ((pl.col("activity") == "WatchEvent") & (pl.col("action") == "started"))
                    ).sum().alias("popolarita_esterna_cum"),
                ])
            )

            # 6️⃣ Unisci con data di creazione → scarta repo senza CreateEvent
            metrics_with_age = metrics_lf.join(creation_lf, on="repo_id", how="inner")

            # 7️⃣ Calcolo età in giorni
            end_dt = datetime.fromisoformat(self.config.analysis_end_date.replace("Z", "")).date()

            metrics_with_age = metrics_with_age.with_columns(
                ((pl.lit(end_dt) - pl.col("creation_date").dt.date()).dt.total_days() + 1.0)
                .alias("age_in_days")
            ).filter(pl.col("age_in_days") > 0)

            # 8️⃣ Calcolo metriche normalizzate per età
            metrics_norm = metrics_with_age.with_columns([
                (pl.col("volume_lavoro_cum") / pl.col("age_in_days")).alias("volume_lavoro_norm"),
                (pl.col("intensita_collaborativa_cum") / pl.col("age_in_days")).alias("intensita_collaborativa_norm"),
                (pl.col("engagement_community_cum") / pl.col("age_in_days")).alias("engagement_community_norm"),
                (pl.col("popolarita_esterna_cum") / pl.col("age_in_days")).alias("popolarita_esterna_norm"),
            ])

            # 9️⃣ Filtro finale: repo realmente attive
            metrics_final = metrics_norm.filter(
                (pl.col("volume_lavoro_cum") > 0)
                | (pl.col("intensita_collaborativa_cum") > 0)
                | (pl.col("engagement_community_cum") > 0)
                | (pl.col("popolarita_esterna_cum") > 0)
            ).sort(["volume_lavoro_cum"], descending=True)

            print("[DataProvider] LazyFrame metriche GOLDEN pronto (cumulative + normalizzate, bot esclusi).")
            return metrics_final

        except Exception as e:
            print(f"[ERROR] Errore durante il calcolo metriche GOLDEN con normalizzazione: {e}")
            return pl.DataFrame(
                {
                    "repo_id": [],
                    "volume_lavoro_cum": [],
                    "intensita_collaborativa_cum": [],
                    "engagement_community_cum": [],
                    "popolarita_esterna_cum": [],
                    "creation_date": [],
                    "age_in_days": [],
                    "volume_lavoro_norm": [],
                    "intensita_collaborativa_norm": [],
                    "engagement_community_norm": [],
                    "popolarita_esterna_norm": [],
                }
            ).lazy()

    """def get_lazy_events_for_repo(self, repo_id: str) -> pl.LazyFrame:
        
        COSTRUISCE e RESTITUISCE il piano 'lazy' per leggere gli eventi di una repo,
        ma NON lo esegue.
        
        lf = self._get_base_lazy_frame()
        return lf.filter(pl.col("repo_id") == repo_id)"""
    
    def get_lazy_events_for_repo(self, repo_id: str) -> pl.LazyFrame:
        """
        Restituisce un LazyFrame ottimizzato per una singola repository, garantendo
        che il filtro per repo_id venga applicato immediatamente dopo la scansione
        per massimizzare le possibilità di predicate pushdown.
        """
        print(f"[Provider] 🚀 Lazy loading per repo {repo_id} (predicate pushdown forzato)...")

        # 1. Genera la lista dei percorsi dei file da scansionare.
        start_dt = datetime.fromisoformat(self.config.analysis_start_date.replace("Z", "")).date()
        end_dt = datetime.fromisoformat(self.config.analysis_end_date.replace("Z", "")).date()
        current_date = start_dt
        date_paths = []
        while current_date <= end_dt:
            daily_path_pattern = os.path.join(
                self.config.dataset_base_dir,
                f"anno={current_date.year}", f"mese={current_date.month:02d}", f"giorno={current_date.day:02d}", "*.parquet"
            )
            date_paths.append(daily_path_pattern)
            current_date += timedelta(days=1)

        if not date_paths:
            print("⚠️ Nessun percorso file trovato. Restituisco un LazyFrame vuoto.")
            return pl.LazyFrame()

        # 2. LA LOGICA CORRETTA: SCAN -> FILTER -> ALTRE OPERAZIONI
        #    Applichiamo il filtro pesante subito dopo la scansione.
        lazy_frame = (
            pl.scan_parquet(date_paths)
            .filter(pl.col("repo_id") == int(repo_id))
            .filter(~pl.col("actor_login").str.ends_with("[bot]").fill_null(False))
            .with_columns(pl.col("repo_id").cast(pl.Utf8)) # Il cast ora è alla fine
        )
        
        return lazy_frame
    
    def get_stratified_data(self) -> pl.DataFrame:
        """
        Implementazione che sa che i dati stratificati si trovano in un file
        il cui percorso è specificato nella configurazione.
        """
        file_path = self.config.stratified_file_path_parquet # Usa il percorso dalla config interna
        print(f"[Provider] 📦 Caricamento dati stratificati da: {file_path}")
        
        try:
            if not os.path.exists(file_path):
                raise FileNotFoundError(...)

            stratified_df = pl.read_parquet(file_path)
            
            print(f"[Provider] ✅ Caricate {stratified_df.height:,} righe stratificate.")
            return stratified_df
            
        except Exception as e:
            print(f"❌ ERRORE CRITICO durante il caricamento del file stratificato: {e}")
            raise IOError(...)
        
    def get_events_for_repo_split_by_peak(self, repo_id: str, peak_event_type: str) -> Optional[Tuple[pl.LazyFrame, pl.LazyFrame]]:
        """
        Carica gli eventi per una repository, trova il giorno con il picco di un dato
        tipo di evento (es. WatchEvent), e divide il log in due LazyFrame:
        uno con gli eventi "prima" di quel giorno e uno con gli eventi "da quel giorno in poi".

        Ritorna None se il log è vuoto o se non ci sono eventi del tipo specificato.
        """
        print(f"[Provider] Avvio analisi temporale per repo {repo_id}, cercando picco di '{peak_event_type}'...")
        
        # 1. Carica in memoria i dati SOLO per questa repository.
        #    È necessario per eseguire aggregazioni come trovare un massimo.
        #    Dato che operiamo su una sola repo alla volta, l'uso di memoria è controllato.
        repo_df = self.get_lazy_events_for_repo(repo_id).collect()

        if repo_df.is_empty():
            print(f"[Provider] ⚠️ Nessun evento trovato per {repo_id}. Impossibile procedere.")
            return None

        # 2. Isola gli eventi che definiscono il "picco"
        peak_events_df = repo_df.filter(pl.col("activity") == peak_event_type)
        
        if peak_events_df.is_empty():
            print(f"[Provider] ⚠️ Nessun evento di tipo '{peak_event_type}' trovato per {repo_id}. Impossibile trovare un picco.")
            return None

        # 3. Trova il giorno con il maggior numero di eventi del tipo specificato
        try:
            daily_counts = (
                peak_events_df
                .with_columns(
                    # Tronca il timestamp al giorno per raggruppare
                    pl.col("timestamp").dt.truncate("1d").alias("day")
                )
                .group_by("day")
                .agg(pl.count().alias("count"))
                .sort("count", descending=True)
            )
            
            # Il primo giorno nella lista ordinata è il nostro timestamp di picco
            peak_timestamp = daily_counts.head(1).get_column("day")[0]
            print(f"[Provider] ✅ Picco di attività '{peak_event_type}' identificato nel giorno: {peak_timestamp.date()}")
        
        except Exception as e:
            print(f"[Provider] ❌ ERRORE durante il calcolo del picco per {repo_id}: {e}")
            return None

        # 4. Dividi il DataFrame originale in due LazyFrame basandoti sul timestamp del picco
        #    Riconvertiamo il DataFrame in memoria a Lazy per mantenere la coerenza dell'API
        repo_lf = repo_df.lazy()
        
        # Eventi che avvengono STRETTAMENTE PRIMA dell'inizio del giorno del picco
        log_prima = repo_lf.filter(pl.col("timestamp") < peak_timestamp)
        
        # Eventi che avvengono DALL'INIZIO del giorno del picco in poi
        log_dopo = repo_lf.filter(pl.col("timestamp") >= peak_timestamp)
        
        # Controlli di sicurezza opzionali
        # print(f"[Provider] Log 'prima': {log_prima.collect().height} eventi. Log 'dopo': {log_dopo.collect().height} eventi.")
        
        return log_prima, log_dopo
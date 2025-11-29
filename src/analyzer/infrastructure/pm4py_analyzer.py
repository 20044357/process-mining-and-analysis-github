import logging
import pandas as pd
import polars as pl
import pm4py
from pm4py.objects.log.obj import EventLog
from ..domain.interfaces import IProcessAnalyzer
from ..infrastructure.logging_config import LayerLoggerAdapter
from ..domain import predicates
from pm4py.objects.heuristics_net.obj import HeuristicsNet

class PM4PyAnalyzer(IProcessAnalyzer):

    def __init__(self):
        base_logger = logging.getLogger(self.__class__.__name__)
        self.logger = LayerLoggerAdapter(base_logger, {"layer": "Infrastructure"})
        self.logger.info("PM4PyAnalyzer inizializzato correttamente.")

    def discover_heuristic_model_frequency(self, event_log: EventLog) -> HeuristicsNet:
        self.logger.info("Avvio discovery del modello di processo (frequency) con Heuristics Miner...")
        
        heuristics_net = pm4py.discover_heuristics_net(
            event_log,
            dependency_threshold=0.95,
            and_threshold=0.8,
            loop_two_threshold=0.7,
            min_act_count=100,
            min_dfg_occurrences=100
            )
        
        self.logger.info("Discovery del modello di processo completata.")

        return heuristics_net

    
    def discover_heuristic_model_performance(self, event_log: EventLog) -> HeuristicsNet:
        self.logger.info("Avvio discovery delle performance del processo (performance) con Heuristics Miner...")   
        
        performance_heuristics_net = pm4py.discover_heuristics_net(
            event_log, 
            activity_key="concept:name",
            dependency_threshold=0.95,
            and_threshold=0.8,
            loop_two_threshold=0.7,
            min_act_count=100,
            min_dfg_occurrences=100,
            decoration="performance" 
        )
        
        self.logger.info("Discovery delle performance completata.")
        return performance_heuristics_net

    def prepare_log(self, raw_ldf: pl.LazyFrame) -> EventLog:
        ldf_no_bots = raw_ldf.filter(~predicates.is_bot_actor)
        ldf_core_events = ldf_no_bots.filter(predicates.is_core_workflow_event)
        ldf_normalized = _normalize_event_names(ldf_core_events)

        ldf_pm4py_format = ldf_normalized.select(
            #  Case ID = Attore + Repo
            (pl.col("actor_id").cast(pl.Utf8) + "_" + pl.col("repo_id").cast(pl.Utf8)).alias("case:concept:name"), 
            pl.col("concept:name"),                       
            pl.col("timestamp").alias("time:timestamp"),  
            pl.col("actor_login").alias("org:resource")   
        )

        self.logger.info("Avvio materializzazione del log per PM4Py...")
        
        pandas_df = ldf_pm4py_format.collect().to_pandas()
        pandas_df['time:timestamp'] = pd.to_datetime(pandas_df['time:timestamp'])
        event_log = pm4py.convert_to_event_log(pandas_df)
        
        self.logger.info(f"Log PM4Py creato con successo. Numero di eventi: {len(pandas_df)}")

        return event_log

def _normalize_event_names(df: pl.LazyFrame) -> pl.LazyFrame:   
    schema = df.collect_schema()
    has_pr_merged = "pr_merged" in schema
    has_review_state = "review_state" in schema
    has_create_ref_type = "create_ref_type" in schema
    has_delete_ref_type = "delete_ref_type" in schema

    return df.with_columns(
        pl.when(pl.col("activity") == "PullRequestEvent")
        .then(
            pl.when((pl.col("action") == "closed") & (pl.col("pr_merged") == True) if has_pr_merged else False)
            .then(pl.lit("PullRequestEvent_merged"))
            .when((pl.col("action") == "closed") & (pl.col("pr_merged") == False) if has_pr_merged else False)
            .then(pl.lit("PullRequestEvent_rejected"))
            .otherwise(pl.col("activity").cast(pl.Utf8) + "_" + pl.col("action").cast(pl.Utf8))
        )
        .when(pl.col("activity") == "IssuesEvent")
        .then(pl.col("activity").cast(pl.Utf8) + "_" + pl.col("action").cast(pl.Utf8))
        .when(pl.col("activity") == "PullRequestReviewEvent")
        .then(
            pl.col("activity").cast(pl.Utf8) + "_" +
            (pl.col("review_state").cast(pl.Utf8) if has_review_state else pl.col("action").cast(pl.Utf8))
        )
        
        
        .when(pl.col("activity").is_in([
            "IssueCommentEvent", 
            "PullRequestReviewCommentEvent", 
            "CommitCommentEvent"  
        ]))
        .then(pl.col("activity").cast(pl.Utf8) + "_" + pl.col("action").cast(pl.Utf8))
        

        .when(pl.col("activity") == "CreateEvent")
        .then(
            pl.col("activity").cast(pl.Utf8) + "_" +
            (pl.col("create_ref_type").cast(pl.Utf8) if has_create_ref_type else pl.lit("ref"))
        )
        .when(pl.col("activity") == "DeleteEvent")
        .then(
            pl.col("activity").cast(pl.Utf8) + "_" +
            (pl.col("delete_ref_type").cast(pl.Utf8) if has_delete_ref_type else pl.lit("ref"))
        )
        .when(pl.col("activity") == "ReleaseEvent")
        .then(pl.col("activity").cast(pl.Utf8) + "_" + pl.col("action").cast(pl.Utf8))
        .otherwise(pl.col("activity").cast(pl.Utf8))
        .alias("concept:name")
    )
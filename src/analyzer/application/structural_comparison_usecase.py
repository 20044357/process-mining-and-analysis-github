import pandas as pd
import polars as pl
import logging
from typing import Dict, Any, List, Set, Tuple, Optional, Union
from itertools import combinations
from ..domain.interfaces import IDataProvider, IModelAnalyzer, IResultWriter
from ..domain import archetypes

def execute_structural_comparison(
    provider: IDataProvider,
    model_analyzer: IModelAnalyzer,
    writer: IResultWriter,
    logger: Union[logging.Logger, logging.LoggerAdapter] 
):
    logger.info("Avvio analisi comparativa strutturale avanzata...")
    
    archetype_names = list(archetypes.ALL_ARCHETYPES.keys())
    
    logger.info("Caricamento modelli e costruzione scheletro globale...")
    
    loaded_models = {}
    all_activities_set = set()
    all_node_counts = []
    
    for name in archetype_names:
        model = provider.load_single_archetype_model(name, "performance")
        
        if model:
            loaded_models[name] = model
            
            nodes = model_analyzer.get_model_nodes_set(model)
            all_activities_set.update(nodes)
            
            occs = getattr(model, 'activities_occurrences', {})
            for act, count in occs.items():
                all_node_counts.append({
                    "Archetype": name,
                    "Activity": act,
                    "Count": count
                })
        else:
            logger.warning(f"Impossibile caricare il modello per l'archetipo: {name}")

    global_skeleton = sorted(list(all_activities_set))
    logger.info(f"Scheletro globale creato: {len(global_skeleton)} attività uniche.")
    
    processed_adj_freq = {}
    processed_adj_perf = {}
    archetype_stats_records = [] 
    
    cache_nodes = {}
    cache_edges = {}

    for name in archetype_names:
        model = loaded_models.get(name)
        if not model: continue
        
        complexity = model_analyzer.get_process_complexity_metrics(model)
        top_act, top_count = model_analyzer.get_most_frequent_activity(model)
        (slow_src, slow_tgt), slow_sec = model_analyzer.get_slowest_edge(model)
        
        stats_row = {
            "Archetype": name,
            "Num_Nodes": model_analyzer.get_nodes_count(model),
            "Num_Edges": model_analyzer.get_edges_count(model),
            "Density": complexity["density"],
            "Cyclomatic_Complexity": complexity["cyclomatic_complexity"],
            "Avg_Degree": complexity["avg_degree"],
            "Most_Freq_Activity": f"{top_act} ({top_count})",
            "Slowest_Edge": f"{slow_src}->{slow_tgt} ({round(slow_sec/3600, 2)}h)"
        }
        archetype_stats_records.append(stats_row)
        
        freq_mat = model_analyzer.get_adjacency_matrix_frequency(model, )
        perf_mat = model_analyzer.get_adjacency_matrix_performance(model)
          
        processed_adj_freq[name] = freq_mat
        processed_adj_perf[name] = perf_mat
        
        cache_nodes[name] = model_analyzer.get_model_nodes_set(model)
        cache_edges[name] = model_analyzer.get_model_edges_set(model)

        writer.write_archetype_artifact_dataframe(
            df=freq_mat.reset_index().rename(columns={'index': 'activity'}),
            archetype_name=name,
            filename=f"adjacency_matrix_frequency_{name}.csv"
        )
                
        writer.write_archetype_artifact_dataframe(
            df=perf_mat.reset_index().rename(columns={'index': 'activity'}),
            archetype_name=name,
            filename=f"adjacency_matrix_performance_{name}.csv"
        )
 
        if hasattr(writer, 'save_archetype_heatmap'):
            
            total_freq = freq_mat.sum().sum()
            if total_freq > 0:
                freq_mat_norm = freq_mat / total_freq
            else:
                freq_mat_norm = freq_mat
            
            writer.save_archetype_heatmap(
                matrix=freq_mat_norm,  
                archetype_name=name,
                filename=f"frequency_matrix_{name}.png",
                title=f"Frequency Matrix (Normalized): {name}",
                metric="frequency"
            )
                  
            writer.save_archetype_heatmap(
                matrix=perf_mat,
                archetype_name=name,
                filename=f"performance_matrix_{name}.png",
                title=f"Performance Matrix: {name}",
                metric="performance"
            )
    
    logger.info("Generazione visualizzazioni comparative (Radar, BarChart)...")
    
    if archetype_stats_records:
        stats_df = pd.DataFrame(archetype_stats_records)
        
        writer.write_dataframe_final_analysis(pl.from_pandas(stats_df), "archetypes_structural_identikit.csv")
        try:
            writer.save_identikit_image(pl.from_pandas(stats_df), "archetypes_structural_identikit.png", "Confronto Identikit")
        except: pass
        
        radar_cols = ["Archetype", "Density", "Num_Nodes", "Num_Edges", "Cyclomatic_Complexity", "Avg_Degree"]
        available_cols = [c for c in radar_cols if c in stats_df.columns]
        
        if len(available_cols) > 1 and hasattr(writer, 'save_radar_chart'):
            radar_df = stats_df[available_cols].copy()
            writer.save_radar_chart(
                radar_df, 
                filename="archetypes_radar_chart.png", 
                title="Profilo Strutturale Archetipi"
            )

    if all_node_counts and hasattr(writer, 'save_activity_grouped_bar_chart'):
        counts_df = pd.DataFrame(all_node_counts)
        writer.save_activity_grouped_bar_chart(
            counts_df,
            filename="top_activities_comparison.png",
            title="Confronto Frequenza Attività (Top 10)",
            top_n=10
        )
 
    logger.info("Calcolo matrici di similarità globali...")
    
    frobenius_freq = pd.DataFrame(index=archetype_names, columns=archetype_names, dtype=float)
    jaccard_nodes = pd.DataFrame(index=archetype_names, columns=archetype_names, dtype=float)
    jaccard_edges = pd.DataFrame(index=archetype_names, columns=archetype_names, dtype=float)
    frobenius_perf = pd.DataFrame(index=archetype_names, columns=archetype_names, dtype=float)

    for row in archetype_names:
        for col in archetype_names:
            
            nodes_row = cache_nodes.get(row, set())
            nodes_col = cache_nodes.get(col, set())
            jaccard_nodes.loc[row, col] = model_analyzer.calculate_jaccard_similarity(nodes_row, nodes_col)
            
            edges_row = cache_edges.get(row, set())
            edges_col = cache_edges.get(col, set())
            jaccard_edges.loc[row, col] = model_analyzer.calculate_jaccard_similarity(edges_row, edges_col)

            m1_freq = processed_adj_freq.get(row)
            m2_freq = processed_adj_freq.get(col)
            if m1_freq is not None and m2_freq is not None:
                dist = model_analyzer.calculate_frobenius_distance(m1_freq, m2_freq, normalize=True)
                frobenius_freq.loc[row, col] = dist
            else:
                frobenius_freq.loc[row, col] = 0.0
            
            m1_perf = processed_adj_perf.get(row)
            m2_perf = processed_adj_perf.get(col)
            if m1_perf is not None and m2_perf is not None:
                dist_p = model_analyzer.calculate_frobenius_distance(m1_perf, m2_perf, normalize=True)
                frobenius_perf.loc[row, col] = dist_p
            else:
                frobenius_perf.loc[row, col] = 0.0

    if not frobenius_freq.empty and hasattr(writer, 'save_dendrogram'):
        writer.save_dendrogram(
            frobenius_freq,
            filename="archetypes_dendrogram_frobenius.png",
            title="Clustering Archetipi (Distanza Frobenius Frequenza)"
        )
    
    if not frobenius_freq.empty and hasattr(writer, 'save_mds_plot'):
        writer.save_mds_plot(
            frobenius_freq,
            filename="archetypes_mds_plot.png",
            title="MDS Plot: Distanza Strutturale (Frobenius)"
        )

    writer.write_heatmap(jaccard_nodes, title="Jaccard Similarity: Activities", filename="jaccard_nodes_heatmap.png")
    writer.write_heatmap(jaccard_edges, title="Jaccard Similarity: Structure", filename="jaccard_edges_heatmap.png")
    writer.write_heatmap(frobenius_freq, title="Euclidean Distance (Freq)", filename="frobenius_frequency_heatmap.png")
    writer.write_heatmap(frobenius_perf, title="Euclidean Distance (Perf)", filename="frobenius_performance_heatmap.png")
    
    writer.write_dataframe_final_analysis(
        pl.from_pandas(frobenius_freq.reset_index().rename(columns={"index": "archetype"})), 
        "frobenius_distance_frequency.csv"
    )
    
    logger.info("Calcolo differenze a coppie...")
    
    comparison_metrics_records = []
    comparisons = list(combinations(archetype_names, 2))

    for arch1, arch2 in comparisons:
        
        m1 = processed_adj_freq.get(arch1)
        m2 = processed_adj_freq.get(arch2)
        
        if m1 is not None and m2 is not None:
            
            diff_matrix = model_analyzer.calculate_comparison_matrix(
                m1, arch1, m2, arch2, metric="frequency", normalize=True
            )
            
            if not diff_matrix.empty:
                manhattan = diff_matrix.abs().sum().sum()
                comparison_metrics_records.append({
                    "Archetype_A": arch1, "Archetype_B": arch2,
                    "Metric": "Frequency", "Manhattan_Dist": manhattan, "TVD": manhattan/2.0
                })
                
                writer.write_difference_matrix_heatmap(
                    diff_matrix, 
                    title=f"Delta Frequenza: {arch1} vs {arch2}",
                    filename=f"delta_freq_{arch1}_vs_{arch2}.png",
                    metric="frequency"
                )

        p1 = processed_adj_perf.get(arch1)
        p2 = processed_adj_perf.get(arch2)
        
        if p1 is not None and p2 is not None:
            diff_matrix_p = model_analyzer.calculate_comparison_matrix(
                p1, arch1, p2, arch2, metric="performance", normalize=True
            )
            
            if not diff_matrix_p.empty:
                manhattan_p = diff_matrix_p.abs().sum().sum()
                comparison_metrics_records.append({
                    "Archetype_A": arch1, "Archetype_B": arch2,
                    "Metric": "Performance", "Manhattan_Dist": manhattan_p, "TVD": manhattan_p/2.0
                })

                writer.write_difference_matrix_heatmap(
                    diff_matrix_p, 
                    title=f"Delta Performance: {arch1} vs {arch2}",
                    filename=f"delta_perf_{arch1}_vs_{arch2}.png",
                    metric="performance"
                )

    if comparison_metrics_records:
        
        writer.write_dataframe_final_analysis(
            pl.from_dicts(comparison_metrics_records), 
            "structural_distance_metrics.csv"
        )

    logger.info("Analisi comparativa completata.")
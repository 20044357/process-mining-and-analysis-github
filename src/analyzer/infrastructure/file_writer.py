import math
import os
import json
from pathlib import Path
import pickle
from matplotlib import pyplot as plt
from matplotlib.colors import LogNorm, SymLogNorm
import numpy as np
import pandas as pd
import polars as pl
import pm4py
import logging
from typing import Dict, Any, List, Optional, Union
from pm4py.objects.log.exporter.xes import exporter as xes_exporter
from ..application.errors import DataPreparationError
from ..domain.interfaces import IResultWriter
from ..config import AnalysisConfig
import seaborn as sns
import matplotlib.pyplot as plt
from matplotlib.colors import LogNorm, SymLogNorm
import seaborn as sns
from scipy.cluster import hierarchy
from scipy.spatial.distance import squareform
from matplotlib.projections.polar import PolarAxes
from sklearn.manifold import MDS

class FileResultWriter(IResultWriter):

    def __init__(self, config: AnalysisConfig, logger: Optional[Union[logging.Logger, logging.LoggerAdapter]] = None):
        self.config = config
        os.makedirs(self.config.output_directory, exist_ok=True)
        self.logger = logger or logging.getLogger(self.__class__.__name__)
        self.logger.info(f"FileResultWriter inizializzato. Directory di output: {self.config.output_directory}")
    

    def _get_or_create_archetype_path(self, archetype_name: str) -> Path:      
        base_dir = Path(self.config.archetype_process_models_directory)
        archetype_path = base_dir / archetype_name
        archetype_path.mkdir(parents=True, exist_ok=True)
        
        return archetype_path

    def save_event_log(self, event_log: Any, archetype_name: str):
        output_dir = self._get_or_create_archetype_path(archetype_name)
        base_filename = archetype_name.lower().replace(" ", "_")
        output_path = output_dir / f"{base_filename}_log.xes"
        
        self.logger.info(f"Salvataggio dell'EventLog per '{archetype_name}' su: {output_path}")
        
        xes_exporter.apply(event_log, str(output_path))
        self.logger.info("Salvataggio XES completato.")

    def save_model_as_pickle(self, model: Any, archetype_name: str, model_type: str):
        if model_type not in ["frequency", "performance"]:
            raise ValueError("model_type deve essere 'frequency' o 'performance'")
            
        output_dir = self._get_or_create_archetype_path(archetype_name)
        base_filename = archetype_name.lower().replace(" ", "_")
        output_path = output_dir / f"{base_filename}_{model_type}.pkl"
        
        self.logger.info(f"Salvataggio del modello '{model_type}' per '{archetype_name}' su: {output_path}")
        with open(output_path, "wb") as f:
            pickle.dump(model, f)
        self.logger.info(f"Salvataggio Pickle del modello '{model_type}' completato.")

    def save_model_visualization(self, model: Any, archetype_name: str, model_type: str):
        if model_type not in ["frequency", "performance"]:
            raise ValueError("model_type deve essere 'frequency' o 'performance'")
            
        output_dir = self._get_or_create_archetype_path(archetype_name)
        base_filename = archetype_name.lower().replace(" ", "_")
        output_path = output_dir / f"{base_filename}_{model_type}.png"
        
        self.logger.info(f"Salvataggio visualizzazione '{model_type}' per '{archetype_name}' su: {output_path}")
    
        pm4py.save_vis_heuristics_net(model, str(output_path))
            
        self.logger.info(f"Salvataggio visualizzazione '{model_type}' completato")

    def write_archetype_artifact_dataframe(
        self, 
        df: Any, 
        archetype_name: str, 
        filename: str
    ) -> None:
        archetype_dir = self._get_or_create_archetype_path(archetype_name)
        output_path = archetype_dir / filename
        
        self.logger.info(f"Avvio scrittura artefatto DataFrame in: {output_path}")

        try:
            
            df.to_csv(output_path)
            self.logger.info(f"Artefatto DataFrame salvato correttamente in {output_path}.")
        except Exception as e:
            self.logger.error(f"Errore durante la scrittura dell'artefatto DataFrame: {e}", exc_info=True)
            raise DataPreparationError(f"Impossibile scrivere l'artefatto su {output_path}") from e

    def write_metrics_parquet(self, lf: pl.LazyFrame, filename: str) -> None:
        output_path = os.path.join(self.config.output_directory, filename)
        self.logger.info(f"Avvio scrittura del LazyFrame di metriche in: {output_path}")

        try:
            lf.sink_parquet(output_path, compression="zstd")
            self.logger.info("Scrittura completata con successo per il file Parquet ", output_path)
        except Exception as e:
            self.logger.error(f"Errore durante la scrittura del file Parquet: {e}", exc_info=True)
            raise DataPreparationError(f"Impossibile salvare il file Parquet: {output_path}") from e

    def write_stratification_thresholds_json(self, thresholds: Dict[str, Any], filename: str) -> None:
        output_path = os.path.join(self.config.output_directory, filename)
        self.logger.info(f"Avvio scrittura del file JSON delle soglie di stratificazione in: {output_path}")

        try:
            with open(output_path, "w", encoding="utf-8") as f:
                json.dump(thresholds, f, indent=4)
            self.logger.info("File JSON dei quantili per ogni metrica salvato correttamente: ", output_path)
        except Exception as e:
            self.logger.error(f"Errore durante la scrittura del file JSON: {e}", exc_info=True)
            raise DataPreparationError(f"Errore di scrittura del file {filename}") from e

    def write_dataframe(self, df: pl.DataFrame, filename: str) -> None:
        output_path = os.path.join(self.config.output_directory, filename)
        self.logger.info(f"Avvio scrittura DataFrame in: {output_path}")

        try:
            if filename.endswith(".parquet"):
                df.write_parquet(output_path)
            else:
                df.write_csv(output_path)
            self.logger.info("Salvataggio del DataFrame completato correttamente: ", output_path)
        except Exception as e:
            self.logger.error(f"Errore durante la scrittura del DataFrame: {e}", exc_info=True)
            raise DataPreparationError(f"Impossibile scrivere il DataFrame su {output_path}") from e

    def write_dataframe_final_analysis(self, df: pl.DataFrame, filename: str) -> None:
        output_path = os.path.join(self.config.structural_comparison_directory, filename)
        self.logger.info(f"Avvio scrittura DataFrame in: {output_path}")
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        try:
            if filename.endswith(".parquet"):
                df.write_parquet(output_path)
            else:
                df.write_csv(output_path)
            self.logger.info("Scrittura del DataFrame completata correttamente.")
        except Exception as e:
            self.logger.error(f"Errore durante la scrittura del DataFrame: {e}", exc_info=True)
            raise DataPreparationError(f"Impossibile scrivere il DataFrame su {output_path}") from e

    def write_heatmap(self, distance_matrix: Any, title: str, filename: str) -> None:
        output_path = os.path.join(self.config.structural_comparison_directory, filename)
        self.logger.info(f"Generazione heatmap in: {output_path}")

        if distance_matrix.empty:
            self.logger.warning("La matrice delle distanze è vuota. Salto la generazione della heatmap.")
            return
        
        cmap = 'magma' 
        
        if 'jaccard' in filename:
            cmap = 'Blues' 
        elif 'frobenius' in filename: 
            cmap = 'YlOrRd'

        try:
            plt.figure(figsize=(12, 10))
            sns.heatmap(
                distance_matrix,
                annot=True,
                fmt=".3f",
                cmap=cmap, 
                linewidths=.5,
                cbar_kws={'label': 'Valore Metrica'}
            )
            plt.title(title, fontsize=16)
            plt.xticks(rotation=45, ha="right")
            plt.yticks(rotation=0)
            plt.tight_layout()
            plt.savefig(output_path)
            plt.close()
            self.logger.info(f"Heatmap salvata correttamente in {output_path}")
        except Exception as e:
            self.logger.error(f"Errore durante la creazione della heatmap: {e}", exc_info=True)
            raise DataPreparationError(f"Impossibile salvare la heatmap in {output_path}") from e
        
    def write_difference_matrix_heatmap(self, diff_matrix: pd.DataFrame, title: str, filename: str, metric: str) -> None:
            output_dir = self.config.structural_comparison_directory
            os.makedirs(output_dir, exist_ok=True)
            output_path = os.path.join(output_dir, filename)
            
            self.logger.info(f"Generazione heatmap della matrice di differenza in: {output_path}")

            if diff_matrix.empty:
                self.logger.warning("La matrice di differenza è vuota. Salto heatmap.")
                return

            try:
                plt.figure(figsize=(24, 20))
                           
                max_abs_val = diff_matrix.abs().max().max()
                if pd.isna(max_abs_val) or max_abs_val == 0:
                    max_abs_val = 1.0 

                is_normalized = max_abs_val <= 1.001
                norm = None 
         
                if metric == 'performance':
                    if is_normalized:
                        matrix_to_plot = diff_matrix
                        cbar_label = 'Delta Distribuzione Tempo (Proporzione)'
                        fmt = ".2f" 
                    else:
                        matrix_to_plot = diff_matrix / 60
                        cbar_label = 'Delta Tempo (Minuti)'
                        fmt = ".5f"
                        max_abs_val = matrix_to_plot.abs().max().max()
                        norm = SymLogNorm(linthresh=1) 

                elif metric == 'frequency':
                    if is_normalized:
                        matrix_to_plot = diff_matrix
                        cbar_label = 'Delta Frequenza (Proporzione Flusso)'
                        fmt = ".5f" 
                    else:
                        matrix_to_plot = diff_matrix
                        cbar_label = 'Delta Frequenza (Conteggio)'
                        fmt = ".5f"
                        norm = SymLogNorm(linthresh=10) 
                else:
                    matrix_to_plot = diff_matrix
                    cbar_label = 'Differenza'
                    fmt = ".5f"

                min_val = matrix_to_plot.min().min()
                has_negatives = min_val < 0

                if has_negatives:
                    cmap = "RdBu_r"
                    vmin = -max_abs_val 
                    vmax = max_abs_val
                else:            
                    cmap = "Reds"
                    vmin = 0            
                    vmax = max_abs_val
        
                    if norm:
                        if is_normalized:
                            norm = None 
                
                sns.heatmap(
                    matrix_to_plot,
                    annot=True,
                    annot_kws={"size": 8},
                    cmap=cmap,         
                    linewidths=.5,
                    fmt=fmt,   
                    center=0,        
                    vmin=vmin if norm is None else None, 
                    vmax=vmax if norm is None else None,
                    norm=norm,
                    cbar_kws={'label': 'Delta (A - B)'}
                )
                
                cbar = plt.gcf().axes[-1]
                cbar.set_ylabel(cbar_label, fontsize=12)

                plt.title(title, fontsize=20, pad=20)
                plt.xlabel("Attività Target", fontsize=14)
                plt.ylabel("Attività Source", fontsize=14)
                plt.xticks(rotation=90, fontsize=8)
                plt.yticks(rotation=0, fontsize=8)
                plt.tight_layout()
                
                plt.savefig(output_path)
                plt.close()
                self.logger.info(f"Heatmap salvata: {filename}")
            except Exception as e:
                self.logger.error(f"Errore heatmap: {e}", exc_info=True)
                raise DataPreparationError(f"Impossibile salvare heatmap {output_path}") from e


    def save_identikit_image(self, df: pl.DataFrame, filename: str, title: str = "") -> None:
        pdf = df.to_pandas()

        if "Archetype" in pdf.columns:
            pdf = pdf.set_index("Archetype")

        pdf_T = pdf.transpose()
        
        num_metrics = len(pdf_T)
        num_archetypes = len(pdf_T.columns)
        
        figsize_w = 4 + (num_archetypes * 3)
        figsize_h = 2 + (num_metrics * 0.6)
        
        fig, ax = plt.subplots(figsize=(figsize_w, figsize_h))
        
        ax.xaxis.set_visible(False) 
        ax.yaxis.set_visible(False)
        ax.set_frame_on(False)

        cell_text_data = pdf_T.astype(str).values.tolist()
        col_labels_data = pdf_T.columns.tolist()
        row_labels_data = pdf_T.index.tolist()

        table = ax.table(
            cellText=cell_text_data,   
            colLabels=col_labels_data,  
            rowLabels=row_labels_data,  
            loc='center',
            cellLoc='center'
        )

        table.auto_set_font_size(True)
        table.set_fontsize(10)
        table.scale(1.2, 1.5) 

        for (row, col), cell in table.get_celld().items():
            cell.set_edgecolor('white')
            cell.set_linewidth(1)

            if row == 0:
                cell.set_text_props(weight='bold', color='white')
                cell.set_facecolor('#40466e')
            elif col == -1:
                cell.set_text_props(weight='bold', color='#40466e')
                cell.set_facecolor('#f0f0f0')
            else:
                if row % 2 == 0:
                    cell.set_facecolor('#f5f5f5')
                else:
                    cell.set_facecolor('white')

        if title:
            plt.title(title, fontsize=16, pad=20, color='#333333', weight='bold')

        output_path = os.path.join(self.config.structural_comparison_directory, filename)
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        plt.savefig(output_path, bbox_inches='tight', dpi=300)
        plt.close()
        self.logger.info(f"Immagine Identikit salvata in: {output_path}")
    
    def save_radar_chart(self, df: pd.DataFrame, filename: str, title: str):
            output_path = os.path.join(self.config.structural_comparison_directory, filename)
            self.logger.info(f"Generazione Radar Chart in: {output_path}")

            try:
                # 1. FILTRO PER I DUE ARCHETIPI
                # Lavoriamo solo su questi due per il confronto diretto
                target_archetypes = ["Giant_All", "Giant_Pop_LowCollab"]
                
                if "Archetype" in df.columns:
                    df_filtered = df[df["Archetype"].isin(target_archetypes)].copy()
                    if df_filtered.empty:
                        self.logger.warning(f"Nessun dato per {target_archetypes}")
                        return
                    plot_df = df_filtered.set_index("Archetype")
                else:
                    plot_df = df.copy()

                # 2. SELEZIONE DATI NUMERICI
                numeric_cols = plot_df.select_dtypes(include=[np.number]).columns
                plot_df = plot_df[numeric_cols]
                
                # 3. NORMALIZZAZIONE "RATIO TO MAX" (LA FIX)
                # Calcoliamo il massimo SOLO tra questi due archetipi
                max_vals = plot_df.max()
                
                # Evitiamo divisioni per zero
                max_vals[max_vals == 0] = 1 
                
                # Formula: Valore / Massimo
                # Esempio Nodi: 6 / 12 = 0.5 (Visibile!) invece di (6-6)/(12-6)=0 (Invisibile)
                normalized_df = plot_df.div(max_vals)
                
                # Opzionale: Aggiungiamo un piccolo offset base (es. 0.1) se vuoi 
                # evitare che valori molto piccoli siano troppo vicini al centro,
                # ma con la divisione semplice è già molto meglio.

                # --- PLOTTING (Standard) ---
                categories = list(normalized_df.columns)
                N = len(categories)
                angles = [n / float(N) * 2 * math.pi for n in range(N)]
                angles += angles[:1]

                plt.figure(figsize=(10, 10))
                ax = plt.subplot(111, polar=True)  

                plt.xticks(angles[:-1], categories, color='grey', size=10)
                ax.set_rlabel_position(0)  # type: ignore
                
                # Griglia fissa 0.25, 0.50, 0.75, 1.00
                plt.yticks([0.25, 0.5, 0.75, 1.0], ["25%", "50%", "75%", "Max"], color="grey", size=7)
                plt.ylim(0, 1.05) # Un po' di margine oltre l'1

                colors = sns.color_palette("bright", n_colors=len(normalized_df))
                
                for i, (row_name, row_data) in enumerate(normalized_df.iterrows()):
                    values = row_data.to_numpy().flatten().tolist()
                    values += values[:1] 
                    
                    ax.plot(angles, values, linewidth=2, linestyle='solid', label=row_name, color=colors[i])
                    ax.fill(angles, values, color=colors[i], alpha=0.1)

                plt.title(title, size=16, y=1.1)
                plt.legend(loc='upper right', bbox_to_anchor=(0.1, 0.1))
                
                plt.tight_layout()
                plt.savefig(output_path, dpi=300)
                plt.close()
                
            except Exception as e:
                self.logger.error(f"Errore generazione Radar Chart: {e}", exc_info=True)

    def save_dendrogram(self, distance_matrix: pd.DataFrame, filename: str, title: str):
        output_path = os.path.join(self.config.structural_comparison_directory, filename)
        self.logger.info(f"Generazione Dendrogramma in: {output_path}")

        try:
            plt.figure(figsize=(12, 8))
            
            dists = squareform(distance_matrix.values)
            
            linkage_matrix = hierarchy.linkage(dists, method='ward')
            
            hierarchy.dendrogram(
                linkage_matrix,
                labels=distance_matrix.index.tolist(),
                leaf_rotation=90,
                leaf_font_size=12,
            )
            
            plt.title(title, fontsize=16)
            plt.xlabel("Archetipi")
            plt.ylabel("Distanza (Ward)")
            plt.tight_layout()
            plt.savefig(output_path)
            plt.close()
        except Exception as e:
            self.logger.error(f"Errore generazione Dendrogramma: {e}", exc_info=True)

    def save_activity_grouped_bar_chart(self, data: pd.DataFrame, filename: str, title: str, top_n: int = 10):
        output_path = os.path.join(self.config.structural_comparison_directory, filename)
        self.logger.info(f"Generazione Bar Chart Attività in: {output_path}")

        try:
            top_activities = (
                data.groupby("Activity")["Count"]
                .sum()
                .sort_values(ascending=False)
                .head(top_n)
                .index.tolist()
            )
            
            filtered_data = data[data["Activity"].isin(top_activities)]

            plt.figure(figsize=(14, 8))
            sns.barplot(
                data=filtered_data,
                x="Activity",
                y="Count",
                hue="Archetype",
                palette="viridis"
            )
            
            plt.title(title, fontsize=16)
            plt.xticks(rotation=45, ha="right")
            plt.legend(title="Archetipo")
            plt.tight_layout()
            plt.savefig(output_path)
            plt.close()
        except Exception as e:
            self.logger.error(f"Errore generazione Bar Chart: {e}", exc_info=True)

    def save_mds_plot(self, distance_matrix: pd.DataFrame, filename: str, title: str):
        output_path = os.path.join(self.config.structural_comparison_directory, filename)
        self.logger.info(f"Generazione MDS Plot in: {output_path}")

        try:
            mds = MDS(n_components=2, dissimilarity='precomputed', random_state=42, normalized_stress='auto', n_init=4)
            
            coords = mds.fit_transform(distance_matrix)
            
            plt.figure(figsize=(10, 8))

            plt.scatter(coords[:, 0], coords[:, 1], c='dodgerblue', s=150, alpha=0.7, edgecolors='k')
            
            for i, txt in enumerate(distance_matrix.index):
                plt.annotate(
                    txt, 
                    (coords[i, 0], coords[i, 1]), 
                    xytext=(0, 10), 
                    textcoords='offset points',
                    fontsize=11, 
                    ha='center',
                    weight='bold'
                )
                
            plt.title(title, fontsize=16)
            plt.xlabel("Dimensione 1 (Scala Relativa)")
            plt.ylabel("Dimensione 2 (Scala Relativa)")
            plt.grid(True, linestyle='--', alpha=0.5)
            plt.tight_layout()
            
            plt.savefig(output_path, dpi=300)
            plt.close()
            self.logger.info("MDS Plot salvato con successo.")
            
        except Exception as e:
            self.logger.error(f"Errore durante la generazione dell'MDS Plot: {e}", exc_info=True)

    def save_archetype_heatmap(self, matrix: pd.DataFrame, archetype_name: str, filename: str, title: str, metric: str = "frequency"):
            archetype_dir = self._get_or_create_archetype_path(archetype_name)
            output_path = archetype_dir / filename
            
            self.logger.info(f"Salvataggio Heatmap archetipo '{archetype_name}' in: {output_path}")
            
            if matrix.empty:
                return

            try:
                plt.figure(figsize=(24, 20)) 
                
                matrix_to_plot = matrix.copy()
                
                matrix_to_plot[matrix_to_plot <= 1e-9] = np.nan

                max_val = matrix_to_plot.max().max()
                if pd.isna(max_val) or max_val == 0: max_val = 1.0
                
                is_normalized = max_val <= 1.001
                
                norm = None
                fmt = ".2f"
                
                if metric == 'performance':
                    cmap = 'YlOrRd'
                    if is_normalized:
                        cbar_label = 'Tempo (Proporzione Totale)'
                        fmt = ".3f"
                    else:
                        matrix_to_plot = matrix_to_plot / 60.0
                        cbar_label = 'Tempo (Minuti)'
                        fmt = ".2f"
                        new_max = matrix_to_plot.max().max()
                        if new_max > 0:
                            norm = SymLogNorm(linthresh=1, vmin=0, vmax=new_max)

                elif metric == 'frequency':
                    cmap = 'Reds'
                    if is_normalized:
                        cbar_label = 'Frequenza (Proporzione Flusso)'
                        fmt = ".5f"
                    else:
                        cbar_label = 'Frequenza (Conteggio Assoluto)'
                        fmt = ".0f" 
                        if max_val > 0:
                            norm = SymLogNorm(linthresh=1, vmin=0, vmax=max_val)
                else:
                    cmap = 'Greens'
                    cbar_label = 'Valore'

                sns.heatmap(
                    matrix_to_plot, 
                    annot=True,
                    annot_kws={"size": 8},
                    fmt=fmt, 
                    cmap=cmap, 
                    linewidths=.5,
                    norm=norm, 
                    cbar_kws={'label': cbar_label}
                )
                
                plt.title(title, fontsize=20, pad=20)
                plt.xlabel("Attività Target", fontsize=14)
                plt.ylabel("Attività Source", fontsize=14)
                plt.xticks(rotation=90, fontsize=8)
                plt.yticks(rotation=0, fontsize=8)
                plt.tight_layout()
                
                plt.savefig(output_path)
                plt.close()
                
            except Exception as e:
                self.logger.error(f"Errore salvataggio Heatmap archetipo: {e}", exc_info=True)
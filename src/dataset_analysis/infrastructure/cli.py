import argparse
import os
import sys
from dotenv import load_dotenv

# Importa i componenti dell'architettura
from ..config import AnalysisConfig
from ..application.pipeline import AnalysisPipeline, AnalysisMode
from ..infrastructure.data_provider import ParquetDataProvider
from ..infrastructure.pm4py_analyzer import PM4PyAnalyzer
from ..infrastructure.file_writer import FileResultWriter
from ..infrastructure.model_analyzer import PM4PyModelAnalyzer

def main():
    """
    Entry point principale del programma CLI.
    Interpreta i comandi e lancia le pipeline appropriate.
    """
    load_dotenv()

    parser = argparse.ArgumentParser(
        prog="dataset_analyzer",
        description="Esegue pipeline di analisi sui dati GitHub per scoprire i workflow di processo.",
        formatter_class=argparse.RawTextHelpFormatter
    )

    # --- Argomento Posizionale [RICICLATO E MIGLIORATO] ---
    # Definisce l'azione principale da compiere.
    parser.add_argument(
        "command",
        choices=[mode.value for mode in AnalysisMode],
        help=(
            "Il comando (o modalità) da eseguire:\n"
            "  full      - Esegue l'intera pipeline di preparazione dati (Metriche -> Stratificazione).\n"
            "  q1_toxic  - Esegue l'analisi comparativa: Collaborazione 'Sana' vs 'Inefficace'.\n"
            "  q2_hype   - Esegue l'analisi temporale: Impatto dell'Hype sui Processi.\n"
            "  q3_review - Esegue l'analisi evolutiva: Nascita della Code Review."
        )
    )
    
    # --- Argomenti Opzionali [RICICLATI DAL TUO CODICE] ---
    # Questi argomenti permettono di sovrascrivere le configurazioni di default o le variabili d'ambiente.
    parser.add_argument(
        "--dataset-dir",
        default=None,
        help="Override del percorso del dataset (default: env DATASET_PATH o ./data/dataset_distillato)."
    )
    parser.add_argument(
        "--analysis-dir",
        default=None,
        help="Override del percorso di output (default: env DATA_ANALYSIS o ./data/dataset_analyzed)."
    )
    parser.add_argument(
        "--samples",
        type=int,
        default=5, # Un default più ragionevole per le analisi mirate
        help="Numero di repository da campionare per ogni archetipo (default: 5)."
    )
    parser.add_argument(
        "--hm-threshold",
        type=float,
        default=0.5,
        help="Soglia di dipendenza per Heuristic Miner (default: 0.5)."
    )

    args = parser.parse_args()
    
    # --- Esecuzione del Comando ---

    # 1. COSTRUZIONE CONFIGURAZIONE [LOGICA RICICLATA DAL TUO CODICE]
    #    Questa gerarchia (argomento CLI > variabile env > default) è una best practice.
    try:
        dataset_dir = args.dataset_dir or os.environ.get("DATASET_PATH", "data/dataset_distillato")
        analysis_dir = args.analysis_dir or os.environ.get("DATA_ANALYSIS", "data/dataset_analyzed")
        start_date_str = os.environ.get("ANALYSIS_START_DATE")
        end_date_str = os.environ.get("ANALYSIS_END_DATE")

        if not start_date_str or not end_date_str:
            raise ValueError("Variabili d'ambiente ANALYSIS_START_DATE e/o ANALYSIS_END_DATE non trovate.")

        config = AnalysisConfig(
            dataset_base_dir=dataset_dir,
            analysis_base_dir=analysis_dir,
            n_per_strato=args.samples, # n_per_strato è usato in config
            analysis_start_date=start_date_str,
            analysis_end_date=end_date_str
        )
        print(f"Configurazione caricata: {config}")
    
    except Exception as e:
        print(f"[ERRORE FATALE] Errore nella configurazione: {e}")
        sys.exit(1)

    # 2. ASSEMBLAGGIO ADATTATORI [LOGICA RICICLATA DAL TUO CODICE]
    #    La gestione centralizzata degli errori è ottima.
    try:
        # Assicurati che i nomi delle tue classi concrete siano corretti
        # (es. ParquetParquetDataProvider, FileResultWriter)
        provider = ParquetDataProvider(config)
        analyzer = PM4PyAnalyzer(config)
        writer = FileResultWriter(config)
        mode_analyzer = PM4PyModelAnalyzer()
    except Exception as e:
        print(f"[ERRORE FATALE] Impossibile inizializzare gli adattatori: {e}")
        sys.exit(1)

    # 3. CREAZIONE E LANCIO DELLA PIPELINE [LOGICA COMBINATA]
    try:
        # Crea la Pipeline, Iniettando le Dipendenze
        pipeline = AnalysisPipeline(provider, analyzer, writer, config, mode_analyzer)
        
        # Converte l'argomento da stringa a Enum e lancia la pipeline
        execution_mode = AnalysisMode(args.command)
        pipeline.run(mode=execution_mode)

    except Exception as e:
        print(f"\n[ERRORE FATALE] La pipeline in modalità '{args.command}' si è interrotta: {e}")
        # import traceback; traceback.print_exc() # Utile per il debug
        sys.exit(1)

if __name__ == "__main__":
    main()
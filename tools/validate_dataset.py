import argparse
import json
from pathlib import Path
from datetime import datetime, timedelta, date
from typing import Dict, List, Tuple

def get_processed_hours_per_day(output_directory: Path) -> Dict[date, int]:
    """
    Scansiona la struttura del dataset e restituisce un dizionario con i giorni
    trovati e il numero di ore processate per ciascuno, leggendo da index.json.
    
    La struttura attesa è base_dir/YYYY/MM/DD/index.json
    """
    processed_days = {}
    index_files = output_directory.glob("*/*/*/index.json")
    
    for index_path in index_files:
        try:
            day_str = "-".join(index_path.parts[-4:-1])
            current_date = datetime.strptime(day_str, "%Y-%m-%d").date()
            
            with open(index_path, "r") as f:
                data = json.load(f)
            
            hours_count = len(data.get("hours_processed", {}))
            if hours_count > 0:
                processed_days[current_date] = hours_count

        except (ValueError, IndexError, json.JSONDecodeError):
            print(f"[ATTENZIONE] Ignorato file o percorso malformato: {index_path}")
            continue
                
    return processed_days

def find_contiguous_periods(processed_days: Dict[date, int], start_date: date, end_date: date) -> List[Tuple[date, date]]:
    """
    Scansiona un range di date e trova i periodi contigui di giorni presenti.
    """
    contiguous_periods = []
    current_start = None
    
    current_date = start_date
    while current_date <= end_date:
        if current_date in processed_days:
            if current_start is None:
                current_start = current_date
        else:
            if current_start is not None:
                contiguous_periods.append((current_start, current_date - timedelta(days=1)))
                current_start = None
        
        current_date += timedelta(days=1)
        
    if current_start is not None:
        contiguous_periods.append((current_start, end_date))
        
    return contiguous_periods

def main():
    parser = argparse.ArgumentParser(
        description="Valida l'integrità temporale di un dataset, controllando anche le ore mancanti per ogni giorno."
    )
    parser.add_argument(
        "--path",
        required=True,
        help="Percorso alla cartella base del dataset (es. 'data/dataset')."
    )
    args = parser.parse_args()
    
    base_dir = Path(args.path)
    if not base_dir.is_dir():
        print(f"[ERRORE] La cartella specificata non esiste: {base_dir}")
        return

    print(f"Scansione dei file index.json in: {base_dir}...")

    processed_days = get_processed_hours_per_day(base_dir)
    
    if not processed_days:
        print("[ERRORE] Nessun file 'index.json' valido trovato. La struttura attesa è 'YYYY/MM/DD/index.json'.")
        return

    start_date = min(processed_days.keys())
    end_date = max(processed_days.keys())
    
    total_days_in_range = (end_date - start_date).days + 1
    
    print(f"\n--- Report di Validità del Dataset ---")
    print(f"Periodo Totale Rilevato: dal {start_date} al {end_date}")
    print(f"Giorni totali nel range: {total_days_in_range}")
    print(f"Giorni con dati trovati: {len(processed_days)}")
    
    missing_count = total_days_in_range - len(processed_days)
    incomplete_days = {day: hours for day, hours in processed_days.items() if hours < 24}
    
    print(f"Giorni completamente mancanti: {missing_count} ({missing_count / total_days_in_range:.2%})")
    
    if incomplete_days:
        print(f"Giorni presenti ma incompleti: {len(incomplete_days)}")
        for day, hours in sorted(incomplete_days.items())[:5]:
             print(f"  - {day}: contiene solo {hours}/24 ore di dati.")
        if len(incomplete_days) > 5:
            print("  ...")
    else:
        print("Nessun giorno incompleto trovato tra quelli presenti.")

    if missing_count == 0 and not incomplete_days:
        print("\n[OK] Dataset completo! Nessun giorno mancante o incompleto nel range.")
        print("\n--- RACCOMANDAZIONE ---")
        print("Puoi usare l'intero periodo per la tua analisi:")
        print(f"  ANALYSIS_START_DATE=\"{start_date.strftime('%Y-%m-%dT00:00:00Z')}\"")
        print(f"  ANALYSIS_END_DATE=\"{end_date.strftime('%Y-%m-%dT23:59:59Z')}\"")
    else:
        periods = find_contiguous_periods(processed_days, start_date, end_date)
        
        print("\n--- Periodi Contigui di Dati (Top 5 più lunghi) ---")
        if not periods:
            print("Nessun periodo contiguo di dati trovato.")
        else:
            periods.sort(key=lambda p: p[1] - p[0], reverse=True)
            
            for i, (start, end) in enumerate(periods[:5]):
                duration_days = (end - start).days + 1
                print(f"  {i+1}. Da: {start}  A: {end} (Durata: {duration_days} giorni)")

            longest_period = periods[0]
            print("\n--- RACCOMANDAZIONE ---")
            print("Per garantire la coerenza temporale, si consiglia di restringere l'analisi al periodo contiguo più lungo.")
            print("\033[93mATTENZIONE:\033[0m Questo periodo potrebbe contenere giorni con ore di dati mancanti. Controlla il report qui sopra.")
            print(f"  ANALYSIS_START_DATE=\"{longest_period[0].strftime('%Y-%m-%dT00:00:00Z')}\"")
            print(f"  ANALYSIS_END_DATE=\"{longest_period[1].strftime('%Y-%m-%dT23:59:59Z')}\"")

if __name__ == "__main__":
    main()
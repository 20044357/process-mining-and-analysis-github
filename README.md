# Analisi Comportamentale della Collaborazione su GitHub tramite Process Mining su Dati Out-of-Core

## 1. Obiettivo del Progetto

L’obiettivo di questo progetto è confrontare il comportamento collaborativo tra repository appartenenti a strati differenti dell’ecosistema open source, al fine di comprendere quali pattern operativi favoriscano la crescita, la popolarità e la qualità del lavoro di gruppo.

**Process Mining e Workflow Tecnico**

Ogni repository GitHub è il contesto dell'analisi. Per ciascuna di esse, la totalità degli eventi grezzi (IssuesEvent, PullRequestEvent, ecc.) viene filtrata e normalizzata in un set finito di attività tecniche di dominio (es. PullRequestEvent_opened, PushEvent, IssueCommentEvent_created).

Attraverso tecniche di Process Mining (in particolare l'algoritmo Heuristics Miner), queste sequenze di attività normalizzate vengono elaborate per produrre un modello di flusso (workflow) tecnico che descrive il comportamento tipico degli attori (sviluppatori) che hanno contribuito a quella specifica repository.

### Sfida tecnica: Elaborazione Out-of-Core

L’analisi è condotta su dataset derivati da GHArchive, che aggregano milioni di eventi GitHub e raggiungono dimensioni di molti gigabyte. La sfida tecnica cruciale è elaborare questi volumi di dati in modo efficiente e riproducibile, mantenendo un basso consumo di memoria (RAM).

Questo problema è risolto tramite un approccio Out-of-Core, basato sull'uso combinato di Polars in modalità lazy e del formato Parquet:

- **Formato Dati Scalabile**: Durante la fase di ingestion, gli eventi grezzi vengono convertiti nel formato Parquet. Questo permette al sistema di accedere selettivamente solo alle colonne e ai blocchi di dati necessari.

- **Lazy Execution**: La fase di analisi utilizza la libreria Polars in modalità lazy. Questo approccio non carica l'intero dataset in memoria, ma costruisce un piano di esecuzione ottimizzato (simile a una query SQL).

- **Elaborazione in Stream**: Il piano viene valutato in modo differito solo quando i risultati sono richiesti. Questo consente al motore di Polars di elaborare i dati direttamente dai file Parquet su disco (out-of-core), scrivendo i risultati finali in stream e garantendo un utilizzo di RAM costante, indipendente dalla dimensione totale dei dati.

In questo modo, la pipeline gestisce in modo efficiente l'analisi quantitativa su scala globale

---

## 2. Architettura ad Alto Livello: Esagonale (Ports and Adapters)

Il progetto è composto da **tre componenti principali**, progettati per operare come stadi **indipendenti e asincroni** di una pipeline ETL-analitica.

I componenti **dataset_ingestor e dataset_analysis** sono implementati seguendo i principi dell'Architettura Esagonale (Ports and Adapters), garantendo una separazione tra la logica di dominio (Application Layer) e i dettagli tecnici (Infrastructure Layer).

### Componente 1: `dataset_ingestor` — Acquisizione e Distillazione Dati

Questo modulo funge da sistema ETL (Extract–Transform–Load), progettato per gestire l'enorme volume di dati di GHArchive con un focus sulla resilienza e sull'efficienza di archiviazione.

*L'Application Layer* implementa il Caso d'Uso di Ingestione (IngestionService), mentre *l'Infrastructure Layer* fornisce gli Adapters (Porte di Uscita) per l'I/O: l'adattatore *GhArchiveEventSource* per l'acquisizione remota in stream dei dati, l'adattatore *DailyEventFileWriter* per la scrittura resiliente su disco, e il *JsonIngestionIndexRepository* per la gestione dell'indice di stato e la persistenza ottimizzata in Parquet.

#### Funzionalità
- **Check Status**: Per ogni ora da processare, l'indice giornaliero (index.json) viene letto.
- **Salto del Download**: Se un file orario è già stato elaborato con successo o è stato precedentemente marcato come non trovato (errore 404, ecc.), il download viene saltato, velocizzando drasticamente le esecuzioni successive.
- **Acquisizione Resiliente**: Il download avviene solo per le ore non ancora processate. Se un file non viene trovato (HTTP 404), viene marcato nell'indice per evitare tentativi futuri, permettendo al sistema di procedere comunque alla conversione Parquet del giorno parziale.
- **Distillazione e Accumulo**: Gli eventi vengono distillati (mantenendo solo i campi essenziali) e accumulati in un file temporaneo JSONL (events.jsonl) per l'intera giornata.
- **Archiviazione Ottimizzata (Parquet)**: A fine giornata, il file JSONL viene convertito in formato Apache Parquet (operazione cruciale perché riduce drasticamente lo spazio occupato) e viene salvato in una struttura partizionata per data (ottimizzando la lettura lazy per la successiva fase di analisi).

#### Output
Struttura tipica:
```
data/
└── dataset_distillato/
    └── anno=YYYY/
        └── mese=MM/
            └── giorno=DD/
                └── events.parquet  # Input per l'analisi out-of-core
````
---

### Componente 2: `validate_dataset` — Utility di Validazione

Tool che funge da filtro di Controllo Qualità pre-analisi.

Il tool scansiona tutti i metadati (index.json) prodotti dall'Ingestor per trovare e raccomandare l'intervallo temporale contiguo più lungo presente nel dataset. Il suo obiettivo è identificare il periodo ottimale (data di inizio e fine) da usare per l'analisi, evitando buchi temporali che potrebbero falsare i modelli di Process Mining

#### Esempio di esecuzione
```bash
python validate_dataset.py --path data/dataset_distillato
````

Output esempio:

```
--- Report di Validità del Dataset ---
Periodo Totale Rilevato: dal 2025-03-10 al 2025-09-30
Giorni totali nel range: 205
Giorni con dati trovati: 184
Giorni completamente mancanti: 21 (10.24%)
Giorni presenti ma incompleti: 2
  - 2025-03-10: contiene solo 1/24 ore di dati.
  - 2025-06-12: contiene solo 22/24 ore di dati.

--- Periodi Contigui di Dati ---
  1. Da: 2025-04-01  A: 2025-09-30 (Durata: 183 giorni)
  2. Da: 2025-03-10  A: 2025-03-10 (Durata: 1 giorni)

--- RACCOMANDAZIONE ---
  ANALYSIS_START_DATE="2025-04-01T00:00:00Z"
  ANALYSIS_END_DATE="2025-09-30T23:59:59Z"
```

---

### Componente 3: `dataset_analysis` — Motore Analitico e Modellazione Comportamentale

È il **motore analitico** centrale della pipeline.
La sua implementazione in Architettura Esagonale separa i Casi d'Uso (AnalysisPipeline *nell'Application Layer*) dalla logica di accesso ai dati (ParquetDataProvider) e dagli strumenti esterni di analisi (PM4PyAnalyzer). 

Esegue l'intera catena di analisi sui dati validati: dalla stratificazione quantitativa delle repository alla modellazione comportamentale (Process Mining) e alla sintesi finale dei Key Performance Indicators (KPI).

#### Funzionalità

* **Preparazione dati**: carica gli eventi dal Parquet Provider in modalità lazy (Polars). Esegue il filtraggio, la deduplicazione, l'identificazione degli eventi core e la normalizzazione dei dati necessari per le fasi successive.
* **Stratificazione statistica (Out-of-Core)**: classifica tutte le repository in gruppi omogenei basati su metriche predefinite, operando interamente su LazyFrame
* **Process Mining per Campione**: per gli archetipi selezionati, esegue l'analisi di Process Mining (Heuristics Miner, tramite libreria pm4py). L'output è un modello di flusso che descrive il workflow tecnico tipico degli attori per le repository campionate
* **Sintesi KPI**: analizza gli artefatti di Process Mining (i modelli) per estrarre e aggregare KPI quantitativi, come la latenza media di fusione o il tasso di successo delle Pull Request, ecc.

---

## Metodologia di Stratificazione

La stratificazione è il meccanismo chiave per ridurre l'enorme eterogeneità dell'ecosistema GitHub, creando gruppi comparabili di repository sui quali eseguire l'analisi comportamentale.

Ogni repository è classificata su **quattro dimensioni principali**. Per ciascuna dimensione, vengono calcolate due metriche distinte (Cumulativa e Normalizzata per l'età) per un totale di 8 metriche di classificazione:

| Dimensione                     | Significato                      | Eventi considerati                                                                                                                |
| ------------------------------ | -------------------------------- | --------------------------------------------------------------------------------------------------------------------------------- |
| **Popolarità Esterna**         | Interesse esterno                | `WatchEvent (started)`, `ForkEvent`, `ReleaseEvent (published)`                                                                   |
| **Engagement della Community** | Interazione e discussione        | `IssuesEvent (opened/reopened)`, `IssueCommentEvent (created)`                                                                    |
| **Intensità Collaborativa**    | Attività di sviluppo e revisione | `PullRequestEvent (opened/reopened/synchronize)`, `PullRequestReviewEvent`, `PullRequestReviewCommentEvent`, `CommitCommentEvent` |
| **Volume di Lavoro**           | Quantità di codice prodotto      | `PushEvent (push_size)`                                                                                                           |

### Metriche Cumulative vs. Normalizzate:
L'utilizzo di 8 metriche è fondamentale per catturare sia la scala assoluta che l'intensità relativa delle attività:

- **4 Metriche Cumulative (Scala Assoluta)**: Misurano il volume totale e la grandezza dei risultati.
- **4 Metriche Normalizzate (Intensità/Vitalità)**: Correggono il bias legato all'età (dividendo il totale per i giorni di vita). Sono cruciali per classificare correttamente le repository giovani che, pur avendo un volume cumulativo basso, mostrano un'intensità di attività altissima (tante operazioni nei pochi giorni di vita), distinguendole dai progetti maturi o in declino.

### Archetipi:

Le 8 metriche derivanti sono classificate in **cinque categorie discrete** (Zero, Low, Medium, High, Giant).

Le soglie quantitative che definiscono queste categorie non sono fisse, ma sono derivate dinamicamente dal dataset utilizzando i **quantili statistici** (es. 50°, 90°, 99%) calcolati su tutte le 8 metriche.

Con 8 metriche e 5 categorie per ciascuna, lo spazio teorico di classificazione è di 5^8 = **390.625 archetipi possibili**. 

Questo ampio spazio di classificazione è un vantaggio analitico cruciale perché:
- Consente la **Massima Granularità** per catturare le sfumature e le combinazioni rare di comportamento.
- Permette di isolare con precisione **Campioni Puri** (omogenei e mirati) per il Process Mining, anche in uno spazio di dati eterogeneo come GitHub

---

## 4. Installazione e Configurazione

### Installazione

```bash
git clone <URL_REPOSITORY> && cd <NOME_CARTELLA>
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### Configurazione

Copiare il file `.env.example` in `.env` e impostare:

```
DATASET_PATH=./data/dataset_distillato
DATA_ANALYSIS=./results
```

Le date `ANALYSIS_START_DATE` e `ANALYSIS_END_DATE` verranno determinate dallo script `validate_dataset.py`.

---

## 5. Utilizzo e CLI

### `dataset_ingestor`

Comandi principali:

```bash
# Ingestione di un intervallo
python -m dataset_ingestor.infrastructure.cli --download 2024-01-01 2024-01-07

# Ingestione ora singola oppure ore specifiche
python -m dataset_ingestor.infrastructure.cli --hours 2024-01-05T10 2024-01-05T11

# Info dataset
python -m dataset_ingestor.infrastructure.cli --info

# Reset dataset locale
python -m dataset_ingestor.infrastructure.cli --reset
```

---

### `validate_dataset`

```bash
python tools/validate_dataset.py --path data/dataset_distillato
```

Mostra i periodi contigui validi e suggerisce il range consigliato per l’analisi.

---

### `dataset_analysis`

```bash
# Esecuzione completa della pipeline di analisi
python -m dataset_analysis.infrastructure.cli full

# Analisi su un archetipo specifico
python -m dataset_analysis.infrastructure.cli archetype --name giganti_popolari

# Sintesi KPI finale
python -m dataset_analysis.infrastructure.cli summary
```

---

## Artefatti Generati

### Da `dataset_ingestor`

| File             | Descrizione                                                   |
| ---------------- | ------------------------------------------------------------- |
| `index.json`     | Metadati giornalieri: ore processate, errori 404, statistiche |
| `events.parquet` | Dati distillati in formato colonnare, partizionati per data   |

### Da `dataset_analysis`

| File                                            | Descrizione                                                |
| ----------------------------------------------- | ---------------------------------------------------------- |
| `stratification_thresholds.json`                | Soglie quantitative per la categorizzazione                |
| `repositories_stratified.parquet`               | Dataset stratificato per repository                        |
| `group_distribution.csv`                        | Distribuzione delle repository per strato                  |
| `frequency_model.pkl` / `performance_model.pkl` | Modelli di processo (HeuristicsNet) (anche .png)           |
| `filtered_log.xes`                              | Log eventi in formato standard XES                         |
| `quantitative_summary.csv`                      | KPI aggregati (tempi di ciclo, metriche strutturali, ecc.) |

---

## Sviluppi Futuri:

- **Manca fase di test** (attualmente datata a versione precedenti del codice)
- **Manca fase di analisi** tra repository campionate da vari strati con risultati dei confronti visualizzabili tramite jupyter per scoprire pattern operativi favoriscano la crescita, la popolarità e la qualità del lavoro di gruppo.

---

Autore: *Federico Mondelli*

---

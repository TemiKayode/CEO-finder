# CEO-finder

One-file CSV pipeline that dedupes contacts by email domain and uses Tavily (**one search per domain**) to find/validate the current CEO, then writes `final.csv` + `left.csv` with resumable caching and strict name cleaning.

## What it does

- **Reads**: Apollo contact CSV exports
- **Dedupes**: contacts by email (so each email appears at most once in outputs)
- **Enriches**: one Tavily lookup per unique email domain (not per row)
- **Assigns**: the found CEO name to every contact that shares the same domain
- **Resumes safely**: progress is cached; you can interrupt and re-run

## Outputs

Files are written to `output/result/`:

- **`final.csv`**: rows with a Tavily-verified valid CEO name
- **`left.csv`**: rows where no CEO name was found
- **`lefts.csv`**: mirror of `left.csv`

## Setup

### 1) Create a virtual environment (recommended)

```bash
python -m venv .venv
```

Windows PowerShell:

```powershell
.\.venv\Scripts\Activate.ps1
```

### 2) Install dependencies

```bash
pip install -r requirements.txt
```

### 3) Configure Tavily (optional but recommended)

Create `.env` (you can copy `.env.example`) and set:

```env
TAVILY_API_KEY=your_key_here
```

If you don't set a key (or you pass `--no-tavily`), the script will still run and produce outputs, but CEO values will remain blank.

## Run

```bash
python -u pipeline.py --input "MyFile.csv"
```

Common options:

- `--no-tavily`: run without API calls (fills cache with empty results for remaining domains)
- `--limit N`: only process the first N remaining domains
- `--reset`: clear cache and reprocess all domains
- `--clean`: re-validate output CSVs only (no API)
- `--strict-clean`: strict-validate cache + rebuild outputs (no API)
- `--lenient`: loosen CEO name validation rules

Outputs are written to `output/result/`.

## Notes

- This project intentionally keeps generated data out of Git (`output/` is ignored).
- Tavily billing typically counts **API search requests**; this pipeline can do **up to 3 searches per domain** (see `tv_lookup`).


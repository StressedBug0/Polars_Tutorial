# Polars Tutorial

A single-file, step-by-step introduction to **Polars**, showcasing data inspection, selection, transformation, aggregation, joining, and saving—all within a regular Python script.

> **If anything is incorrect or unclear, please let me know!**

---

## Features

- **Fake dataset** generation using NumPy (only here; the rest is pure Polars)  
- Essential DataFrame operations:  
  - **Inspection**: `head()`, `tail()`, `.schema`, `.describe()`  
  - **Selection**: `.select()`, `.slice(start, length)`, `.filter(pl.col(...))`  
  - **Column ops**: `.with_columns()`, vector math, string methods (`.str.to_lowercase()`)  
  - **Missing values**: `.is_null()`, `.fill_null()`  
  - **Grouping & aggregation**: `.groupby().agg()` with named outputs  
  - **Sorting & counts**: `.sort()`, `.value_counts()`  
  - **Combining**: `pl.concat()`, `.join()`  
  - **Saving & loading**: `.write_csv()`, `.write_parquet()`, `pl.read_csv()`  
- Prints everything to the console—ideal for VS Code’s *Run Python File* button  
- No external data required beyond the built-in fake dataset  

---

## Requirements

- Python 3.9+ (tested on 3.12)  
- **polars** (`pip install polars`)  
- **NumPy** (`pip install numpy`)  
- *(Optional)* VS Code or any editor/terminal  

---

## Real-World Use Cases

| Scenario                         | How Polars Helps                                                             |
|----------------------------------|-------------------------------------------------------------------------------|
| **Large CSV processing**         | Fast, low-memory `pl.read_csv()` with on-the-fly filtering                    |
| **ETL pipelines**                | Chainable, lazy execution and efficient `.with_columns()` + `.groupby().agg()` |
| **Data cleansing**               | Quick `.filter()`, `.is_null()` + `.fill_null()` operations                   |
| **Time-series summarization**    | `.slice()`, `.groupby()`, `.sort()` for rolling or resampling                |
| **Dashboard back-ends**          | Lightning-fast aggregations feeding Streamlit or other BI tools               |
| **In-memory joins**              | High-performance `.join()` for lookups and enrichment                         |
| **Export to downstream systems** | One-line `.write_csv()` / `.write_parquet()` for clean hand-offs              |

> Master these basics once, and harness Polars’ speed for any table-shaped task—from real-time analytics to batch ETL jobs.

---

## Installation & Run

1. Clone this repo:
    ```bash
    git clone https://github.com/yourusername/polars_tutorial.git
    cd polars_tutorial
    
    ```

2. Create and activate the environment:
    ```bash
    conda create -n polars-env python=3.12.9
    conda activate polars-env

    ```

3. Install dependencies:

    ```bash
    pip install -r imports.txt

    ```
    
4. Run the program:
    ```bash
    python main.py

    ```
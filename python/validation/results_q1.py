#!/usr/bin/env python
"""
Se calculan los resultados de la Q1 leyendo directamente del
parquet (o sea no se pasa por el sistema distribuido) 

Requiere de: 
    pip install pyarrow

Ejemplo de uso:
    python3 validation/results_q1.py oracle --clients 2 datasets/enwiki_00000.parquet datasets/frwiki_00000.parquet
"""
import os
import glob
import sys
import csv
import argparse

FILTER_YEAR = 2020
ENGLISH_LANGUAGE = "en"

#Columnas que me interesan para esta query. Se asume que el parquet tiene estas columnas.
COLUMNS = ["url", "date_modified", "in_language"]

def to_str(value):
    if isinstance(value, str):
        return value
    if value is not None : return str(value)
    return ""

def flatten(value):
    if isinstance(value, dict):
        return value.get("identifier", "")
    return value

def q1_rows(records):
    for r in records:
        date = to_str(r["date_modified"])
        language = to_str(flatten(r["in_language"]))

        if int(date[:4]) <= FILTER_YEAR and language == ENGLISH_LANGUAGE:
            yield (to_str(r["url"]),date)

def parquet_files(path):
    if os.path.isdir(path):
        files = sorted(glob.glob(os.path.join(path, "*.parquet")))
        if not files: raise FileNotFoundError(f"No parquet files found in {path}")
        return files
    if os.path.isfile(path): return [path]
    raise FileNotFoundError(f"Invalid file path: {path}")

def read_records(path):
    import pyarrow.parquet as pq
    for file in parquet_files(path):
        for batch in pq.ParquetFile(file).iter_batches(batch_size=1000, columns=COLUMNS):
            yield from batch.to_pylist()

def main():
    p = argparse.ArgumentParser()
    p.add_argument("out_dir")
    p.add_argument("inputs", nargs="+", help="archivo o carpeta de parquets (ruta del HOST)")
    p.add_argument("--clients", type=int, default=1)
    args = p.parse_args()
 
    for i in range(args.clients):
        source = args.inputs[i % len(args.inputs)]
        client_dir = os.path.join(args.out_dir, f"client_{i}")
        os.makedirs(client_dir, exist_ok=True)
        path = os.path.join(client_dir, "result_Q1.csv")
        count = 0
        with open(path, "w", newline="") as f: #que se sobreescriba 
            writer = csv.writer(f, delimiter=",", quotechar='"')
            for row in q1_rows(read_records(source)):
                writer.writerow(row)
                count += 1
        print(f"client_{i}: {count} filas de Q1 desde {source} -> {path}")
    return 0
 
 
if __name__ == "__main__":
    sys.exit(main())
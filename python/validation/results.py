#!/usr/bin/env python
"""
Calcula los resultados esperados de las queries (Q1, Q2, Q3, Q4, Q5) leyendo
directamente de los archivos parquet en UNA SOLA PASADA (sin pasar por el sistema distribuido)
y los guarda como archivos estáticos en:

    expected/<nombre_del_dataset>/result_Q<N>.csv

Requiere de:
    pip install pyarrow nltk

Ejemplo de uso:

     .venv/bin/activate
     
     # Calcular todas las queries (1 a 5) juntas:
     python3 validation/results.py expected datasets --force

     # Calcular solo queries específicas:
     python3 validation/results.py expected datasets -q 1 2 5 --force
"""
import argparse
import collections
import csv
import glob
import json
import os
import re
import sys
try:
    import compare
except ImportError:
    from validation import compare

FILTER_YEAR = 2020
ENGLISH_LANGUAGE = "en"
WORD_ONLY_REGEX = re.compile(r'\b[^\W\d_]+\b')
BIOGRAPHY_SECTION_NAMES = {'biographie', 'biography'}

COLUMNS_BY_QUERY = {
    1: ["url", "date_modified", "in_language"],
    2: ["name", "url", "date_modified", "sections"],
    3: ["abstract", "in_language"],
    4: ["abstract"],
    5: ["abstract", "date_modified", "url"],
}


def get_stopwords_set():
    try:
        from nltk.corpus import stopwords
        return set(stopwords.words('english')) | set(stopwords.words('french'))
    except (ImportError, LookupError):
        try:
            import nltk
            nltk.download('stopwords', quiet=True)
            from nltk.corpus import stopwords
            return set(stopwords.words('english')) | set(stopwords.words('french'))
        except Exception as e:
            print(f"Advertencia: no se pudieron cargar las stopwords de nltk: {e}", file=sys.stderr)
            return set()


def to_str(value):
    if isinstance(value, str):
        return value
    if value is not None:
        return str(value)
    return ""


def flatten(value):
    if isinstance(value, dict):
        return value.get("identifier", "")
    return value


def has_biography_section(sections):
    if not isinstance(sections, str):
        return False
    try:
        parsed = json.loads(sections)
        if not isinstance(parsed, list):
            return False
        for section in parsed:
            if isinstance(section, dict) and section.get("name", "").lower() in BIOGRAPHY_SECTION_NAMES:
                return True
    except (json.JSONDecodeError, TypeError):
        return False
    return False


def parquet_files(path):
    if os.path.isdir(path):
        files = sorted(glob.glob(os.path.join(path, "*.parquet")))
        if not files:
            raise FileNotFoundError(f"No se encontraron archivos parquet en: {path}")
        return files
    if os.path.isfile(path):
        return [path]
    raise FileNotFoundError(f"Ruta de archivo inválida: {path}")


def read_records(path, columns):
    import pyarrow.parquet as pq
    for file in parquet_files(path):
        for batch in pq.ParquetFile(file).iter_batches(batch_size=2000, columns=columns):
            yield from batch.to_pylist()


def compute_expected(source, expected_dir, queries, force=False, stopwords_set=None):
    """
    Calcula los resultados de las queries seleccionadas para una fuente (archivo o directorio)
    en una única pasada de lectura.
    """
    # Determinar qué queries realmente necesitamos procesar
    active_queries = []
    target_paths = {}
    tmp_paths = {}

    for q in sorted(queries):
        path = compare.expected_file(expected_dir, source, q)
        target_paths[q] = path
        if os.path.exists(path) and not force:
            print(f"{source}: ya existe {path}, no se pisa (usar --force para regenerarlo)")
        else:
            active_queries.append(q)

    if not active_queries:
        return

    # Determinar columnas necesarias para las queries activas
    columns_needed = set()
    for q in active_queries:
        columns_needed.update(COLUMNS_BY_QUERY[q])
    columns_needed = sorted(columns_needed)

    # Preparar archivos de salida para streaming (Q1 y Q2 escriben directamente fila a fila)
    q1_file = None
    q1_writer = None
    q1_count = 0

    q2_file = None
    q2_writer = None
    q2_count = 0

    for q in active_queries:
        path = target_paths[q]
        os.makedirs(os.path.dirname(path), exist_ok=True)
        tmp = path + ".tmp"
        tmp_paths[q] = tmp

        if q == 1:
            q1_file = open(tmp, "w", newline="", encoding="utf-8")
            q1_writer = csv.writer(q1_file, delimiter=",", quotechar='"')
        elif q == 2:
            q2_file = open(tmp, "w", newline="", encoding="utf-8")
            q2_writer = csv.writer(q2_file, delimiter=",", quotechar='"')

    # Estructuras en memoria para queries agregadas (Q3, Q4, Q5)
    need_stopwords = bool({3, 4, 5} & set(active_queries))
    if need_stopwords and stopwords_set is None:
        stopwords_set = get_stopwords_set()

    q3_english_words = set()
    q3_french_words = set()

    q4_word_counts = collections.Counter()

    q5_hist_total_length = 0
    q5_hist_quantity = 0
    q5_recent_records = []

    try:
        for r in read_records(source, columns_needed):
            # Parsear fecha y año si alguna query activa lo necesita
            year = None
            date_str = ""
            if {1, 2, 5} & set(active_queries):
                date_str = to_str(r.get("date_modified"))
                if len(date_str) >= 4 and date_str[:4].isdigit():
                    year = int(date_str[:4])

            # URL y Name
            url = to_str(r.get("url"))
            name = to_str(r.get("name"))

            # Idioma (aplanado)
            in_language = ""
            if {1, 3} & set(active_queries):
                in_language = to_str(flatten(r.get("in_language")))

            # === Q1: date <= 2020 and en ===
            if 1 in active_queries and year is not None and year <= FILTER_YEAR and in_language == ENGLISH_LANGUAGE:
                q1_writer.writerow([url, date_str])
                q1_count += 1

            # === Q2: year <= 2020 and has biography section ===
            if 2 in active_queries and year is not None and year <= FILTER_YEAR:
                if has_biography_section(r.get("sections")):
                    q2_writer.writerow([name, url])
                    q2_count += 1

            # === Palabras para Q3, Q4, Q5 ===
            if need_stopwords:
                abstract = to_str(r.get("abstract"))
                if abstract:
                    words = WORD_ONLY_REGEX.findall(abstract.lower())
                    filtered_words = [w for w in words if w not in stopwords_set]

                    # Q3: palabras por idioma (en vs fr)
                    if 3 in active_queries and in_language in ("en", "fr"):
                        if in_language == "en":
                            q3_english_words.update(filtered_words)
                        elif in_language == "fr":
                            q3_french_words.update(filtered_words)

                    # Q4: palabras únicas por artículo
                    if 4 in active_queries:
                        unique_words = set(filtered_words)
                        q4_word_counts.update(unique_words)

                    # Q5: longitud de palabras sin stopwords comparada con histórico
                    if 5 in active_queries and year is not None and url:
                        length = len(filtered_words)
                        if year <= FILTER_YEAR:
                            q5_hist_total_length += length
                            q5_hist_quantity += 1
                        else:
                            q5_recent_records.append((url, length))

        # Cerrar archivos de streaming (Q1 y Q2)
        if q1_file:
            q1_file.close()
            q1_file = None
            os.replace(tmp_paths[1], target_paths[1])
            print(f"{source}: {q1_count} filas de Q1 -> {target_paths[1]}")

        if q2_file:
            q2_file.close()
            q2_file = None
            os.replace(tmp_paths[2], target_paths[2])
            print(f"{source}: {q2_count} filas de Q2 -> {target_paths[2]}")

        # Finalizar y escribir Q3
        if 3 in active_queries:
            intersection = sorted(q3_english_words.intersection(q3_french_words))
            with open(tmp_paths[3], "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f, delimiter=",", quotechar='"')
                for word in intersection:
                    writer.writerow([word])
            os.replace(tmp_paths[3], target_paths[3])
            print(f"{source}: {len(intersection)} filas de Q3 -> {target_paths[3]}")

        # Finalizar y escribir Q4
        if 4 in active_queries:
            top_20 = sorted(q4_word_counts.items(), key=lambda x: (-x[1], x[0]))[:20]
            with open(tmp_paths[4], "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f, delimiter=",", quotechar='"')
                for word, count in top_20:
                    writer.writerow([word, str(count)])
            os.replace(tmp_paths[4], target_paths[4])
            print(f"{source}: {len(top_20)} filas de Q4 -> {target_paths[4]}")

        # Finalizar y escribir Q5
        if 5 in active_queries:
            avg_length = q5_hist_total_length / q5_hist_quantity if q5_hist_quantity > 0 else 0
            q5_count = 0
            with open(tmp_paths[5], "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f, delimiter=",", quotechar='"')
                for url, length in q5_recent_records:
                    if length > avg_length:
                        writer.writerow([url, str(length)])
                        q5_count += 1
            os.replace(tmp_paths[5], target_paths[5])
            print(f"{source}: {q5_count} filas de Q5 (promedio histórico={avg_length:.2f}) -> {target_paths[5]}")

    except BaseException:
        if q1_file:
            q1_file.close()
        if q2_file:
            q2_file.close()
        for tmp in tmp_paths.values():
            if os.path.exists(tmp):
                try:
                    os.remove(tmp)
                except OSError:
                    pass
        raise


def main():
    p = argparse.ArgumentParser(description="Calcula los resultados esperados para las queries especificadas.")
    p.add_argument("expected_dir", help="Carpeta donde se guardan los esperados (ej: expected)")
    p.add_argument("inputs", nargs="+", help="Archivo o carpeta de parquets (ej: datasets/enwiki_00000.parquet o datasets)")
    p.add_argument("-q", "--queries", nargs="+", type=int, choices=[1, 2, 3, 4, 5], default=[1, 2, 3, 4, 5],
                   help="Lista de queries a calcular (default: 1 2 3 4 5)")
    p.add_argument("--force", action="store_true", help="Pisar archivos esperados que ya existen")
    args = p.parse_args()

    stopwords_set = None
    if {3, 4, 5} & set(args.queries):
        stopwords_set = get_stopwords_set()

    for source in args.inputs:
        compute_expected(source, args.expected_dir, args.queries, force=args.force, stopwords_set=stopwords_set)

    return 0


if __name__ == "__main__":
    sys.exit(main())

"""
Compara los resultados de una corrida contra los esperados.

Cuando se tienen varios nodos el orden en el que llegan los resultados puede variar, es decir, es no determinista.
Por eso se compara como conjuntos, no como listas ordenadas. Se ignora el orden pero se detectan filas faltantes, de mas o duplicadas.

Ademas, si hay algun archivo de mas en 'expected' sin 'output' falla.

"""
import csv
from collections import Counter
import os
 
ROWS_IN_FAIL = 8 #cuantas filas mostrar en caso de error

def read_rows(path):
    with open(path, newline="") as f:
        rows= Counter()
        for row in csv.reader(f):
            rows.update([tuple(row)])
        return rows

def compare_multiset(got,expected):
    """
    Devuelve (ok,detalles).
    Cada fila se identifica por su contenido completo.
    """
    missing = expected - got
    extra = got - expected
    ok = not missing and not extra

    duplicated = Counter()
    unexpected = Counter()

    # Se clasifican a los elementos de extra en duplicados o inesperados
    for elemento, cantidad in extra.items():
        if elemento in expected:
            duplicated[elemento] = cantidad
        else:
            unexpected[elemento] = cantidad

    return ok, {"missing": missing, "unexpected": unexpected,"duplicated": duplicated}

COMPARATORS = {
    1: compare_multiset
}

def show(prefix,counter):
    for row, count in list(counter.items())[:ROWS_IN_FAIL]:
        suffix = f"  (x{count})" if count > 1 else ""
        print(f"{prefix}{list(row)}{suffix}")


def dataset_name(input_path):
    """
    Devuelve el nombre del dataset a partir de la ruta del parquet.
    Ej: datasets/enwiki_00000.parquet -> enwiki_00000
    """
    return os.path.splitext(os.path.basename(os.path.normpath(input_path)))[0]


def expected_file(expected_dir, input_path, query_number):
    """
    Devuelve la ruta del archivo de resultados esperados para un dataset y query.
    Ej: expected/enwiki_00000/result_Q1.csv
    """
    return os.path.join(expected_dir, dataset_name(input_path), f"result_Q{query_number}.csv")
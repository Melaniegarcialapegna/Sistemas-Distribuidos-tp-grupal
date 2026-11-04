"""
Compara los resultados de una corrida contra los esperados.

Cuando se tienen varios nodos el orden en el que llegan los resultados puede variar, es decir, es no determinista.
Por eso se compara como conjuntos, no como listas ordenadas. Se ignora el orden pero se detectan filas faltantes, de mas o duplicadas.

Ademas, si hay algun archivo de mas en 'expected' sin 'output' falla.

"""
import csv
from typing import Counter

def read_rows(path):
    with open(path, newline="") as f:
        rows= []
        for row in csv.reader(f):
            rows.extend(row)
        return tuple(rows)

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

def list_clients(directory):
    found = []
    for entry in os.listdir(directory):
        m = CLIENT_DIR.match(entry)
        if m and os.path.isdir(os.path.join(directory, entry)):
            found.append((int(m.group(1)), entry))
    return [name for _, name in sorted(found)]
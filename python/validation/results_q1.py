#!/usr/bin/env python
"""
Se calculan los resultados de la Q1 leyendo directamente del
parquet (o sea no se pasa por el sistema distribuido) 

Requiere de: 
    pip install pyarrow
"""

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

def q1_row(records):
    for r in records:
        date = to_str(r["date_modified"])
        language = to_str(flatten(r["in_language"]))

        if int(date[:4]) <= FILTER_YEAR and language == ENGLISH_LANGUAGE:
            yield (to_str(r["url"]),date)


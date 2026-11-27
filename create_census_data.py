import argparse
import json
import os
import random
import shutil
from pathlib import Path

from pyspark.sql import types as T


SOURCE_URL = "data/census_2010.json"

RAW_SCHEMA = T.StructType([
    T.StructField(name, T.LongType())
    for name in ["year", "age", "females", "males"]
] + [T.StructField("_corrupt_record", T.StringType())])


# ---------- Synthetische Daten ----------

def read_jsonl(path):
    return [
        json.loads(line)
        for line in Path(path).read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


def write_jsonl(path, rows):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "".join(
            json.dumps(row, separators=(",", ":")) + "\n"
            for row in rows
        ),
        encoding="utf-8",
    )


def mortality(age, sex):
    # Rein didaktische Parameter, keine amtliche Sterbetafel.
    if age == 0:
        q = 0.004
    elif age < 15:
        q = 0.00015
    elif age < 40:
        q = 0.0006
    elif age < 65:
        q = 0.003
    elif age < 80:
        q = 0.015
    else:
        q = min(0.25, 0.03 + 0.006 * (age - 80))

    return q * (1.15 if sex == "males" else 0.90)


def next_year(previous, seed=42):
    previous = sorted(previous, key=lambda row: row["age"])
    year = previous[0]["year"] + 1

    assert {row["year"] for row in previous} == {year - 1}
    assert [row["age"] for row in previous] == list(range(101))

    rng = random.Random(seed + year)
    result = []

    for age in range(101):
        row = {"year": year, "age": age}

        for sex in ["females", "males"]:
            if age == 0:
                # Vereinfachtes Geburtenmodell.
                value = (
                    previous[0][sex]
                    * 1.004
                    * (1 + rng.uniform(-0.002, 0.002))
                )
            else:
                # Kohorten altern; 100+ nimmt 99-Jährige auf
                # und behält Überlebende der bisherigen offenen Gruppe.
                origins = [age - 1] if age < 100 else [99, 100]

                survivors = sum(
                    previous[a][sex] * (1 - mortality(a, sex))
                    for a in origins
                )

                migration = 0.003 if 18 <= age < 50 else 0.0005
                value = survivors * (
                    1 + migration + rng.uniform(-0.001, 0.001)
                )

            row[sex] = max(0, round(value))

        result.append(row)

    return result


def prepare_data(folder):
    folder = Path(folder)
    folder.mkdir(parents=True, exist_ok=True)

    original = folder / "census_2010.json"
    if not original.exists():
        shutil.copyfile(SOURCE_URL, original)
    base = read_jsonl(original)
    previous = base

    for year in [2011, 2012]:
        previous = next_year(previous)
        write_jsonl(folder / f"census_{year}.json", previous)

    corrected = [dict(row) for row in base]
    row40 = next(row for row in corrected if row["age"] == 40)
    row40["females"] += 10_000
    write_jsonl(folder / "census_2010_revision2.json", corrected)

    invalid = [dict(row) for row in next_year(base)]
    invalid[20]["females"] = None
    invalid.append(dict(invalid[30]))
    write_jsonl(folder / "census_2011_invalid.json", invalid)




# ---------- Kommandozeile ----------

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--raw", default="data/raw")
    args = parser.parse_args()
    prepare_data(args.raw)


if __name__ == "__main__":
    main()
import json
import yaml
import os
import csv
import glob
import re
import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity as sk_cosine

# === CONFIGURATION ===
BASE_DIR         = os.path.dirname(os.path.abspath(__file__))
OPENAPI_PATH     = os.path.join(BASE_DIR, "mapping", "schema", "kraken-schema.json")
CSV_DIR          = os.path.join(BASE_DIR, "data")
OUTPUT_DIR       = os.path.join(BASE_DIR, "mapping", "mapping_templates")
SAMPLE_ROWS      = 10   # how many CSV data rows to sample per column
FIELD_THRESHOLD  = 0.05 # min TF-IDF cosine score to record a column match for a field
ENTITY_THRESHOLD = 0.08 # min avg top-3-field score for an entity to be written out
# Note: TF-IDF scores are lower than neural embeddings — 0.1 is a strong match


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def normalise_name(name: str) -> str:
    """Convert UPPER_CASE / snake_case / camelCase to spaced lowercase words."""
    name = re.sub(r"([a-z])([A-Z])", r"\1 \2", name)
    return re.sub(r"[_\-]+", " ", name).lower().strip()


def strip_html(text: str) -> str:
    return re.sub(r"<[^>]+>", "", text).strip()


def get_all_csvs(csv_dir):
    return glob.glob(os.path.join(csv_dir, "*.csv"))


def load_csv(csv_path):
    """Return (headers, sample_rows) where sample_rows is a list of dicts."""
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        headers = [h.strip() for h in reader.fieldnames]
        rows = []
        for i, row in enumerate(reader):
            if i >= SAMPLE_ROWS:
                break
            rows.append({k.strip(): v.strip() for k, v in row.items()})
    return headers, rows


def build_column_context(col: str, rows: list) -> str:
    """
    Combine the normalised column name with unique sample values so TF-IDF
    sees both what the column is called and what data it contains.

    E.g.  "city: Adelaide Perth Melbourne Sydney Brisbane"
    """
    seen, values = set(), []
    for row in rows:
        v = row.get(col, "").strip()
        if v and v not in seen:
            seen.add(v)
            values.append(v)
    sample = " ".join(values[:8]) if values else ""
    return f"{normalise_name(col)} {sample}".strip()


def build_field_context(field: str, field_schema: dict) -> str:
    """
    Combine the normalised field name with its schema description and any
    enum labels so TF-IDF sees rich, human-readable text for each field.

    E.g. for 'locality':
      "locality city town portion address Australian suburb town"
    """
    parts = [normalise_name(field)]

    desc = strip_html(field_schema.get("description", ""))
    if desc:
        parts.append(desc)

    enum_descs = field_schema.get("x-enum-descriptions", {})
    if enum_descs:
        labels = [str(v) for v in list(enum_descs.values())[:8] if str(v) not in ("None", "")]
        if labels:
            parts.append(" ".join(labels))
    elif "enum" in field_schema:
        enums = [str(e) for e in field_schema["enum"] if e]
        if enums:
            parts.append(" ".join(enums[:8]))

    return " ".join(parts)


# ---------------------------------------------------------------------------
# Core matching
# ---------------------------------------------------------------------------

def build_vectorizer(all_texts: list):
    """Fit a TF-IDF vectorizer on the combined corpus of column + field texts."""
    vectorizer = TfidfVectorizer(
        analyzer="word",
        ngram_range=(1, 2),   # unigrams + bigrams
        min_df=1,
        sublinear_tf=True,
    )
    vectorizer.fit(all_texts)
    return vectorizer


def score_and_map_entity(entity_name, entity_schema, col_keys, col_matrix, vectorizer):
    """
    For every field in the entity schema find the best-matching CSV column via
    TF-IDF cosine similarity.

    Returns (entity_score, mapping_dict) or (0, None) if not a strong enough match.
    """
    properties = entity_schema.get("properties", {})
    if not properties:
        return 0.0, None

    field_keys = list(properties.keys())
    field_texts = [build_field_context(f, properties[f]) for f in field_keys]
    field_matrix = vectorizer.transform(field_texts)

    # similarity matrix: rows=fields, cols=csv columns
    sim_matrix = sk_cosine(field_matrix, col_matrix)  # shape (n_fields, n_cols)

    column_map = {}
    top_scores = []

    for i, field in enumerate(field_keys):
        scores = sim_matrix[i]
        best_j = int(np.argmax(scores))
        best_score = float(scores[best_j])
        best_col = col_keys[best_j]

        top_scores.append(best_score)

        if best_score >= FIELD_THRESHOLD:
            column_map[field] = {"source_column": best_col, "confidence": round(best_score, 3)}
        else:
            column_map[field] = {"source_column": None, "confidence": round(best_score, 3)}

    # Entity relevance = average of the top-3 field scores
    top_scores.sort(reverse=True)
    entity_score = float(np.mean(top_scores[:3])) if top_scores else 0.0

    if entity_score < ENTITY_THRESHOLD:
        return entity_score, None

    mapping = {
        "kraken_entity": entity_name,
        "required_fields": entity_schema.get("required", []),
        "column_map": column_map,
    }
    return entity_score, mapping


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    print("Loading Kraken schema...")
    with open(OPENAPI_PATH, "r", encoding="utf-8") as f:
        openapi = json.load(f)
    entities = openapi.get("components", {}).get("schemas", {})

    csv_files = get_all_csvs(CSV_DIR)
    if not csv_files:
        print(f"No CSV files found in: {CSV_DIR}")
        return

    print(f"\nFound {len(csv_files)} CSV file(s):\n")

    for csv_path in csv_files:
        csv_name = os.path.splitext(os.path.basename(csv_path))[0]
        csv_output_dir = os.path.join(OUTPUT_DIR, csv_name)
        os.makedirs(csv_output_dir, exist_ok=True)

        print(f"  Processing: {os.path.basename(csv_path)}")
        headers, rows = load_csv(csv_path)
        col_keys = headers
        col_texts = [build_column_context(col, rows) for col in col_keys]

        # Collect all field texts across every entity to fit the vocabulary
        all_field_texts = []
        for entity_schema in entities.values():
            props = entity_schema.get("properties", {})
            for field, field_schema in props.items():
                all_field_texts.append(build_field_context(field, field_schema))

        print(f"    Building TF-IDF vocabulary ({len(col_texts)} columns, "
              f"{len(all_field_texts)} schema fields)...")
        vectorizer = build_vectorizer(col_texts + all_field_texts)
        col_matrix = vectorizer.transform(col_texts)

        # Score every entity
        matches = []
        for entity_name, entity_schema in entities.items():
            score, mapping = score_and_map_entity(
                entity_name, entity_schema, col_keys, col_matrix, vectorizer
            )
            if mapping is not None:
                matches.append((score, entity_name, mapping))

        matches.sort(key=lambda x: x[0], reverse=True)

        for score, entity_name, mapping in matches:
            out_path = os.path.join(csv_output_dir, f"{entity_name}.yml")
            with open(out_path, "w", encoding="utf-8") as out_file:
                yaml.dump(mapping, out_file, sort_keys=False, allow_unicode=True)

        print(f"    -> {len(matches)} mapping template(s) written to: mapping/mapping_templates/{csv_name}/")
        if matches:
            print(f"    Top match: {matches[0][1]} (score: {matches[0][0]:.3f})\n")
        else:
            print()


if __name__ == "__main__":
    main()
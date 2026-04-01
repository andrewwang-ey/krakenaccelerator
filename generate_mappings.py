import json
import yaml
import os
import csv
import glob

# === CONFIGURATION ===
BASE_DIR     = os.path.dirname(os.path.abspath(__file__))
OPENAPI_PATH = os.path.join(BASE_DIR, "mapping", "schema", "kraken-schema.json")  # Path to your OpenAPI JSON schema
CSV_DIR      = os.path.join(BASE_DIR, "data")                                      # Folder containing client CSV files
OUTPUT_DIR   = os.path.join(BASE_DIR, "mapping", "mapping_templates")             # Output directory for mapping files

def get_csv_columns(csv_path):
    with open(csv_path, newline='', encoding='utf-8') as csvfile:
        reader = csv.reader(csvfile)
        return [col.strip() for col in next(reader)]  # First row is header

def get_all_csvs(csv_dir):
    return glob.glob(os.path.join(csv_dir, "*.csv"))

def generate_mapping(entity_name, entity_schema, csv_columns):
    required_fields = entity_schema.get("required", [])
    properties = entity_schema.get("properties", {})
    column_map = {}
    for field in properties.keys():
        # Case-insensitive match to CSV columns
        match = next((col for col in csv_columns if col.lower() == field.lower()), None)
        column_map[field] = match
    mapping = {
        "kraken_entity": entity_name,
        "required_fields": required_fields,
        "column_map": column_map
    }
    return mapping

def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    with open(OPENAPI_PATH, "r", encoding="utf-8") as f:
        openapi = json.load(f)

    entities = openapi.get("components", {}).get("schemas", {})

    csv_files = get_all_csvs(CSV_DIR)
    if not csv_files:
        print(f"No CSV files found in: {CSV_DIR}")
        return

    print(f"Found {len(csv_files)} CSV file(s) in {CSV_DIR}:\n")

    for csv_path in csv_files:
        csv_name = os.path.splitext(os.path.basename(csv_path))[0]
        csv_output_dir = os.path.join(OUTPUT_DIR, csv_name)
        os.makedirs(csv_output_dir, exist_ok=True)

        print(f"  Processing: {os.path.basename(csv_path)}")
        csv_columns = get_csv_columns(csv_path)

        generated = 0
        for entity_name, entity_schema in entities.items():
            # Only write a mapping if at least one Kraken field matches a CSV column
            properties = entity_schema.get("properties", {})
            has_match = any(
                col.lower() == field.lower()
                for field in properties.keys()
                for col in csv_columns
            )
            if not has_match:
                continue

            mapping = generate_mapping(entity_name, entity_schema, csv_columns)
            out_path = os.path.join(csv_output_dir, f"{entity_name}.yml")
            with open(out_path, "w", encoding="utf-8") as out_file:
                yaml.dump(mapping, out_file, sort_keys=False)
            generated += 1

        print(f"    → {generated} mapping template(s) written to: mapping/mapping_templates/{csv_name}/\n")

if __name__ == "__main__":
    main()
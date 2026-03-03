#!/usr/bin/env python3
"""
Schema Discovery Script
Run this ONCE to print the exact column names of your key tables.
Paste the output back so the prompts can be fixed with real column names.

Usage:
    python discover_schema.py
"""

import sys
import os
sys.path.insert(0, os.path.dirname(__file__))

from database import DatabaseManager

def discover():
    db = DatabaseManager()

    # Key tables we need exact columns for
    key_tables = [
        "TERP_LS_CONTRACT",
        "TERP_LS_TENANTS",
        "TERP_LS_CONTRACT_CHARGES",
        "TERP_LS_CONTRACT_UNIT",
        "TERP_LS_PROPERTY_UNIT",
        "TERP_LS_PROPERTY_UNIT_TYPE",
        "TERP_LS_PROPERTY_UNIT_STATUS",
        "TERP_LS_PROPERTY",
        "TERP_LS_CONTRACT_SPLIT_PAYMENT",
        "TERP_ACC_VOUCHER_CHEQUES",
        "TERP_ACC_BOUNCED_VOUCHERS",
        "TERP_ACC_TENANT_RECEIPT",
    ]

    print("=" * 70)
    print("SCHEMA DISCOVERY RESULTS")
    print("=" * 70)

    for table in key_tables:
        try:
            schema = db.get_table_schema(table)
            row_count = db.get_row_count(table)
            print(f"\nTABLE: {table}  (rows: {row_count})")
            print("-" * 50)
            for col in schema["columns"]:
                pk = " [PK]" if col["name"] in schema["primary_keys"] else ""
                fk_info = ""
                for fk in schema["foreign_keys"]:
                    if fk["column"] == col["name"]:
                        fk_info = f"  → {fk['references_table']}.{fk['references_column']}"
                print(f"  {col['name']:<35} {col['type'].upper()}{pk}{fk_info}")

            # Sample 3 rows to see actual data values
            sample = db.execute_query(f"SELECT * FROM `{table}` LIMIT 3")
            if sample.get("success") and sample.get("rows"):
                print(f"\n  SAMPLE DATA (3 rows):")
                cols = sample["column_names"]
                for row in sample["rows"]:
                    row_dict = dict(zip(cols, row))
                    # Show only interesting columns (not huge text/blob)
                    interesting = {k: v for k, v in row_dict.items()
                                   if v is not None and len(str(v)) < 60}
                    print(f"    {interesting}")
        except Exception as e:
            print(f"\nTABLE: {table}  ERROR: {e}")

    print("\n" + "=" * 70)
    print("PASTE THIS OUTPUT BACK TO GET FIXED PROMPTS")
    print("=" * 70)

    db.close()

if __name__ == "__main__":
    discover()

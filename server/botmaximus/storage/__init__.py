"""Two stores, one discipline.

Postgres owns everything the system decides or commits. Parquet on object
storage owns the permanent record. Nothing else exists — no third store, no
cache layer, no local JSON stand-in.
"""

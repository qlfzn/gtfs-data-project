"""
Docstring for main

Read txt with duckdb
"""
import duckdb
import pandas as pd

df = pd.read_csv("data/routes.txt")

query = duckdb.sql("select * from df")

query.show()
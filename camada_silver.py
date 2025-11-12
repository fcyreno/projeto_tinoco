import numpy as np
import pandas as pd
from ydata.profiling import ProfileReport
import os

DIR_BASE = os.path.dirname(__file__)
RAW_PATH = os.path.join(DIR_BASE, "dataset\\raw")
BRONZE_PATH = os.path.join(DIR_BASE, "dataset\\bronze")
SILVER_PATH = os.path.join(DIR_BASE, "dataset\\silver")
#GOLD_PATH = os.path.join(DIR_BASE, "dataset\\gold")

# Leitura do dataset bronze
df = pd.read_parquet('dataset/bronze/')

df = df.drop(columns=['codigo_elemento_despesa', 'codigo_funcao'])
profile = ProfileReport(df, title="Profiling Report")

profile.to_file('report.html')

#df = df.drop(columns=['codigo_favorecido'])
#print(df)
#print(df.info())
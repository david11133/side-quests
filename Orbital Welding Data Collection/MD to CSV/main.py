import pandas as pd

df = pd.read_table("table.md", sep="|", engine="python", skipinitialspace=True)
df = df.dropna(axis=1, how="all")  # remove empty columns caused by leading/trailing pipes
df.to_csv("table.csv", index=False)
df.to_excel("table.xlsx", index=False)

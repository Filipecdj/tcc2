import pandas as pd
import sys
#!/usr/bin/python3

# Lê o arquivo CSV da entrada padrão
file = pd.read_csv(sys.stdin)

grouped = file.groupby(['Country_Name', 'Gender']).size().unstack(fill_value=0).reset_index()

# Removendo o nome da coluna 'Gender'
grouped.columns.name = None
# Imprime o resultado
grouped.to_csv(sys.stdout, index=False)
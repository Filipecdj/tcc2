import pandas as pd
import sys
#!/usr/bin/python3

# Lê o arquivo CSV da entrada padrão
file = pd.read_csv(sys.stdin)

# Calcula a média de idade por nacionalidade
average_age_by_nationality = file.groupby('Nationality')['Age'].mean().reset_index()

# Imprime o resultado
average_age_by_nationality.to_csv(sys.stdout, index=False)

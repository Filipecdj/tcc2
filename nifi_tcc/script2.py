import pandas as pd
import sys
#!/usr/bin/python3

# Lê o arquivo CSV da entrada padrão
file = pd.read_csv(sys.stdin)

# Agrupe os dados pela coluna 'Country Name' e conte a quantidade de voos em cada país
flight_counts = file.groupby('Country_Name').size().reset_index(name='Flight_Count')

# Imprime o resultado
flight_counts.to_csv(sys.stdout, index=False)

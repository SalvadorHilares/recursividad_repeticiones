import pandas as pd

df = pd.read_csv('baby-names-state.csv')

df1 = df[df['state_abb'] == 'CA']

name = df1['name'].value_counts().idxmax()

print(name)

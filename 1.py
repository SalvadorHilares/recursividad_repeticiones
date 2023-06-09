from joblib import Parallel, delayed
from tqdm import tqdm
import pandas as pd

def calcula(df):
    # do some processing here with the df parameter
    df_ca = df[df['state_abb'] == 'CA']
    
    if df_ca.empty:
        return None, 0
    
    counts = df_ca['name'].value_counts()
    name = counts.idxmax()
    count = counts.max()
    return name, count
    
filename = "baby-names-state.csv"
batch_size = 1000000

data_frames = pd.read_csv(filename, chunksize=batch_size)

results = Parallel(n_jobs=-1)(delayed(calcula)(df) for df in tqdm(data_frames))

name_counts = {}
for name, count in results:
    if name is not None:
        name_counts[name] = name_counts.get(name, 0) + count

max_name = max(name_counts, key=name_counts.get)
max_name_count = name_counts[max_name]

print(f"Nombre : {max_name}, Cantidad: {max_name_count}")

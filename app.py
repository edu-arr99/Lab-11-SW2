from joblib import Parallel, delayed
import pandas as pd
from tqdm import tqdm # to show a progress bar

batches = []

for df in tqdm(pd.read_csv('baby-names-state.csv', chunksize=10000)):
    batches.append(df)

def merge_dicts(dicts):
    merged_dict = {}
    for dictionary in dicts:
        for key, value in dictionary.items():
            if key in merged_dict:
                merged_dict[key] += value
            else:
                merged_dict[key] = value
    return merged_dict

def count(row, names):
    name = row.loc["name"]
    if row.loc["state_abb"] == "CA":
        if row["name"] not in names:
            names[name] = 1
        else:
            names[name] += 1
        # print (names[name])
    
    return names


def process(batch):
    names = {}
    for row in batch.iterrows():
        count(row[1], names)
    return names


results = Parallel(n_jobs=-1)(delayed(process)(batch) for batch in batches)
names = merge_dicts(results)
key_with_highest_value = max(names, key=names.get)
print(key_with_highest_value, names[key_with_highest_value])
from joblib import Parallel, delayed
import pandas as pd


df = pd.read_csv('baby-names-state.csv')

CA = df[df["state_abb"] == "CA"]

name_counts = CA['name'].value_counts().to_dict()

most_common_name = max(name_counts, key=name_counts.get)

print("Most common name in CA:", most_common_name)
print("Count:", name_counts[most_common_name])
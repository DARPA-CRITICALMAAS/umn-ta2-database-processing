import pickle

with open('/home/yaoyi/pyo00005/CriticalMAAS/umn-ta2-database-processing/procmine/_entities/_selected_cols.pkl', 'rb') as handle:
    dict_data = pickle.load(handle)

print(dict_data)
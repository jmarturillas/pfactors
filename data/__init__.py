
import pandas as pd


my_dict = {
    'name': ["a", "b", "c", "d"],
    'age': [1, 2, 3, 4],
    'designation' : ["VP", "VP", "VP", "VP"]

}

my_data = pd.DataFrame(my_dict)




my_data.to_csv("sample.csv", index=False, header=False, sep=":")

print(pd.read_csv("sample.csv", names=['pangalan', 'edad', 'posisyon']))



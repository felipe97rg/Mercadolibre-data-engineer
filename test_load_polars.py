import sys
import os
sys.path.append(os.path.abspath("src"))

from data_loader_polars import load_all_data

prints, taps, pays = load_all_data()

print("\nPRINTS")
print(prints.head(5))

print("\nTAPS")
print(taps.head(5))

print("\nPAYS")
print(pays.head(5))

import sys
import os
sys.path.append(os.path.abspath("src"))

from data_loader import load_all_data

prints, taps, pays = load_all_data()

print("\nPRINTS")
print(prints.head())

print("\nTAPS")
print(taps.head())

print("\nPAYS")
print(pays.head())

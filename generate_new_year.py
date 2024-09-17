from tkinter import *
import Budget as budget_lib
import sys

budget = budget_lib.Budget()
input_year = sys.argv[1]
try:
    budget._insert_new_year(int(input_year))
except Exception as e:
    print(f"'{input_year}' is not a valid year. {e}")


from tkinter.filedialog import askopenfilename

import importlib
budget_backend = importlib.import_module('budget')
filename = askopenfilename(initialdir=r'C:\Users\James\OneDrive\Budget\Budget Files\Transactions')
trans = budget_backend.import_transactions(filename, logging=False)
budget_backend.insert_transactions(trans, logging=False)


from tkinter.filedialog import askopenfilename
import Budget as budget_lib
budget = budget_lib.Budget()
filename = askopenfilename(initialdir=f"{budget.get_working_dir()}\\Transactions")
trans = budget.import_transactions(filename, logging=False)
budget.insert_transactions(trans, logging=False)


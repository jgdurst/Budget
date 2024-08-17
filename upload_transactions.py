from tkinter.filedialog import askopenfilenames
import Budget as budget_lib
budget = budget_lib.Budget()
filenames = askopenfilenames(initialdir=f"{budget.get_working_dir()}\\Transactions")
for i,filename in enumerate(filenames):
    print(f"{i}) === Importing file ({filename})")
    trans = budget.import_transactions(filename, logging=False)
    budget.insert_transactions(trans, logging=False)
    print(f"{i}) === Import Complete\n")


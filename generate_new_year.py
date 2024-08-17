from tkinter.filedialog import askopenfilename
import Budget as budget_lib
budget = budget_lib.Budget()
filename = askopenfilename(initialdir=f"{budget.get_working_dir()}\\Allocations")
allocations = budget.import_allocations(filename)
budget.mass_allocation(allocations, 'set')
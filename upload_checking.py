import importlib
budget_backend = importlib.import_module('Budget')

budget_backend.import_insert_transactions('Checking', 'Checking Transactions.csv')
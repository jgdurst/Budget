import importlib
budget_backend = importlib.import_module('budget')

budget_backend.import_insert_transactions('CreditCard', 'CreditCard Transactions.csv')
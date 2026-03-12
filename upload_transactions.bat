@echo off
title Upload Transactions

call activate budget

python code\upload_transactions.py

pause
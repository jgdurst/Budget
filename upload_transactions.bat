@echo off
title Upload Transactions

call activate budget

python upload_transactions.py

pause
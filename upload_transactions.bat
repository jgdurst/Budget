@echo off
title Upload Transactions

call "C:\Users\jgdur\Anaconda3\Scripts\activate.bat" budget

python "C:\Users\jgdur\Documents\GitHub\Budget\upload_checking.py"

python "C:\Users\jgdur\Documents\GitHub\Budget\upload_savings.py"

python "C:\Users\jgdur\Documents\GitHub\Budget\upload_creditcard.py"

pause
@echo off
title Upload Allocations

call activate budget

python refresh_budget_db.py

pause
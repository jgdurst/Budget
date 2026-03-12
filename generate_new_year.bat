@echo off
title Upload Allocations

call activate budget

python code\generate_new_year.py %1

pause
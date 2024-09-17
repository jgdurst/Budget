@echo off
title Upload Allocations

call activate budget

python generate_new_year.py %1

pause
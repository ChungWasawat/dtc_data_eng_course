import sys
import pandas as pd

print(sys.argv)

# sys.argv[0] = name of this file
# [1, ...] next argument
day = sys.argv[1]

print(f"job finished successfully for day = {day}")
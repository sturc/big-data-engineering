# %% [markdown]
# # Generate Sudoku Quizzes with different difficulties and one or more solutions

# %%
import sys
sys.path.append("..")
from sudoku import Sudoku, DIFFICULTY_LEVELS
import random
import csv
import numpy as np

# %%
path = "../sudoku_data/"
random.seed(1234)

# %%
diff_levels = list(DIFFICULTY_LEVELS.keys())
diff_levels.remove("Pretty hard")
diff_levels.remove("Hard")
result = list()
# write the headers
with open(path+'sudokufile.csv','w',newline='') as out:
    csv_out=csv.writer(out,delimiter=',',quoting=csv.QUOTE_NONE, escapechar='\\')
    csv_out.writerow(['quizzes','solutions','difficulty','num_solutions'])       
for i in range (100):
    for i in range(10000):
        quiz, solution, diff_level, num_sol = Sudoku().generate(random.choice(diff_levels))
        result.append ((''.join(map(str, np.concatenate(quiz))),
                        ''.join(map(str, np.concatenate(solution))), 
                        diff_level,num_sol)) 
    # write the result to file
    with open(path+'sudokufile.csv','a',newline='') as out:
        csv_out=csv.writer(out,delimiter=',',quoting=csv.QUOTE_NONE, escapechar='\\')
        csv_out.writerows(result)



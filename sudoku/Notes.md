# Implementation Idea for Sudoku

The Sudoku data example can be used for several tasks

- sequence the data (every number should get a single column) = ``sudoku_sequencing.ipynb``
- Test the data quality by checking the correctness of the solution =``sudoku_quality_test.ipynb``
  - Implement the test file in Spark =``sudoku_quality_test_spark.ipynb``
- Test if there is a single solution of the Sudoku puzzle = ``sudoku_single_solution_exists.ipynb``
- Remodel the data => make single columns out of the Array = ``sudoku_as_array.ipynb``
- Calc the difficulty mode of the sample = = ``sudoku_single_solution_exists.ipynb``
- Generate new Sudokus with SudokuSolver & SudokuGenerator ``sudoku_generator.ipynb`` and ``sudoku_generator.py``
- Label the puzzles as valid (solveable and one unique solution exists) or invalid (TODO continue)
- Create a deep Learing Model to solve the puzzle <https://www.kaggle.com/code/yashchoudhary/deep-sudoku-solver-multiple-approaches/notebook>

## Create a Deep Neural Network

- GAN (Genearative aversarial Network) might be a try
- CNN (Convolutional Neural Network) might also be a try

## Implementation notes

- Input are 81 numbers, 0 representing the empty cells. Represented in two Dimentions (9x9). As such we need a input shape of 9,9,1 .
- Output are 81 x 9 numbers. 81 rows, representing the cells and 9 columns, representing the classes 1 to 9

- Create Class Labels 
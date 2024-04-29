import random
import sys
import numpy as np

DIFFICULTY_LEVELS = {"Pretty easy": 28,
                     "Easy":39,
                     "Medium":53,
                     "Hard":64, 
                     "Pretty hard":71}

class Sudoku:
    """
    Represents a Sudoku board.
    The implementation is based on https://github.com/Periculum/SudokuGenerator

    The Sudoku class provides methods to initialize a Sudoku object, reset the board, convert the board to an SVG image,
    generate a Sudoku board with a specified difficulty level, evaluate the number of empty cells based on the difficulty level,
    print the board in the console, check if a number is valid in a given row, column, and square, and solve the Sudoku board using backtracking.
    """

    def __init__(self, board:np.array=None):
        """
        Initializes a Sudoku object.
        """
        if board is None: 
            self.reset()
        else :
            self.board = np.empty((9,9), dtype=int) 
            self.board[:] = board

    def reset(self):
        """
        Resets the Sudoku board to an empty 9x9 grid.
        """
        rows = 9
        columns = 9
        self.board = [[0 for j in range(columns)] for i in range(rows)]

    def toSVG(self):
        """
        Converts the Sudoku board to an SVG image.

        Returns:
            str: SVG representation of the Sudoku board.
        """
        # Variables
        cell_size = 40
        line_color = "black"

        # creating a rectangle in white with the size of a 9x9-Sudoku
        svg = '<svg xmlns="http://www.w3.org/2000/svg" version="1.1">'
        svg += f'<rect x="0" y="0" width="{9 * cell_size}" height="{9 * cell_size}" fill="white" />'

        # Draw the grid lines
        for i in range(10):
            line_width = 2 if i % 3 == 0 else 0.5
            # row lines
            svg += f'<line x1="{i * cell_size}" y1="0"  x2="{i * cell_size}" y2="{9 * cell_size}" \
                            style="stroke:{line_color}; stroke-width:{line_width}" />'
            # column lines
            svg += f'<line x1="0" y1="{i * cell_size}"  x2="{9 * cell_size}" y2="{i * cell_size}" \
                            style="stroke:{line_color}; stroke-width:{line_width}" />'

        # Draw the numbers
        for row in range(9):
            for column in range(9):
                if self.board[row][column] != 0:
                    svg += f'<text x="{(column + 0.5) * cell_size}" y="{(row + 0.5) * cell_size}" \
                                    style="font-size:20; text-anchor:middle; dominant-baseline:middle"> {str(self.board[row][column])} </text>'

        svg += '</svg>'
        return svg

    def generate(self, difficulty:str)->tuple[np.ndarray,np.ndarray,str, int]:
        """
        Generates a Sudoku board with the specified difficulty level.

        Args:
            difficulty (str): The difficulty level of the Sudoku board.

        Returns:
            tuple: Consisting of the quiz, a solution, the difficulty level and the number of pos. solutions
        """
        self.fill_with_random_numbers()

        # fill rest
        for solutions in self.solve():
            break
        # save solution
        solution = np.copy(self.board)
        # empty cells 
        empty_success = False
        while not empty_success :
            empty_success = self.empty_cells_from_board(difficulty)
        
        # checking how many solutions are in the board
        num_solutions = len(list(self.solve()))
        return self.board, solution, difficulty,num_solutions




    def fill_with_random_numbers(self)->None: 
     # fill diagonal squares
        for i in range(0, 9, 3):
            square = [1, 2, 3, 4, 5, 6, 7, 8, 9]
            random.shuffle(square)
            for r in range(3):
                for c in range(3):
                    self.board[r + i][c + i] = square.pop()

    def empty_cells_from_board(self, difficulty : str) -> bool:
        # difficulty
        empty_cells = self.get_num_of_empty_cells_by_diff_level(difficulty)
        # creating a list of coordinates to visit and shuffling them
        unvisited = [(r, c) for r in range(9) for c in range(9)]
        random.shuffle(unvisited)

        # remove numbers
        while empty_cells > 0 and len(unvisited) > 0:
            # saving a copy of the number, just in case, if we can't remove it
            r, c = unvisited.pop()
            backup_copy = self.board[r][c]
            self.board[r][c] = 0
            if empty_cells > DIFFICULTY_LEVELS.get("Easy") :
             # checking how many solutions are in the board             
                solutions = list(self.solve())
                if len(solutions) > 1:
                    self.board[r][c] = backup_copy
                else :
                    empty_cells -= 1
            else :
                empty_cells -= 1
        # if unvisited is empty, but empty_cells not -> trying again
        if empty_cells > 0:
            print("No Sudoku found. Trying again.")
            return False
        else:
            return True


    def get_num_of_empty_cells_by_diff_level(self, difficulty:str)->int:
        """
        Evaluates the number of empty cells based on the specified difficulty level.

        Args:
            difficulty (str): The difficulty level of the Sudoku board.

        Returns:
            int: The number of empty cells in the Sudoku board.
        """
        empty_cells = DIFFICULTY_LEVELS.get(difficulty,3)
        # reduce every number by 3
        empty_cells -=3
        return empty_cells


    def evaluate(self)->str:
        """
        Evaluates the difficulty level of the board based on the number of empty cells

        Returns:
            str: The name of the difficulty level
        """
        empty_cells = np.count_nonzero(self.board == 0)
        if empty_cells == 0: 
            return "Solved"
        for diff_level, value in DIFFICULTY_LEVELS.items():
             if value <= empty_cells:
                 return diff_level
        return "Diabolical"

    def print(self):
        """
        Prints the Sudoku board in the console.
        """
        for i in range(9):
            print(" ".join([str(x) if x != 0 else "." for x in self.board[i]]))

    def number_is_valid(self, row, column, number):
        """
        Checks if a number is valid in the given row, column, and square.

        Args:
            row (int): The row index.
            column (int): The column index.
            number (int): The number to be checked.

        Returns:
            bool: True if the number is valid, False otherwise.
        """
        # check row and column
        for i in range(9):
            if self.board[row][i] == number or self.board[i][column] == number:
                return False

        # check square
        start_column = column // 3 * 3
        start_row = row // 3 * 3
        for i in range(3):
            for j in range(3):
                if self.board[i + start_row][j + start_column] == number:
                    return False
        return True

    def solve(self):
        """
        Solves the Sudoku board using backtracking.

        Yields:
            bool: True if the Sudoku board is solved, False otherwise.
        """
        # find an empty cell
        for r in range(9):
            for c in range(9):
                if self.board[r][c] == 0:
                    # for every empty cell fill a valid number into it
                    for n in range(1, 10):
                        if self.number_is_valid(r, c, n):
                            self.board[r][c] = n
                            # is it solved?
                            yield from self.solve()
                            # backtrack
                            self.board[r][c] = 0
                    return False
        yield True
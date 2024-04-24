"""Helper functions for sudoku data conversion"""

import numpy as np
from pyspark.sql import Row

QUIZ_ATTR = ["Q1A","Q1B","Q1C","Q1D","Q1E","Q1F","Q1G","Q1H","Q1I","Q2A","Q2B","Q2C","Q2D","Q2E","Q2F","Q2G","Q2H","Q2I","Q3A","Q3B","Q3C","Q3D","Q3E","Q3F","Q3G","Q3H","Q3I","Q4A","Q4B","Q4C","Q4D","Q4E","Q4F","Q4G","Q4H","Q4I","Q5A","Q5B","Q5C","Q5D","Q5E","Q5F","Q5G","Q5H","Q5I","Q6A","Q6B","Q6C","Q6D","Q6E","Q6F","Q6G","Q6H","Q6I","Q7A","Q7B","Q7C","Q7D","Q7E","Q7F","Q7G","Q7H","Q7I","Q8A","Q8B","Q8C","Q8D","Q8E","Q8F","Q8G","Q8H","Q8I","Q9A","Q9B","Q9C","Q9D","Q9E","Q9F","Q9G","Q9H","Q9I"]
SOLUTION_ATTR = ["S1A","S1B","S1C","S1D","S1E","S1F","S1G","S1H","S1I","S2A","S2B","S2C","S2D","S2E","S2F","S2G","S2H","S2I","S3A","S3B","S3C","S3D","S3E","S3F","S3G","S3H","S3I","S4A","S4B","S4C","S4D","S4E","S4F","S4G","S4H","S4I","S5A","S5B","S5C","S5D","S5E","S5F","S5G","S5H","S5I","S6A","S6B","S6C","S6D","S6E","S6F","S6G","S6H","S6I","S7A","S7B","S7C","S7D","S7E","S7F","S7G","S7H","S7I","S8A","S8B","S8C","S8D","S8E","S8F","S8G","S8H","S8I","S9A","S9B","S9C","S9D","S9E","S9F","S9G","S9H","S9I"]

def convert_to_block (row: Row, attr_list=QUIZ_ATTR) ->np.array:  
  """transform one row to a 9x9 np.array"""
  list_of_values = convert_to_array(row)
  return np.reshape(np.array(list_of_values),(9,9))

def convert_to_array (row: Row, attr_list=QUIZ_ATTR) ->np.array:
  """transforms on row to a one dimensional np.array """
  return [row[x] for x in attr_list] 
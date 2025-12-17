import numpy as np
from scipy.optimize import linear_sum_assignment
import sqlite3


def hungarian_algorithm(cost_matrix):
    """
    Solves the assignment problem using the Hungarian algorithm.

    Args:
        cost_matrix (numpy.ndarray): A 2D array representing the cost matrix.

    Returns:
        list of tuples: Each tuple is (worker_index, task_index)
    """
    workers, tasks = cost_matrix.shape

    if workers == tasks:
        row_ind, col_ind = linear_sum_assignment(cost_matrix)
        total_assignment_arr = [(row_ind[i], col_ind[i]) for i in range(len(row_ind))]
        return total_assignment_arr

    elif workers > tasks:
        # Pad with max value columns to make square
        max_in_matrix = np.max(cost_matrix)
        col_vector = np.full((workers, 1), max_in_matrix)
        for _ in range(workers - tasks):
            cost_matrix = np.hstack((cost_matrix, col_vector))

        row_ind, col_ind = linear_sum_assignment(cost_matrix)
        total_assignment_arr = [
            (row_ind[i], col_ind[i])
            for i in range(len(row_ind))
            if col_ind[i] < tasks  # Only include real tasks
        ]
        return total_assignment_arr

    else:
        return skewed_hungarian(cost_matrix)


def skewed_hungarian(cost_matrix):
    """
    Handles the case where there are more tasks than workers by splitting the matrix.

    Args:
        cost_matrix (np.ndarray): The cost matrix where tasks > workers.

    Returns:
        list of tuples: Each tuple is (worker_index, task_index)
    """
    cost_matrix = cost_matrix.copy()
    n, m = cost_matrix.shape
    original_n = n
    total_assignment_arr = []

    if n < m:
        rem = m % n
        loop_time = m // n
        total = rem + loop_time

        for i in range(total):
            matrix = cost_matrix[:, i * n:(i + 1) * n]
            chunk_assignments = hungarian_algorithm(matrix)
            for row, col in chunk_assignments:
                task_index = i * original_n + col
                total_assignment_arr.append((row, task_index))

    return total_assignment_arr

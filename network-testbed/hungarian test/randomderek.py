import numpy as np


def random(cost_matrix):
    workers, task = cost_matrix.shape
    total_assignment_arr = []

    for i in range(task):
        #min_val = float('inf')
        #min_ind = None
        currval = np.random.randint(0, workers)
        total_assignment_arr.append((currval, i))#publisher assigned to task j

    return total_assignment_arr


def bruteforce(cost_matrix):
    workers, task = cost_matrix.shape
    total_assignment_arr = []

    for i in range(task):
        min_val = float('inf')
        min_ind = None
        for j in range(workers):
            if(min_val>cost_matrix[j][i]):
                min_val = cost_matrix[j][i]
                min_ind = j
        total_assignment_arr.append((min_ind , i))

    return total_assignment_arr


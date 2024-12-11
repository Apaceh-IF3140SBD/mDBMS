import re

def remove_duplicates(data):
    seen_data = []
    res = []
    for row in data:
        if row not in seen_data:
            seen_data.append(row)
            res.append(row)
    return res

def split_dot_contained_data(data):
    with_dot = [x for x in data if re.search(r'\.', x)]
    without_dot = [x for x in data if not re.search(r'\.', x)]
    
    return with_dot, without_dot
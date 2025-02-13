
import os
def get_files(path):
    """Lists all files in the specified directory."""
    files = []
    for entry in os.listdir(path):
        if os.path.isfile(os.path.join(path, entry)):
            files.append(entry)
    return files

for fname in get_files('./'):
    print (fname)
    with open(fname, "r") as f:
        for line in f:
            if 'np.double' in line:
                if ')' in line:
                    if 'aggregations' not in line:
                       pass
                       # print("Need to update: ", line)
                    else:
                        print(line)
                else:
                    next_line = next(f)
                    if 'aggregations' not in line and ')' in line:
                        print('Also need to update: ', next_line)
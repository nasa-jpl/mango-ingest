import os

import fileinput

# with fileinput.input(files=('file1.txt',), inplace=True) as f:
#     for line in f:
#         print(line.replace('old_text', 'new_text'), end='')



def get_files(dir_path):
    """Lists all files in the specified directory."""
    file_paths = []
    for filename in os.listdir(dir_path):
        file_path = os.path.join(dir_path, filename)
        if os.path.isfile(file_path):
            file_paths.append(os.path.abspath(file_path))
    return file_paths

files = get_files('primary/')
print("QQQQQQ ", files)
for fname in files:
#   fname = files[1]
    with fileinput.input(files=(fname,), inplace=True) as f:
        for line in f:
            if 'np.double' in line:
                if ')' in line:
                    if 'aggregations' not in line:
                       #pass
                        print(line.replace("),", ", aggregations=['min', 'max']),")[:-1])
                    else:
                        print(line[:-1])
                else:
                    next_line = next(f)
                    if 'aggregations' in next_line:
                        print(line[:-1])
                        print(next_line[:-1])
                    else:
                        print(line[:-1])
                        print(next_line.replace("),", ", aggregations=['min', 'max']),")[:-1])
                #         print('Also need to update: ', next_line)
            else:
                print(line[:-1])



    # with open(fname, "r") as f:
    #     for line in f:
    #             if ')' in line:
    #                 if 'aggregations' not in line:
    #                    #pass
    #                    print("Need to update: ", line)
    #                 else:
    #                     print(line)
    #             # else:
    #             #     next_line = next(f)
    #             #     if 'aggregations' not in line and ')' in line:
    #             #         print('Also need to update: ', next_line)
    #         else:
    #             print(line

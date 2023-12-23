import os

out_dir = '/nomount/masschange/test/input_data/gracefo_1A_2023-06-01_RL04.ascii.noLRI-truncated/'
in_dir = '/nomount/masschange/test/input_data/gracefo_1A_2023-06-01_RL04.ascii.noLRI/'

for fn in os.listdir(in_dir):
    try:
        in_fp = os.path.join(in_dir, fn)
        out_fp = os.path.join(out_dir, fn)

        if not os.path.splitext(fn)[-1] in {'.txt', '.rpt'}:
            print(f'skipping file: {fn}')
            continue

        print(fn)

        with open(in_fp) as infile, open(out_fp, 'w+') as outfile:
            header_consumed = False
            records_count = 0

            for l in infile.readlines():
                outfile.write(l)


                if l.startswith('# End of YAML header'):
                    header_consumed = True

                if header_consumed:
                    records_count += 1

                if records_count > 100:
                    break
    except Exception as err:
        print(f'Error processing {fn}: {err}')

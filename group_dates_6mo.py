import datetime
import csv
from dateutil import relativedelta

f = open('prov_list.csv', 'r')
lines = f.readlines()
f.close()

results = []

for line in lines:
    line = line.split(",")
    
    result = []
    result.append(line[0])

    if len(line[1]) > 4:
        result.append(line[1][:4])
        if 1 <= int(line[1][-2:]) <= 6:
            result.append('A')
        elif 7 <= int(line[1][-2:]) <= 12:
            result.append('B')
    else:
        continue
    result.append(int(line[2]))

    if len(line[3]) > 4:
        year_month = line[3][:6]
        result.append(year_month[:4])
        if 1 <= int(year_month[-2:]) <= 6:
            result.append('A')
        elif 7 <= int(year_month[-2:]) <= 12:
            result.append('B')
    results.append(result)

grouped_result = []
cur_id = '000'
cur_group = 'A'
cur_year = '2008'
first_run = True
for result in results:
    if result[0] == cur_id:
        if result[1] == cur_year:
            if result[2] == cur_group:
                row[3] += int(result[3])
    else:
        if not first_run:
            grouped_result.append(row)
        first_run = False
        cur_id = result[0]
        cur_year = result[1]
        cur_group = result[2]
        row = result
    
final_results = []
for result in grouped_result:
    row = []
    row.append(result[0])
    row.append(result[1])
    row.append(result[2])
    row.append(result[3])
    if len(result) == 6:
        if (result[4] == result[1] and result[5] == result[2]) or (result[4] == result[1] and result[5] == 'B' and
        result[2] == 'A') or (int(result[4]) - int(result[1]) == 1 and result[5] == 'A' and result[2] == 'B'):
            row.append(1)
        else:
            row.append(0)
    else:
        row.append(0)
    final_results.append(row)

with open('provs_grouped_6mo.txt', 'w') as out_file:
    wr = csv.writer(out_file, lineterminator = '\n')
    wr.writerows(final_results)













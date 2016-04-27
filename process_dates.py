import datetime
import csv
from dateutil import relativedelta

f = open('prov_list.csv', 'r')
lines = f.readlines()
f.close()

results = []
for line in lines:
    line = line.split(",")
    entry = []
    entry.append(line[0]) #append patient
    entry.append(line[1])
    entry.append(line[2])
    if  len(line[3]) > 4 and len(line[1]) > 4:
        cur_date = datetime.date(int(float(line[1][:4])), int(float(line[1][-2:])), 1)
        year_month = line[3][:6]
        death_date = datetime.date(int(float(year_month[:4])), int(float(year_month[-2:])), 1)
        difference = relativedelta.relativedelta(death_date, cur_date)
        months = difference.months
        if months <= 6:
            entry.append(1)
        else:
            entry.append(0)
    else:
        entry.append(0)
    results.append(entry)

unique_results = [list(x) for x in set(tuple(x) for x in results)]

results = unique_results
with open('prov_dates.txt', 'w') as out_file:
    wr = csv.writer(out_file, lineterminator = '\n')
    wr.writerows(results)

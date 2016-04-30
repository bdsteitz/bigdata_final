import csv

f = open('icd_outcome.out', 'r')
lines = f.readlines()
f.close()

results = []
for line in lines:
    line = line.split(",")
    if line[1] == '':
        continue
    result = [0] * 23
    result[0] = line[0][1:]
    result[1] = line[1]
    result[2] = line[2]
    result[22] = line[13][:1]
    for i in range(3, 12):
        if line[i] == '' or line[i] == 'OTH':
            continue
        elif line[i][:1] == 'V' or line[i][:1] == 'E':
            result[21] += 1
        elif 1 <= int(line[i]) <= 139:
            result[3] += 1
        elif 140 <= int(line[i]) <= 239:
            result[4] += 1
        elif 240 <= int(line[i]) <= 279:
            result[5] += 1
        elif 280 <= int(line[i]) <= 289:
            result[6] += 1
        elif 290 <= int(line[i]) <= 319:
            result[7] += 1
        elif 320 <= int(line[i]) <= 359:
            result[8] += 1
        elif 360 <= int(line[i]) <= 389:
            result[9] += 1
        elif 390 <= int(line[i]) <= 459:
            result[10] += 1
        elif 460 <= int(line[i]) <= 519:
            result[11] += 1
        elif 520 <= int(line[i]) <= 579:
            result[12] += 1
        elif 580 <= int(line[i]) <= 629:
            result[13] += 1
        elif 630 <= int(line[i])<= 679:
            result[14] += 1
        elif 680 <= int(line[i]) <= 709:
            result[15] += 1
        elif 710 <= int(line[i]) <= 739:
            result[16] += 1
        elif 740 <= int(line[i]) <= 759:
            result[17] += 1
        elif 760 <= int(line[i]) <= 779:
            result[18] += 1
        elif 780 <= int(line[i]) <= 799:
            result[19] += 1
        elif 800 <= int(line[i]) <= 999:
            result[20] += 1
    results.append(result)

final_results = []
cur_id = '000'
first_run = True
print "begin reduction"
for result in results:
    if result[0] == cur_id:
        for i in range(3, 21):
            row[i] += result[i]
    else:
        if not first_run:
            final_results.append(row)
        first_run = False
        cur_id = result[0]
        row = result

add_total = []
for result in final_results:
    outcome = result[-1]
    row = result
    row_sum = 0
    for i in range(3, 21):
        row_sum += row[i]
    row[-1] = row_sum
    row.append(outcome)
    add_total.append(row)

with open('outpt_icd_group.txt', 'w') as out_file:
    wr = csv.writer(out_file, lineterminator = '\n')
    wr.writerows(add_total)
    








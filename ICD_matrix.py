import scipy
import numpy
from collections import defaultdict

f = open('ICD_testfile.out', 'r')
lines = f.readlines()
f.close()

ICD_list = []
for line in lines:
    line = line.strip()
    line = line[1:-1]
    line = line.split(',')
    ICD_list.append(line[2:])

mat = scipy.zeros((len(ICD_list), 21))
for i in range(0, len(ICD_list)):
    for j in range(0, len(ICD_list[0]) - 1):
        mat[i][20] = ICD_list[i][-1]
        if ICD_list[i][j] != '':
            if ICD_list[i][j][:1] == 'V' or 'E':
                mat[i][18] += 1
                mat[i][19] += 1
            elif 1 <= (ICD_list[i][j]) <= 139:
                mat[i][0] += 1
                mat[i][19] += 1
            elif 140 <= int(ICD_list[i][j]) <= 239:
                mat[i][1] += 1
                mat[i][19] += 1
            elif 240 <= int(ICD_list[i][j]) <= 279:
                mat[i][2] += 1
                mat[i][19] += 1
            elif 280 <= int(ICD_list[i][j]) <= 289:
                mat[i][3] += 1
                mat[i][19] += 1
            elif 290 <= int(ICD_list[i][j]) <= 319:
                mat[i][4] += 1
                mat[i][19] += 1
            elif 320 <=  int(ICD_list[i][j]) <= 359:
                mat[i][5] += 1
                mat[i][19] += 1
            elif 360 <= int(ICD_list[i][j]) <= 389:
                mat[i][6] += 1
                mat[i][19] += 1
            elif 390 <= int(ICD_list[i][j]) <= 459:
                mat[i][7] += 1
                mat[i][19] += 1
            elif 460 <= int(ICD_list[i][j]) <= 519:
                mat[i][8] += 1
                mat[i][19] += 1
            elif 520 <= int(ICD_list[i][j]) <= 579:
                mat[i][9] += 1
                mat[i][19] += 1
            elif 580 <= int(ICD_list[i][j]) <= 629:
                mat[i][10] += 1
                mat[i][19] += 1
            elif 630 <= int(ICD_list[i][j]) <= 679:
                mat[i][11] += 1
                mat[i][19] += 1
            elif 680 <= int(ICD_list[i][j]) <= 709:
                mat[i][12] += 1
                mat[i][19] += 1
            elif 710 <= int(ICD_list[i][j]) <= 739:
                mat[i][13] += 1
                mat[i][19] += 1
            elif 740 <= int(ICD_list[i][j]) <= 759:
                mat[i][14] += 1
                mat[i][19] += 1
            elif 760 <= int(ICD_list[i][j]) <= 779:
                mat[i][15] += 1
                mat[i][19] += 1
            elif 780 <= int(ICD_list[i][j]) <= 799:
                mat[i][16] += 1
                mat[i][19] += 1
            elif 800 <= int(ICD_list[i][j]) <= 999:
                mat[i][17] += 1
                mat[i][19] += 1
print mat[0:20]


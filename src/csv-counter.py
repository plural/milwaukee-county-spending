import csv

ifile = open('get-spending-by-division.log', 'rb')
reader = csv.reader(ifile)

for row in reader:
  num_cols = 0
  for col in row:
    num_cols += 1
  print num_cols

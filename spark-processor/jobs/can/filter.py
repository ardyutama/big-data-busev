# Copyright (c) 2022 Yandi Banyu Karima Waly
# 
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

import os
import csv
import cantools

DBC = cantools.db.load_file('j1939.dbc')
DBC.add_dbc_file('iso.dbc')

UNIQUE_CSV = []
MSG_HIT = {}
for msg in DBC.messages:
    MSG_HIT[msg.frame_id] = False

DATA_FILE = os.listdir('Data')

filteredCSV = open('filtered.csv', 'w')
filteredWriter = csv.writer(filteredCSV, 'unix')
filteredWriter.writerow(["Bus","No","Time (abs)","State","ID (hex)","DLC","Data (hex)","ASCII"])

for filename in DATA_FILE:
    csvFile = open("Data/{}".format(filename), 'r')
    csvReader = csv.reader(csvFile)
    for line in csvReader:
        if line[4] != "ID (hex)":
            ID_HEX = int(line[4].replace(' ', ''), 16)
            DATA_HEX = int(line[6].replace(' ', ''), 16)

            PRIORITY = ID_HEX & (0b00011100 << 24)
            RESERVED = ID_HEX & (0b00000010 << 24)
            DATA_PAGE = ID_HEX & (0b00000001 << 24)
            PDU_FORMAT = ID_HEX & (0b11111111 << 16)
            PDU_SPECIFIC = ID_HEX & (0b11111111 << 8)
            SOURCE_ADDRESS = ID_HEX & (0b11111111 << 0)

            PGN = RESERVED | DATA_PAGE | PDU_FORMAT | PDU_SPECIFIC
            DBC_ID = PRIORITY | PGN | 0xFE

            if DBC_ID not in UNIQUE_CSV:
                UNIQUE_CSV.append(DBC_ID)

            try:
                DBC.get_message_by_frame_id(DBC_ID)
                filteredWriter.writerow(line)
                MSG_HIT[DBC_ID] = True
            except KeyError as e:
                pass

hitCounter = 0
print("Only found this data:")
for id in MSG_HIT:
    if MSG_HIT[id] is False:
        # print(DBC.get_message_by_frame_id(id))
        pass
    else:
        hitCounter = hitCounter+1
        print(DBC.get_message_by_frame_id(id))
print(f"Got {hitCounter} message from {len(DBC.messages)} in DBC")
print(f"Total unique data in data dump is {len(UNIQUE_CSV)}")
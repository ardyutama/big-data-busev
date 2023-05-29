# Copyright (c) 2022 Yandi Banyu Karima Waly
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

import csv
import json
import cantools
import math

DBC = cantools.db.load_file('j1939.dbc')
DBC.add_dbc_file('iso.dbc')

DATA_FILE = open('filtered.csv', 'r')
csvFile = csv.reader(DATA_FILE)

DECODED_MESSAGE = []
LONG_MESSAGE = {}

for line in csvFile:
    if len(line) > 0 and line[4] != "ID (hex)":
        ID_HEX = int(line[4].replace(' ', ''), 16)
        DATA_LEN = int(line[5].replace(' ', ''))
        DATA_HEX_STR = line[6].replace(' ', '')

        PRIORITY = ID_HEX & (0b00011100 << 24)
        RESERVED = ID_HEX & (0b00000010 << 24)
        DATA_PAGE = ID_HEX & (0b00000001 << 24)
        PDU_FORMAT = ID_HEX & (0b11111111 << 16)
        PDU_SPECIFIC = ID_HEX & (0b11111111 << 8)
        SOURCE_ADDRESS = ID_HEX & (0b11111111 << 0)

        PGN = RESERVED | DATA_PAGE | PDU_FORMAT | PDU_SPECIFIC
        DBC_ID = PRIORITY | PGN | 0xFE

        currMsg = DBC.get_message_by_frame_id(DBC_ID)
        try:
            DECODED_MESSAGE.append(currMsg.decode(bytes.fromhex(DATA_HEX_STR)))
        except:
            if currMsg.frame_id in LONG_MESSAGE.keys():
                LONG_MESSAGE[currMsg.frame_id] = f"{LONG_MESSAGE[currMsg.frame_id]}{DATA_HEX_STR}"
                try:
                    DECODED_MESSAGE.append(currMsg.decode(bytes.fromhex(LONG_MESSAGE[currMsg.frame_id])))
                    del LONG_MESSAGE[currMsg.frame_id]
                except:
                    pass
            else:
                LONG_MESSAGE[currMsg.frame_id] = DATA_HEX_STR

for msg in DECODED_MESSAGE:
    print(msg)
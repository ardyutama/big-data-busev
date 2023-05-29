import cantools
import csv
import json

file = open("DA1-12-13 (2).csv", mode="r")

csvdata = csv.DictReader(file)

db = cantools.database.load_file('./j1939.dbc')

tempdict = {}

result = []

def parsing_asd(idhex, datahex):
    idhex = idhex[0:len(idhex)-2]
    idhex = idhex + "FE"
    idhex = idhex.strip()
    
    outdata = None
    try:
        outdata = decodeee(idhex, datahex)
    except:
        try:
            newhex = idhex[0:len(idhex) - 4] + "FE" + idhex[len(idhex) - 2:len(idhex)]
            outdata = decodeee(newhex, datahex)
        except:
            pass
            # print(idhex)
    if(outdata):
        for key in outdata.keys():
            if not isinstance(outdata[key], int) and not isinstance(outdata[key], float) and not isinstance(outdata[key], str):
                outdata[key] = str(outdata[key])
        result.append(outdata)  

def decodeee(idhex, datahex):
    ih = int("0x" + idhex, 16)
    curmsg = db.get_message_by_frame_id(ih)
    
    if curmsg.length > 8:
        if idhex in tempdict:
            tempdict[idhex] += datahex
        else:
            tempdict[idhex] = datahex
            
        if len(tempdict[idhex]) / 2.875 >= curmsg.length:
            outdata = db.decode_message(ih, bytes.fromhex(tempdict[idhex]))
            outdata["MessageName"] = curmsg.name
            tempdict.pop(idhex)
            return outdata
    else:
        outdata = db.decode_message(ih, bytes.fromhex(datahex))
        outdata["MessageName"] = curmsg.name
        return outdata
    
    
for lines in csvdata:
    idhex = lines["ID (hex)"]
    datahex = lines["Data (hex)"]
    
    parsing_asd(idhex, datahex)
    

print("done")
print(len(result))
json.dump(result, fp=open("result.json", mode="w"), indent=4)


# print(result[0]["EngTorqueMode"])
# i = 0
# for a in result[i].keys():
#     if not isinstance(result[i][a], int) and not isinstance(result[i][a], float):
#         result[i][a] = str(result[i][a])
#         # print(a, ":", result[i][a])
#         # print(a, result[0][a])
#     print(type(result[i][a]))

# for x in result:
#     print(x)
#     print("\n")
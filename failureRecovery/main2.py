import json
import pickle
from functions.utils import bin_to_json

# # def json_to_bin(json_file, bin_file):
# #     with open(json_file, 'r') as jfile:
# #         data = json.load(jfile)
    
# #     with open(bin_file, 'wb') as bfile:
# #         pickle.dump(data, bfile)

# # def bin_to_json(bin_file, json_file):
# #     with open(bin_file, 'rb') as bfile:
# #         data = pickle.load(bfile)
    
# #     with open(json_file, 'w') as jfile:
# #         json.dump(data, jfile, indent=4)

bin_to_json('write_ahead_log.bin', 'write_ahead_log2.json')

# # json_to_bin('write_ahead_log.json', 'data.bin')
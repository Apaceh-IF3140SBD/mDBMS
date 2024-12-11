import json
import pickle
import os

def json_to_bin(json_file, bin_file):
    with open(json_file, 'r') as jfile:
        data = json.load(jfile)
    
    with open(bin_file, 'wb') as bfile:
        pickle.dump(data, bfile)

def bin_to_json(bin_file, json_file):
    try:
        # Check if the binary file exists
        if not os.path.exists(bin_file):
            # If the file doesn't exist, create an empty JSON (no content)
            open(json_file, 'w').close()  # Creates an empty file
            return None

        # Attempt to open and read the binary file
        with open(bin_file, 'rb') as bfile:
            # Read the content to determine if the file is empty
            content = bfile.read()
            if not content:
                # If the binary file is empty, create an empty JSON (no content)
                open(json_file, 'w').close()  # Creates an empty file
                return None

            # Deserialize the data if the file is not empty
            data = pickle.loads(content)

        # Write the deserialized data to the JSON file
        with open(json_file, 'w') as jfile:
            json.dump(data, jfile, indent=4)

    except EOFError:
        # Handle case where the binary file is empty or corrupted
        open(json_file, 'w').close()  # Creates an empty file
        return None

    except Exception as e:
        # Handle other unexpected exceptions
        print(f"An error occurred: {e}")
        open(json_file, 'w').close()  # Creates an empty file
        return None

    return None

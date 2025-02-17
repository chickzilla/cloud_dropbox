import requests
import base64
import os
import json

API_GATEWAY_URL = "https://fvomwqhcdi.execute-api.ap-southeast-1.amazonaws.com/default/cloud-dropbox"

def put_file(cmd):
    if len(cmd) < 2:
        print("Error: Please provide a file path.")
        return
    
    # read file in directory
    file_path = cmd[1]
    if not os.path.isfile(file_path):
        print("Error: File does not exist.")
        return

    file_name = os.path.basename(file_path)

    with open(file_path, "rb") as file:
        file_content = base64.b64encode(file.read()).decode("utf-8")

    response = requests.post(
        API_GATEWAY_URL,
        headers={"Content-Type": "application/json"},
        json={"action": "put", "file_name": file_name, "file_content": file_content}
    )

    try:
        response_data = response.json()
        if response.status_code == 200 :
            print("OK")
        else:
            print(f"Error: {response_data.get('message')}")
    except json.JSONDecodeError:
        print("Error: Invalid server response")
        
# ========================================================================================================
def view_files():
    response = requests.get(
        API_GATEWAY_URL,
        headers={"Content-Type": "application/json"},
        json={"action": "view"}
    )

    try:
        response_data = response.json()
        if response.status_code == 200:
            files = response_data.get("files", [])
            # no files
            if not files:
                print("No files found.")
                return
            
            # loop and print metadata
            for file in files:
                print(f"{file.get('file_name')} {file.get('size')} bytes {file.get('last_modified')} {file.get('owner')}")
        else:
            print(f"Error: {response_data.get('message')}")
    except json.JSONDecodeError:
        print("Error: Invalid server response")

# ========================================================================================================
def get_file(cmd):
    if len(cmd) < 2:
        print("Error: Please provide a file name.")
        return
    
    file_name = cmd[1]

    response = requests.get(
        API_GATEWAY_URL,
        headers={"Content-Type": "application/json"},
        json={"action": "get", "file_name": file_name},
        stream=True 
    )

    if response.status_code == 200:
        with open(file_name, "wb") as file:
            for chunk in response.iter_content(chunk_size=8192): 
                file.write(chunk)

        print(f"Download complete: {file_name}")
    else:
        try:
            error_message = response.json().get("message")
        except requests.JSONDecodeError:
            error_message = "Invalid server response"
        print(f"Error: {error_message}")

# ========================================================================================================
def main():
    print("Welcome to myDropbox Application")
    print("======================================================")
    print("Please input command ( put filename, get filename, view ).")
    print("If you want to quit the program just type quit.")
    print("======================================================")
    while True:
        command = input().strip().split()

        if not command:
            continue

        cmd = command[0].lower()

        match cmd:
            case "put":
                put_file(command)
            case "view":
                view_files()
            case "get":
                get_file(command)
            case "quit":
                print("======================================================")
                break
            case _:
                print("Invalid command.")

if __name__ == "__main__":
    main()

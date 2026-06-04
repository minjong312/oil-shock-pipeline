import requests
import sys

ZEPPELIN_HOST = "127.0.0.1"
ZEPPELIN_PORT = "9995"
NOTE_ID = "2MSYV1S3Q"

URL = f"http://{ZEPPELIN_HOST}:{ZEPPELIN_PORT}/api/notebook/job/{NOTE_ID}"

def trigger_refresh():
    print(f"[*] Triggering Zeppelin Dashboard refresh... (Note ID: {NOTE_ID})")
    try:
        response = requests.post(URL)
        if response.status_code == 200:
            print("[+] Success: Zeppelin notebook execution started.")
        else:
            print(f"[-] Failed: Zeppelin API error (Status code: {response.status_code})")
            sys.exit(1)
    except requests.exceptions.ConnectionError:
        print("[-] Error: Could not connect to Zeppelin server.")
        sys.exit(1)

if __name__ == "__main__":
    trigger_refresh()

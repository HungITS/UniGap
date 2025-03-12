import json

def save_failed_ips(failed_ips):
    with open('failed_ips.json', 'a') as f:
        json.dump(failed_ips, f)
        f.write("\n")
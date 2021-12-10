import json
with open('config.txt') as f:
    data = f.read()
    #print(data)
    y = json.loads(data)
    table =   y["table_name"]
    mail_addr=y["recipient_email"]
    mail_addr = y["recipient_email"]
    #print(table)

import json
import socket
import sys
import traceback


# Read Message Template
msg = json.load(open("Message.json"))

# Initialize
sender = "Controller"
target = sys.argv[1]
port = 5555

# Request
msg['sender_name'] = sender
msg['term'] = None
msg['Key'] = sys.argv[2]
msg['Value'] = sys.argv[3]
msg['request'] = "STORE"

print(f"Request Created:{msg}, TARGET:{target}")

# Socket Creation and Binding
skt = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
skt.bind((sender, port))
skt.settimeout(10)
try:
    # Encoding and sending the message
    skt.sendto(json.dumps(msg).encode('utf-8'), (target, port))
    resp_msg, add = skt.recvfrom(1024)
    decoded_resp = json.loads(resp_msg.decode('utf-8'))
    print('response:', decoded_resp)
    if decoded_resp['request'] == 'LEADER_INFO':
        print(f"Request Created:{msg}, TARGET:{decoded_resp['value']}")
        skt.sendto(json.dumps(msg).encode('utf-8'), (decoded_resp['value'], port))

except socket.timeout:
    #  socket.gaierror: [Errno -3] would be thrown if target IP container does not exist or exits, write your listener
    # print(f"ERROR WHILE SENDING REQUEST ACROSS : {traceback.format_exc()}")
    pass
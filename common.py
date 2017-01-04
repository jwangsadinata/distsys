import common2
import hashlib
import json
import socket
import struct
import time

MAX_MESSAGE_SIZE = 8192
HASH_MAX = 100000

###### PHASE 3 RELATED FUNCTIONS ######

# Hash function using the hashlib library
def hash_key(d):
    sha1 = hashlib.sha1(d)
    return int(sha1.hexdigest(), 16) % HASH_MAX

# Sorting view by the hash functions
def sort_view_by_hash(view):
    ids_hash = [ (x[0], hash_key(x[0]), x[1]) for x in view ]
    sorted_ids_hash = sorted(ids_hash, key = lambda x : x[1])
    return sorted_ids_hash

# Idempotent bucket allocator
def bucket_allocator(key, view):
    buckets = []
    bucket_count = 0
    
    if len(view) <= common2.REPLICATION_COUNT:
        buckets = [ (x[0],x[1]) for x in view ]
    else:
        sorted_ids_hash = sort_view_by_hash(view)

        # Going through view until getting replication_count bins
        for s in sorted_ids_hash:
            if s[1] >= hash_key(key) and bucket_count < common2.REPLICATION_COUNT:
                buckets.append((s[0],s[2]))
                bucket_count += 1

        # wraps around the circular distributed hashtable
        index = 0
        while bucket_count < common2.REPLICATION_COUNT:
            if bucket_count < len(sorted_ids_hash):
                buckets.append((sorted_ids_hash[index][0], sorted_ids_hash[index][2]))
                bucket_count += 1
                index += 1

    return buckets

# Parameters for server connection
def host_port(servers):
    server_id = servers[0]
    address = servers[1].split(":")
    hp = {"addr": address[0], "port": int(address[1]), "id" : server_id}
    return hp   

###### SOCKET STUFF ######

# Encode and send a message on an open socket
def send(sock, message):
    message = json.dumps(message).encode()

    nlen = len(message)
    if nlen >= MAX_MESSAGE_SIZE:
        return {"error": "maxmimum message size exceeded"}
    slen = struct.pack("!i", nlen)

    if sock.sendall(slen) is not None:
        return {"error", "incomplete message"}
    if sock.sendall(message) is not None:
        return {"error", "incompletely sent message"}

    return {}

# Expect a message on an open socket
def receive(sock):
    nlen = sock.recv(4, socket.MSG_WAITALL)
    if not nlen:
        return {"error": "can't receive"}

    slen = (struct.unpack("!i", nlen)[0])
    if slen >= MAX_MESSAGE_SIZE:
        return {"error": "maximum response size exceeded"}
    response = sock.recv(slen, socket.MSG_WAITALL)

    return json.loads(response.decode())

# Encapsulates the send/receive functionality of an RPC client
# Parameters
#   host, port - host and port to connect to
#   message - arbitrary Python object to be sent as message
# Return value
#   Response received from server
#   In case of error, returns a dict containing an "error" key
def send_receive(host, port, message):
    sock = None
    try:
        sock = socket.create_connection((host, port), 5)
        if not sock:
            return {"error": "can't connect to %s:%s" % (host, port)}

        send_result = send(sock, message)
        if "error" in send_result:
            return send_result

        receive_result = receive(sock)
        return receive_result

    except ValueError as e:
        return {"error": "json encoding error %s" % e}
    except socket.error as e:
        return {"error": "can't connect to %s:%s because %s" % (host, port, e)}
    finally:
        if sock is not None:
            sock.close()

# A simple RPC server
# Parameters
#   port - port number to listen on for all interfaces
#   handler - function to handle respones, documented below
#   timeout - if not None, after how many seconds to invoke timeout handler
# Return value
#   in case of error, returns a dict with "error" key
#   otherwise, function does not return until timeout handler returns "abort"
#
# the handler function is invoked by the server in response
# handler is passed a dict containing a "cmd" key indicating the event
# the following are possible values of the "cmd" key:
#    init: the port has been bound, please perform server initializiation
#    timeout: timeout occurred
#    anything else: RPC command received
# the return value of the handler function is sent as an RPC response
def listen(port, handler, timeout=None):
    bindsock = None
    try:
        bindsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        bindsock.bind(('', port))
        bindsock.listen(50)
        if timeout:
            bindsock.settimeout(timeout)

        if "abort" in handler({"cmd":"init", "port": port}, None):
            return {"error": "listen: abort in init"}

        sock = None
        addr = None

        while True:
            try:
                sock, (addr, accepted_port) = bindsock.accept()

                msg = receive(sock)
                if "error" in msg:
                    print "listen: when receiving, %s" % msg["error"]
                    continue

                try:
                    response = handler(msg, addr)
                    if "abort" in response:
                        print "listen: abort"
                        return response
                        break
                except Exception as e:
                    print "listen: handler error: %s" % e
                    continue

                res = send(sock, response)
                if "error" in res:
                    print "listen: when sending, %s" % res["error"]
                    continue
            except socket.timeout:
                if "abort" in handler({"cmd":"timeout"}, None):
                    return {"error": "listen: abort in timeout"}
            except ValueError as e:
                print "listen: json encoding error %s" % e
            except socket.error as e:
                print "listen: socket error %s" % e
            finally:
                if sock is not None:
                    sock.close()
    except socket.error as e:
        return {"error": "can't bind %s" % e}
    finally:
        if bindsock is not None:
            bindsock.close()

def default_viewleader_ports(n=3): 
    hostname = socket.gethostname()
    servers = [ "%s:%s" % (hostname , port) for port in range(common2.VIEW_LEADER_LOW ,min(common2.VIEW_LEADER_HIGH , common2.VIEW_LEADER_LOW+n))] 
    return ",".join(servers)

def pretty_print_log(a):
    if a['msg']['cmd'] == 'lock_get':
        return "%s. %s requests %s from %s" % (str(a['msg']['proposal_number']), a['msg']['requester'], a['msg']['lockname'], socket.gethostname())
    elif a['msg']['cmd'] == 'lock_release':
        return "%s. %s releases %s from %s" % (str(a['msg']['proposal_number']), a['msg']['requester'], a['msg']['lockname'], socket.gethostname())
    elif a['msg']['cmd'] == 'heartbeat':
        return "%s. Heartbeat of %s at time %s" % (str(a['msg']['proposal_number']), a['msg']['server_id'], a['msg']['timestamp'])
    else:
        return "%s. Some event happen here" % (str(a['msg']['proposal_number']))

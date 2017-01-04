#!/usr/bin/python
'''
Server Implementation - Jason Wangsadinata
'''

import argparse
import common
import common2
import threading
import time
import uuid
import signal
import socket
import os

#####################
###### GLOBALS ######
#####################

# Stores global configuration variables
config = {"epoch_number": None,
          "host": None,
          "port": None,
          "server_id": str(uuid.uuid4()),
          "last_heartbeat": None,
          "rebalance_status": False,
          "view" : None }

# Stores shared values for get and set commands
store = {}

# Store for pending commits
pending = {}

# Lock for rebalancing
thread_lock = threading.RLock()

#################################
###### RPC IMPLEMENTATIONS ######
#################################

# Init function - nop
def init(msg, addr):
    config['host'] = socket.gethostname()
    config["port"] = msg["port"]
    send_heartbeat()
    return {}

# set command sets a key in the value store
def set_val(msg, addr):
    key = msg["key"]
    val = msg["val"]
    store[key] = {"val": val}
    print "Setting key %s to %s in local store" % (key, val)
    return {"status": "ok"}

# fetches a key in the value store
def get_val(msg, addr):
    key = msg["key"]
    if key in store:
        print "Querying stored value of %s" % key
        return {"status": "ok", "value": store[key]["val"],}
    else:
        print "Stored value of key %s not found" % key
        return {"status": "not_found"}

# Returns all keys in the value store
def query_all_keys(msg, addr):
    print "Returning all keys"
    keyvers = [ key for key in store.keys() ]
    return {"result": keyvers}

# Print a message in response to print command
def print_something(msg, addr):
    print "Printing %s" % " ".join(msg["text"])
    return {"status": "ok"}

# accept timed out - nop
def tick(msg, addr):
    return {}

#############################################
###### TWO PHASE COMMIT IMPLEMENTATION ######
#############################################

# Vote request
def vote_request(msg, addr):
    key = msg["key"]

    # Vote No if there is a pending commit
    # also when the server is currently rebalancing
    if key in pending:
        print "Vote for 2PC: No"
        return {"vote": False, "epoch": config["epoch_number"], "id": config["server_id"]}
    else:
        pending[key] = addr
        print "Vote for 2PC: Yes" 
        return {"vote": True, "epoch": config["epoch_number"], "id": config["server_id"]}

# Global commit -> in this case it will be as a result of setr, and
# therefore it will call the set RPC
# then remove it from pending store
def two_phase_global_commit(msg, addr):
    key = msg['key']

    # Invoke the set command for the distributed RPC
    msg['cmd'] = 'set' 
    handler(msg, addr)

    # Remove things from the pending 2PC
    pending.pop(key, None)

    print "Commit successful"
    return {"status": "commit success"}

# Global abort just remove things from pending store
# not executing anything for the server
def two_phase_global_abort(msg, addr):
    key = msg["key"]

    # Remove things from the pending 2PC
    pending.pop(key, None)

    print "Commit aborted"
    return {"status": "commit aborted"}

#######################################
###### HEARTBEAT IMPLEMENTATIONS ######
#######################################

# Helper function to format heartbeat message to the view leader
def send_heartbeat():
    if config["port"] is None:
        return {}
    config["last_heartbeat"] = time.time()
    vl_list = config["viewleader"].split(',')
    vl_list.sort()
    vl_list.reverse()

    # Initialize connections to the view leader
    for viewleader in vl_list:
        hp = viewleader.split(':')
        host = str(hp[0])
        port = int(hp[1])
        request = {
            "cmd": "heartbeat",
            "server_id": config['server_id'],
            "host": host,
            "port": port,
            "rebalance_status": config['rebalance_status']
        }
        response = common.send_receive(host, port, request)

        if 'heartbeat' in response.keys():
            if response['heartbeat'] == 'invalid':
                print "Viewleader: Heartbeat is rejected"
                #os.kill(os.getpid(), signal.SIGINT)

        if "error" in response:
            continue

        if 'status' in response.keys():
            if response['status'] == 'failed':
                return "View Cluster has failed!"

        if config['epoch_number'] != response['epoch']:
            reb_thread = threading.Thread(target=rebalance, 
                                            args=(config["view"], response["view"]))
            reb_thread.start()

        if 'view' in response.keys():
            config['view'] = response['view']

        config['epoch_number'] = response['epoch']
        config["last_heartbeat"] = time.time()

        break
    else:
        print "Can't connect to the view leader, trying again" 
    return {}

###########################
######  REBALANCING  ######
###########################

# Sorry this is a make-do rebalancing function that I write in very short time
# I will talk about this more in my README

def rebalance(prev, curr):
    config['rebalance_status'] = True
    print "status : rebalancing"
    with thread_lock:
        for key in store.keys():
            prev_buckets = common.bucket_allocator(key, prev)
            curr_buckets = common.bucket_allocator(key, curr)

            diff = list(set(list(curr_buckets))-set(list(prev_buckets)))

            if config["server_id"] in diff:
                diff.remove((config["server_id"]))

            for server in diff:
                hp = common.host_port(server)
                print "Sending %s to %s" % (key,hp['id'])
                result = common.send_receive(hp['addr'],hp['port'],
                    {"cmd":"set", "key":key, "val" : store[key]["val"] } )

        # Remove keys that should not be on server
        for key in store.keys():
            key_buckets = common.bucket_allocator(key, curr)
            id_list = []
            for server in key_buckets:
                id_list.append(server[0])
            if config["server_id"] not in id_list:
                del store[key]
                print "%s is removed from the local store during rebalancing" % key

        config['rebalance_status'] = False
        print "status : done rebalancing"
        return {"status" : "done rebalancing"}

##########################
###### MAIN PROGRAM ######
##########################

# Server RPC dispatcher invokes appropriate function
def handler(msg, addr):
    cmds = {
        "init": init,
        "set": set_val,
        "get": get_val,
        "print": print_something,
        "query_all_keys": query_all_keys,

        "vote_request": vote_request,
        "two_phase_global_commit": two_phase_global_commit,
        "two_phase_global_abort": two_phase_global_abort,
        
        "timeout": tick, 

        "rebalance": rebalance

    }
    res = cmds[msg["cmd"]](msg, addr)

    # Conditionally send heartbeat
    if time.time() - config["last_heartbeat"] >= 10:
        send_heartbeat()

    return res

# Server entry point
def main():
    # Argparse implementation for optional viewleader arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('--viewleader', nargs='?', default=common.default_viewleader_ports())
    args = parser.parse_args()
    config["viewleader"] = args.viewleader

    for port in range(common2.SERVER_LOW, common2.SERVER_HIGH):
        print "Trying to listen on %s..." % port
        result = common.listen(port, handler, 10)
        print result
    print "Can't listen on any port, giving up"

if __name__ == "__main__":
    main()
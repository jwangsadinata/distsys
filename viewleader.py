#!/usr/bin/python
'''
ViewLeader Implementation - Jason Wangsadinata
'''

import argparse
from collections import deque
import common
import common2
import socket
import sys
import threading
import time

#####################
###### GLOBALS ######
#####################

# Stores global configuration variables
config = {
    'epoch_number' : 0,
    'reader_count' : 0,
    'server_addrs' : None,

    # Global lock constants
    'LOCK_STORE_HELD' : 0,
    'LOCK_STORE_UNHELD' : 1,
    'LOCK_WAIT' : 2,
    'LOCK_ALL' : 3,

    # Rebalancing constants
    'rebalance_status' : False,

    # Local endpoint
    'endpoint' : None,

    # Proposal number
    'proposal_number' : 0
}

# Stores the server that is connected through the heartbeat
connected_servers = {}
dead_servers = {}

# Lock storage
lock_store = {}

# Lock for threads
thread_lock1 = threading.RLock()

# Log
view_log = []

#################################
###### RPC IMPLEMENTATIONS ######
#################################

# Init function - nop
def init(msg, addr):
    return {}

def heartbeat(msg, addr):
    server_id = msg['server_id']
    config['rebalance_status'] = msg['rebalance_status']
    if (server_id not in connected_servers.keys()):        
        view_log.append({"msg":msg,"addr":addr})
        increment_epoch()
    elif server_id in dead_servers.keys():
        return {'heartbeat': 'invalid'}
    connected_servers[server_id] = {'time_stamp' : time.time(), 'addr' : msg['host'], 'port' : msg['port']} 
    server_addrs = [ (server_id, str(connected_servers[server_id]['addr']) + ':' + str(connected_servers[server_id]['port'])) for server_id in connected_servers.keys() ]
    config['server_addrs'] = server_addrs
    view_log.append({"msg":msg,"addr":addr})
    
    return {'heartbeat' : 'ok', 'epoch' : config['epoch_number'], 'view' : config['server_addrs']}

def query_servers(msg, addr):
    print "Returning all servers"
    server_addrs = [ (server_id, str(connected_servers[server_id]['addr']) + ':' + str(connected_servers[server_id]['port'])) for server_id in connected_servers.keys() ]
    return {'epoch': config['epoch_number'], 'result' : server_addrs, 'rebalance_status' : config['rebalance_status']}

def get_lock(msg, addr):
    name = msg['lockname']
    requester = msg['requester']
    requester_type = msg['type']

    view_log.append({"msg":msg,"addr":addr})

    if name not in lock_store.keys():
        if requester_type == 'reader':
            config['reader_count'] += 1
        lock_store[name] = {}
        lock_store[name]['state'] = config['LOCK_STORE_HELD']
        lock_store[name]['requester'] = deque([(requester, requester_type)])
        lock_store[name]['wait_queue'] = deque([])
        return {'status' : 'granted'}

    elif lock_store[name]['state'] == config['LOCK_STORE_UNHELD'] and len(lock_store[name]['wait_queue']) == 0:
        if requester_type == 'reader':
            config['reader_count'] += 1
        lock_store[name]['state'] = config['LOCK_STORE_HELD']
        lock_store[name]['requester'].append((requester, requester_type))
        return {'status' : 'granted'}

    elif lock_store[name]['state'] == config['LOCK_STORE_HELD'] and (requester, requester_type) in lock_store[name]['requester']:
        return {'status' : 'granted'}

    elif lock_store[name]['state'] == config['LOCK_STORE_HELD'] and lock_store[name]['requester'][0][1] == 'reader' and requester_type == 'reader':
        config['reader_count'] += 1
        lock_store[name]['requester'].append((requester,requester_type))
        return {'status' : 'granted', 'reader_count' : config['reader_count']}

    elif lock_store[name]['state'] == config['LOCK_STORE_HELD'] and (requester, requester_type) not in lock_store[name]['requester']:
        if (requester, requester_type) in lock_store[name]['wait_queue']:
            return {'status' : 'retry'}
        else:
            lock_store[name]['wait_queue'].append((requester, requester_type))
            return {'status' : 'retry'}
    else:
        return {'status' : 'retry'}

def release_lock(msg, addr):
    name = msg['lockname']
    requester = msg['requester']
    requester_type = msg['type']

    view_log.append({"msg":msg,"addr":addr})

    if name not in lock_store.keys():
        return {'status' : 'invalid'}

    if lock_store[name]['state'] == config['LOCK_STORE_HELD'] and (requester, requester_type) in lock_store[name]['requester']:
        if requester_type == 'reader' and config['reader_count'] > 1:
            config['reader_count'] -= 1
            lock_store[name]['requester'].remove((requester, requester_type))
            return {'status' : 'released', 'reader_count' : config['reader_count']}

        if len(lock_store[name]['wait_queue']) == 0:
            if requester_type == 'reader':
                config['reader_count'] -= 1
            lock_store[name]['state'] = config['LOCK_STORE_UNHELD']
            lock_store[name]['requester'].remove((requester, requester_type))
            return {'status' : 'released', 'red_count' : config['reader_count']}
        else:
            if requester_type == 'reader':
                config['reader_count'] -= 1
            curr_requester = lock_store[name]['wait_queue'].popleft()
            if curr_requester[1] == 'reader':
                config['reader_count'] += 1
            lock_store[name]['state'] = config['LOCK_STORE_HELD']
            lock_store[name]['requester'].remove((requester, requester_type))
            lock_store[name]['requester'].append(curr_requester)
            return {'status' : 'released', 'reader_count' : config['reader_count']}
    else:
        return {'status' : 'invalid'}

# An RPC to append missing logs
def append_logs(msg,addr):
    backup_log = msg["backup_log"]

    for log in backup_log:
        log['msg']['replay'] = True
        handler(log['msg'], log['addr'])

    return {"status" : "ok", 'epoch' : config['epoch_number'], 'view' : config['server_addrs']}

def receive_prepare(msg,addr):
    proposal_number = int(msg['proposal_number'])

    # Case 1 : Equally up-to-date
    if len(view_log) == proposal_number:
        print "Positive: Logs are up-to-date."
        update_proposal_number(proposal_number)
        return {"status" : "ok", 'epoch' : config['epoch_number'], 'view' : config['server_addrs']}

    # Case 2 : View leader needs backup
    elif len(view_log) < proposal_number:
        print "Positive: Sending backup for missing logs."
        update_proposal_number(proposal_number)
        return {"status" : "backup_vl", "logs" : view_log[proposal_number:], 'epoch' : config['epoch_number']}

    # Case 3 : Backing up view replica
    else:
        print "Positive: View replica is behind."
        update_proposal_number(proposal_number)
        return {"status" : "update_replica", "index": len(view_log), 'epoch' : config['epoch_number'], 'view' : config['server_addrs']}

# Helper function to query logs
def query_logs(msg, addr):
    print "Returning view log"
    logs = [ common.pretty_print_log(log) for log in view_log ]
    return {"result": logs[len(log)-10:]}


#############################################
########## CONSENSUS IMPLEMENTATION #########
#############################################

def update_proposal_number(proposal_number):
    config['proposal_number'] = proposal_number

def prepare_message(proposal_number):
    print "Prepare message; Proposal number: %s" % proposal_number

    vl_list = config["viewleader"].split(",")
    quorum = (len(vl_list) / 2) + 1
    bad_response = 0

    for viewleader in vl_list:
        # prevent sending prepare message to myself
        if config['endpoint'] == str(viewleader):
            continue

        hp = viewleader.split(":")
        host = str(hp[0])
        port = int(hp[1])
        response = common.send_receive(host,port,
            {"cmd":"prepare_message", "proposal_number":proposal_number})

        # Case 1 - Everything is good.
        if response.get('status') == 'ok':
            continue

        # Case 2 - Replica provide backup for viewleader
        if response.get('status') == 'backup_vl':
            backup_log = response.get('backup_log')
            for log in backup_log:
                handler(log['msg'], log['addr'])
                print log['msg']
            continue

        # Case 3 - Replica provide backup for view replica
        if response.get('status') == 'update_replica':
            index = int(response['index'])
            print 'Sending missing logs to view replica'
            response = common.send_receive(host,port,{
                'cmd' : 'append_logs',
                'backup_log':view_log[index:]
            })
            print response
            continue

        else:
            bad_response += 1
            print "Problem with", viewleader

    # No Quorum
    if quorum <= bad_response:
        return {'status':'failed', 'epoch' : config['epoch_number'], 'view' : config['server_addrs']}

    # Quorum reached
    else:
        return {'status':'ok', 'epoch' : config['epoch_number'], 'view' : config['server_addrs']}

def commit_message(msg, proposal_number):    
    if(proposal_number < config['proposal_number']):
        return {'status':'failed', 'epoch' : config['epoch_number'], 'view' : config['server_addrs']}

    vl_list = config['viewleader'].split(',')
    msg['replay'] = True

    for viewleader in vl_list:
        if config['endpoint'] == str(viewleader):
            continue
        print 'Sending commit with proposal number %s to %s' % (proposal_number,viewleader)

        hp = viewleader.split(':')
        addr = str(hp[0])
        port = int(hp[1])
        msg['proposal_number'] = proposal_number
        response = common.send_receive(addr,port,msg)

        if 'error' in response:
            continue

        if response.get('status') == 'ok':
            print response
            continue

    return {'status':'ok', 'epoch' : config['epoch_number'], 'view' : config['server_addrs']}


###############################
###### UTILITY FUNCTIONS ######
###############################

def increment_epoch():
    config['epoch_number'] += 1

# Utility function to simulate a circular list -> used for circular DHT
def circular(index, length):
    return (index - length) if (index >= length) else index

# A function to create a graph in terms of adjacency list representation of the locks and processes
def graphify(lock_store):
    store = {}
    for lock in lock_store.keys():
        store[lock] = set(lock_store[lock]['wait_queue'])
        for requester in store[lock]:
            store[requester] = connected_locks(requester, config['LOCK_WAIT'])
    return store

# A basic check whether there is a cycle in a graph using a modified recursive DFS implementation
def check_cycle(graph):
    path = set()
    visited = set()

    def visit(node):
        if node in visited:
            return False
        visited.add(node)
        path.add(node)
        
        for neighbor in graph.get(node, ()):
            if neighbor in path or visit(neighbor):
                return True
        path.remove(node)
        return False

    return any(visit(node) for node in graph)

# A function to check the locks that the requester is currently holding/waiting for
def connected_locks(requester, param):
    lock_set = set()
    for lock in lock_store.keys():
        if lock_store[lock]['requester'] == requester:
            lock_set.add(lock)
        elif requester in lock_store[lock]['wait_queue']:
            if param == config['LOCK_WAIT']:
                pass
            elif param == config['LOCK_ALL']:
                lock_set.add(lock)
            else:
                pass
    return lock_set

# Helper function to detect deadlocks
def detect_deadlocks():
    while not detect_deadlocks.cancelled:
        global lock_store
        graph = graphify(lock_store)
        if check_cycle(graph):
            print "Deadlock is detected"
            print (str(graph))
        time.sleep(10)

# Helper function to detect failed processes
def detect_failed_processes():
    while not detect_failed_processes.cancelled:
        with thread_lock1:
            for server in connected_servers.keys():
                lock_requester = ':' + str(connected_servers[server]['port'])
                lock_connected_to = connected_locks(lock_requester, config['LOCK_ALL'])
                last_time = connected_servers[server]['time_stamp']
                curr_time = time.time()

                if (curr_time - last_time) > common2.LOCK_LEASE:
                    dead_servers[server] = connected_servers[server]
                    connected_servers.pop(server)

                    for lock in lock_connected_to:
                        # Remove any locks the server currently holding
                        status = release_lock({'lockname' : lock, 'requester' : lock_requester}, '')
                        if status == {'status' : 'released'}:
                            continue
                        else:
                            # Remove the requester from the lock queue as well
                            global lock_store
                            lock_store[lock]['wait_queue'].remove(lock_requester)
                    increment_epoch()
                    print "Server %s has failed" % server
                    print "Releasing %s from %s" % (lock_requester, str(lock_connected_to))
                time.sleep(5)



##########################
###### MAIN PROGRAM ######
##########################

# Viewleader RPC dispatcher invokes appropriate function
def handler(msg, addr):
    cmds = {
        "init": init,
        "heartbeat": heartbeat,
        "query_servers": query_servers,
        "lock_get": get_lock,
        "lock_release": release_lock,
        "prepare_message": receive_prepare,
        "append_logs": append_logs,
        "query_logs": query_logs
    }

    if msg['cmd'] not in ['prepare'] and msg.get('proposal_number') >= config['proposal_number']:
        print 'Incoming RPC with number %s' % msg.get('proposal_number')
        if msg.get('proposal_number') < len(view_log):
            return {'status' : 'failed', 'epoch' : config['epoch_number'], 'view' : config['server_addrs']}

    if msg.get('replay') == True:
        return cmds[msg['cmd']](msg, addr)

    if msg['cmd'] in ['lock_get','lock_release','heartbeat']:
        proposal_number = len(view_log)
        prepare_response = prepare_message(proposal_number)

        if msg['cmd'] == 'heartbeat':
            msg['timestamp'] = time.time()

        if prepare_response.get('status') == 'ok':
            new_proposal_number = len(view_log)
            commit_message(msg, new_proposal_number)
            return cmds[msg['cmd']](msg, addr)

        # Prepare has failed
        else:
            return {'status':'failed', 'epoch' : config['epoch_number'], 'view' : config['server_addrs']}

    else:
        return cmds[msg['cmd']](msg, addr)

# Viewleader entry point
def main():

    parser = argparse.ArgumentParser()
    parser.add_argument('--viewleader', nargs='?', default=common.default_viewleader_ports())
    args = parser.parse_args()
    config['viewleader'] = args.viewleader

    # Setting up a new thread for failed processes
    detect_failed_processes.cancelled = False
    fp_thread = threading.Thread(target = detect_failed_processes)
    fp_thread.daemon = True
    fp_thread.start()

    ### DEADLOCK DETECTION - Commented since I have not fixed the threading issues ###
    # Setting up a new thread for detecting deadlocks
    # detect_deadlocks.cancelled = False
    # deadlock_thread = threading.Thread(target = detect_deadlocks)
    # deadlock_thread.daemon = True
    # deadlock_thread.start()

    try:
        # Listen to connections
        for port in range(common2.VIEW_LEADER_LOW, common2.VIEW_LEADER_HIGH):
            print "Trying to listen on %s..." % port
            host = socket.gethostname()
            endpoint = str(host)+':'+str(port)

            if endpoint not in config['viewleader']:
                print 'This program is not a viewleader/replica. Aborting...'
                return {}

            config['endpoint'] = endpoint

            result = common.listen(port, handler)
            print result
        print "Can't listen on any port, giving up"

    except KeyboardInterrupt:
        detect_failed_processes.cancelled = True
        # detect_deadlocks.cancelled = True

if __name__ == "__main__":
    main()
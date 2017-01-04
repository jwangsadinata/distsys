'''
Client Helper Implementation - Jason Wangsadinata
setr, getr
'''
import common
import common2
import time

config = {'connectedToVL' : False}

def connectVL(args, addr):
    vl_list = addr.split(',')
    vl_list.sort()
    vl_list.reverse()

    for viewleader in vl_list:
        hp = viewleader.split(':')
        host = str(hp[0])
        port = int(hp[1])

        print "Trying to connect to %s:%s..." % (host, port)
        config['connectedToVL'] = False

        while True:
            response = common.send_receive(host, port, args)

            if "error" in response:
                break

            if 'status' in response.keys():
                if response['status'] in ['granted', 'released', 'invalid']:
                    return response
                if response['status'] == 'failed':
                    return "View Cluster has failed!"
                if response['status'] == 'retry':
                    print "Waiting on lock"
                    time.sleep(5)
                    continue

            else:
                config['connectedToVL'] = True
                break

        if config['connectedToVL']:
            return response
            break

        

    else:
        print "Can't connect on any port, giving up"
        return {'status' : 'Error: unable to connect'}

# Helper function to connect to the appropriate RPC
def connect(low_port, high_port, args, addr):
    for port in range(low_port, high_port):
        print "Trying to connect to %s:%s..." % (addr, port)

        if addr == args['viewleader']:
            while True:
                response = common.send_receive(addr, port, args)

                if 'status' in response.keys():
                    if response['status'] != 'retry':
                        break

                else:
                    break
                print response
                time.sleep(5)
        else:
            response = common.send_receive(addr, port, args)

        if "error" in response:
            continue

        return response
    else:
        print "Can't connect on any port, giving up"
        return {'status' : 'Error: unable to connect'}

#####################################################
###### DISTRIBUTED GET AND SET IMPLEMENTATIONS ######
#####################################################

# setr command sets a key in the value store
# uses the distributed hash table
def setr(args):
    key = args['key']
    value = args['val']
    vl_list = args['viewleader'].split(',')
    vl_list.sort()
    vl_list.reverse()
    hp = vl_list[0].split(':')
    host = str(hp[0])
    port = int(hp[1])

    votes = []

    # query_servers to get the current view
    vl_response = connect(port, port + 1,
        {'cmd' : 'query_servers', 'viewleader' : host, 'dist' : True}, host)
    print vl_response

    if 'error' in vl_response.keys():
        return False

    if vl_response['rebalance_status']:
        print "Viewleader: Servers are rebalancing. Aborting setr."
        return True

    epoch = vl_response['epoch']
    view = vl_response['result']

    servers = [ common.host_port(x) for x in common.bucket_allocator(key, view) ]
    if len(servers) == 0:
        print "Error: No servers available. Aborting setr"
        return True

    for server in servers:
        response = common.send_receive(server['addr'], server['port'], 
            {'cmd' : 'vote_request', 'key' : key})
        if "error" in response:
            votes.append(False)
            print "Error: Server %s:%s does not respond" % (server['addr'], server['port'])    
        elif response['id'] != server['id']:
            votes.append(False)
            print "Error: id does not match"
        elif response['epoch'] != epoch:
            votes.append(False)
            print "Error: incorrect epoch"
        else:
            votes.append(response['vote']) 

    if False in votes:
        for server in servers:
            response = common.send_receive(server['addr'], server['port'], 
                {'cmd' : 'two_phase_global_abort', 'key' : key})
        print "Error: Vote turned down. Aborting setr."
        
    else:
        for server in servers:
            response = common.send_receive(server['addr'], server['port'], 
                {'cmd' : 'two_phase_global_commit', 'key' : key, 'val' : value})
            print response
            print 'Commit to', server['id']
        print "Successfully commit setr."
        
    return None

# fetches a key in the value store
# uses the distributed hash table
def getr(args):
    key = args['key']
    vl_list = args['viewleader'].split(',')
    vl_list.sort()
    vl_list.reverse()
    hp = vl_list[0].split(':')
    host = str(hp[0])
    port = int(hp[1])

    # query_servers to get the current view
    vl_response = connect(port, port + 1,
        {'cmd' : 'query_servers', 'viewleader' : host, 'dist' : True}, host)
    print vl_response

    if 'error' in vl_response.keys():
        return False

    if vl_response['rebalance_status']:
        print "Viewleader: Servers are rebalancing. Aborting getr."
        return True

    view = vl_response['result']
    servers = [ common.host_port(x) for x in common.bucket_allocator(key, view) ]

    for server in servers:
        response = common.send_receive(server['addr'], server['port'], 
            {'cmd' : 'get', 'key' : key})

        if 'value' in response:
            print response
            return True

    print "Error: Cannot find keys in any server"
    return True
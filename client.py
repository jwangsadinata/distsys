#!/usr/bin/python
'''
Client Implementation - Jason Wangsadinata
'''

import argparse
import common
import common2
import client_helper

# List of values for server RPC
server_rpc = ['get', 'set', 'print', 'query_all_keys']
viewleader_rpc = ['lock_get', 'lock_release', 'query_servers', 'query_logs']

##########################
###### MAIN PROGRAM ######
##########################

# Client entry point
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--server', default='localhost')
    parser.add_argument('--viewleader', nargs = '?', default=common.default_viewleader_ports())

    subparsers = parser.add_subparsers(dest='cmd')

    parser_set = subparsers.add_parser('set')
    parser_set.add_argument('key', type=str)
    parser_set.add_argument('val', type=str)

    parser_setr = subparsers.add_parser('setr')
    parser_setr.add_argument('key', type=str)
    parser_setr.add_argument('val', type=str)

    parser_get = subparsers.add_parser('get')
    parser_get.add_argument('key', type=str)

    parser_getr = subparsers.add_parser('getr')
    parser_getr.add_argument('key', type=str)

    parser_print = subparsers.add_parser('print')
    parser_print.add_argument('text', nargs="*")

    parser_query_all_keys = subparsers.add_parser('query_all_keys')
    parser_query_servers = subparsers.add_parser('query_servers')
    parser_query_logs = subparsers.add_parser('query_logs')

    parser_lock_get = subparsers.add_parser('lock_get')
    parser_lock_get.add_argument('lockname', type=str)
    parser_lock_get.add_argument('requester', type=str)
    parser_lock_get.add_argument('type', nargs="?", default='writer')

    parser_lock_release = subparsers.add_parser('lock_release')
    parser_lock_release.add_argument('lockname', type=str)
    parser_lock_release.add_argument('requester', type=str)
    parser_lock_release.add_argument('type', nargs="?", default='writer')

    args = vars(parser.parse_args())

    # Connecting to the server RPC
    if args['cmd'] in server_rpc:
        response = client_helper.connect(common2.SERVER_LOW, common2.SERVER_HIGH, args, args['server'])
        print response
    # Connecting to the Viewleader RPC
    elif args['cmd'] in viewleader_rpc:
        response = client_helper.connectVL(args, args['viewleader'])
        print response
    # The distributed getr and setr RPC functionality
    else:
        if args['cmd'] == "setr":
            client_helper.setr(args)
        else:
            client_helper.getr(args)

if __name__ == "__main__":
    main()
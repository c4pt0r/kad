import rpc
import gevent
import uuid
import json
import sys
import getopt
import os.path
import md5
import logger
import threading  
import time

def hex_to_int(h):
    return int(h, 16)

def load_guid():
    if os.path.isfile('.kad.conf'):
        with open('.kad.conf', 'r') as f:
            config = json.load(f)
            guid = config['guid']
    else:    
        guid = uuid.uuid1().hex
        config = {'guid': guid}
        with open('.kad.conf', 'w') as f:
            json.dump(config, f)
    return guid

def distance(node1, node2):
    return abs(node1 ^ node2)

class Kad:
    def __init__(self, ip, port, guid=None, K=8):
        if guid == None:
            self.guid = load_guid()
        else:
            self.guid = guid
        self.K = K
        self.bucket = self.generate_buckets()
        self.ip = ip
        self.port = port
        self.info = {'ip': ip, 'port': port, 'id': self.guid}

        self.stop_signal = threading.Event()
        threading.Thread(target = self.check_alive).start()

    def generate_buckets(self):
        buckets = []
        for i in range(128):
            buckets.append((2 ** i, []))
        return buckets

    def get_client(self, node_info):
        # TODO cache?
        cli = rpc.get_client(node_info['ip'], node_info['port'])
        return cli

    def is_node_alive(self, node_info):
        cli = self.get_client(node_info)
        try:
            cli.ping(self.info)
        except:
            return False
        return True

    def check_alive(self):
        # randomly ping other nodes in bucket
        while not self.stop_signal.wait(5):
            logger.info("check alive...")
            peers = self.get_peer_list()
            for peer in peers:
                if self.is_node_alive(peer) == False:
                    self.remove_peer(peer)
                time.sleep(0.5)

    def get_peer_list(self):
        peers = []
        for node_list in [b[1] for b in self.bucket]:
            peers += node_list
        return peers

    def find_nearby(self, key):
        key_hash = hex_to_int(md5.new(key).hexdigest())
        logger.info("find key:%s (hash: %s)", key, key_hash)
        # find nearest bucket
        peers = self.get_peer_list()
        peers.sort(key = lambda x: distance(key_hash, hex_to_int(x['id'])))
        return peers[:self.K]

    def update_bucket(self, remote_node_info):
        node_id = remote_node_info['id']
        # choose bucket 
        bucket = next((x for x in self.bucket[::-1] if x[0] <=
            distance(hex_to_int(node_id),
            hex_to_int(self.guid))), None)

        if bucket == None:
            return

        node_list = bucket[1]
        old = None 
        for i, node_info in enumerate(node_list):
            if node_info['ip'] == remote_node_info['ip'] and \
                node_info['port'] == remote_node_info['port']:
                    old = i
        # if ip:port is already in the list, we just move it to head
        if old != None:
            node_list.insert(0, node_list.pop(old))
        else:
            if len(node_list) < self.K:
                node_list.insert(0, remote_node_info)
            else:
                # if last one is dead, pop last one or just ignore it
                if not self.is_node_alive(node_list[-1]):
                    node_list.pop(len(node_list) - 1)
                    node_list.insert(0, remote_node_info)
                else:
                    # just ignore it 
                    pass

    def remove_peer(self, peer):
        for bucket in self.bucket:
            for p in bucket[1]:
                if p['id'] == peer['id']:
                    logger.info("remove peer: %s", peer)
                    bucket[1].remove(p) 


    def join_peer(self, peer):
        try:
            cli = rpc.get_client(peer['ip'], peer['port'])
            # get peer info through ping
            remote_info = cli.ping(self.info)
            # update to local bucket
            self.update_bucket(remote_info)
            # get more peer info through find_node rpc
            more_peers = cli.find_node(self.info, self.guid)
            for new_peer in more_peers:
                self.update_bucket(new_peer)
        except:
            logger.error("unexcept error when joining: %s", peer)
            return
        
    def join(self, peers):
        for peer in peers:
            self.join_peer(peer)

    def stop(self):
        self.stop_signal.set()
        self.rpc_server.force_stop()

    def serve(self):
        # server rpcs
        def ping(remote_node):
            logger.info("remote node: %s, ping me", remote_node)
            self.update_bucket(remote_node)
            return self.info

        def find_node(remote_node, key):
            self.update_bucket(remote_node)
            node_list = self.find_nearby(key)
            return node_list

        self.rpc_server = rpc.StoppableRPCServer(self.ip, self.port, [
            ping,
            find_node
        ])

        self.rpc_server.serve()

def main(argv):
    host, port = '127.0.0.1', 5532
    try:
        opts, args = getopt.getopt(argv,"h:p:",["host=","port="])
    except getopt.GetoptError:
        print 'kad.py -h <ip> -p <port>'
        sys.exit(2)
    for opt, arg in opts:
        if opt in ("-h", "--host"):
            host = arg
        elif opt in ("-p", "--port"):
            port = int(arg)

    kad = Kad(host, port)
    try:
        kad.serve()
    except (KeyboardInterrupt, SystemExit):
        print 'exit!'
        kad.stop()
    
if __name__ == '__main__':
    main(sys.argv[1:]) 

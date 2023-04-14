import os, re
import subprocess
import itertools
import argparse

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Generate configuration file for a batch of replicas')
    parser.add_argument('--prefix', type=str, default='hotstuff.gen')
    parser.add_argument('--ips', type=str, default=None)
    parser.add_argument('--iter', type=int, default=1)
    parser.add_argument('--pport', type=int, default=10000)
    parser.add_argument('--cport', type=int, default=20000)
    parser.add_argument('--keygen', type=str, default='./hotstuff-keygen')
    parser.add_argument('--tls-keygen', type=str, default='./hotstuff-tls-keygen')
    parser.add_argument('--nodes', type=str, default='nodes.txt')
    parser.add_argument('--block-size', type=int, default=1)
    parser.add_argument('--stable-period', type=int, default=100)
    parser.add_argument('--liveness-delta', type=int, default=50)
    parser.add_argument('--pace-maker', type=str, default='rr')
    args = parser.parse_args()

    if args.ips is None:
        ips = ['weak_0', 'weak_1', 'weak_2', 'weak_3']
    else:
        ips = [l.strip() for l in open(args.ips, 'r').readlines()]
        
    prefix = args.prefix
    iter = args.iter
    base_pport = args.pport
    base_cport = args.cport
    keygen_bin = args.keygen
    tls_keygen_bin = args.tls_keygen

    main_conf = open("./conf-gen/{}.conf".format(prefix), 'w')
    nodes = open('./conf-gen/'+args.nodes, 'w')

    # Speedbumps for large experiment
    # ips = ['strong_0', 'strong_1', 'strong_2', 'strong_3',
    #        'strong_4', 'strong_5', 'strong_6', 'strong_7',
    #        'strong_8', 'strong_9', 'strong_10', 'strong_11']
    # ips = ['weak_0', 'weak_1', 'weak_2', 'weak_3',
    #        'weak_4', 'weak_5', 'weak_6', 'weak_7',
    #        'weak_8', 'weak_9', 'weak_10', 'weak_11']
    # weight = [4, 10, 3, 4, 12, 3, 3, 15, 5, 3, 3, 15]
    # replicas = []
    # for i in range(12):
    #     ip = ips[i]
    #     w = weight[i]
    #     for j in range(w):
    #         replicas.append("{}:{};{}".format(ip, base_pport + j, base_cport + j))

    # Nodes for large experiment
    replicas = []
    for i in range(20):
        for j in range(4):
            replicas.append("host{}:{};{}".format(i, base_pport + j, base_cport + j))

    # Original replicas
    #replicas = ["{}:{};{}".format(ip, base_pport + i, base_cport + i)
    #            for ip in ips
    #            for i in range(iter)]


    p = subprocess.Popen([keygen_bin, '--num', str(len(replicas))],
                        stdout=subprocess.PIPE, stderr=open(os.devnull, 'w'))
    keys = [[t[4:] for t in l.decode('ascii').split()] for l in p.stdout]
    tls_p = subprocess.Popen([tls_keygen_bin, '--num', str(len(replicas))],
                        stdout=subprocess.PIPE, stderr=open(os.devnull, 'w'))
    tls_keys = [[t[4:] for t in l.decode('ascii').split()] for l in tls_p.stdout]
    if not (args.block_size is None):
        main_conf.write("block-size = {}\n".format(args.block_size))
    if not (args.stable_period is None):
        main_conf.write("stable-period = {}\n".format(args.stable_period))
    if not (args.liveness_delta is None):
        main_conf.write("liveness-delta = {}\n".format(args.liveness_delta))
        
    if not (args.pace_maker is None):
        main_conf.write("pace-maker = {}\n".format(args.pace_maker))
    for r in zip(replicas, keys, tls_keys, itertools.count(0)):
        main_conf.write("replica = {}, {}, {}\n".format(r[0], r[1][0], r[2][2]))
        r_conf_name = "./conf-gen/{}-sec{}.conf".format(prefix, r[3])
        nodes.write("{}:{}\t{}\n".format(r[3], r[0], r_conf_name))
        r_conf = open(r_conf_name, 'w')
        r_conf.write("privkey = {}\n".format(r[1][1]))
        r_conf.write("tls-privkey = {}\n".format(r[2][1]))
        r_conf.write("tls-cert = {}\n".format(r[2][0]))
        r_conf.write("idx = {}\n".format(r[3]))

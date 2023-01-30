
import os
import time

import networkx as nx
from networkx.algorithms.community import louvain_communities as networkx_louvain


import ukv.umem as ukv
import louvain

dataset_dir = 'python/community/datasets'

datasets = [('Email', 'email-Eu-core.txt', 'https://snap.stanford.edu/data/email-Eu-core.txt.gz'),
            ('Facebook', 'facebook_combined.txt',
            'https://snap.stanford.edu/data/facebook.tar.gz'),
            ('Amazon', 'com-amazon.ungraph.txt',
             'https://snap.stanford.edu/data/bigdata/communities/com-amazon.ungraph.txt.gz'),
            ('Youtube', 'com-youtube.ungraph.txt',
             'https://snap.stanford.edu/data/bigdata/communities/com-youtube.ungraph.txt.gz'),
            ('Orkut', 'com-orkut.ungraph.txt', 'https://snap.stanford.edu/data/bigdata/communities/com-orkut.ungraph.txt.gz')]

for name, dataset, _ in datasets:

    G = nx.read_edgelist(os.path.join(dataset_dir, dataset), nodetype=int)

    t1 = time.perf_counter()
    networkx_louvain(G)
    t2 = time.perf_counter()
    print('Elapsed time for {} dataset with NetworkX: {:.3f}s '.format(name, t2-t1))

    db = ukv.DataBase()
    main = db.main
    graph = main.graph
    for v1, v2 in G.edges:
        graph.add_edge(v1, v2)
    G.clear()

    t1 = time.perf_counter()
    louvain.best_partition(graph)
    t2 = time.perf_counter()

    print('Elapsed time for {} dataset with UKV Python: {:.3f}s '.format(name, t2-t1))

    t1 = time.perf_counter()
    graph.community_louvain()
    t2 = time.perf_counter()

    print('Elapsed time for {} dataset with UKV CPP: {:.3f}s '.format(name, t2-t1))

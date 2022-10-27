import ukv.umemkv as ukv
import numpy as np


def line(net):
    # 1 -> 2
    assert not net.has_node(1)
    assert not net.has_node(2)
    assert not net.has_edge(1, 2)

    net.add_edge(1, 2)
    assert net.has_node(1)
    assert net.has_node(2)
    assert net.number_of_edges(1, 2) == 1
    assert net.has_edge(1, 2)

    assert not net.has_edge(2, 2)

    net.remove_edge(1, 2)
    assert net.has_node(1)
    assert net.has_node(2)
    assert not net.has_edge(1, 2)


def triangle(net):
    # 1 -> 2 -> 3 -> 1

    net.add_edge(1, 2)
    net.add_edge(2, 3)
    net.add_edge(3, 1)

    assert net.has_node(1) and net.has_node(2) and net.has_node(3)

    assert net.number_of_edges(1, 2) == 1
    assert net.number_of_edges(2, 3) == 1
    assert net.number_of_edges(3, 1) == 1

    assert len(list(net.successors(1))) == 1
    assert len(list(net.predecessors(1))) == 1

    net.remove_edge(1, 2)
    net.remove_edge(2, 3)
    net.remove_edge(3, 1)


def triangle_batch(net):
    # TODO: Check why matrix casting fails!
    # edges = np.array([[1, 2], [2, 3], [3, 1]])
    # assert edges.shape[1] == 2
    # net.add_edges_from(edges)
    net.add_edges_from(np.array([1, 2, 3]), np.array([2, 3, 1]))

    assert net.has_node(1) and net.has_node(2) and net.has_node(3)
    assert not 4 in net

    assert net.has_edge(1, 2) and net.has_edge(2, 3) and net.has_edge(3, 1)
    assert not net.has_edge(2, 1)

    assert len(list(net.successors(1))) == 1
    assert len(list(net.predecessors(1))) == 1

    net.remove_edges_from(np.array([1, 2]), np.array([2, 3]))

    assert not net.has_edge(1, 2) and not net.has_edge(2, 3)
    assert net.has_edge(3, 1)


def test():
    db = ukv.DataBase()
    main = db.main
    net = main.graph
    line(net)
    triangle(net)
    triangle_batch(net)

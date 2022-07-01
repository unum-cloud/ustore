import ukv.stl as ukv


def line(net):
    # 1 -> 2

    assert not net.has_node(1)
    assert not net.has_node(2)
    assert not net.has_edge(1, 2)

    net.add_edge(1, 2)
    assert net.has_node(1)
    assert net.has_node(2)
    assert net.count_edges(1, 2) == 1
    assert net.has_edge(1, 2)

    assert not net.has_edge(2, 2)
    assert not net.has_edge(1, 2)

    net.remove_edge(1, 2)
    assert not net.has_node(1)
    assert not net.has_node(2)
    assert not net.has_edge(1, 2)

def triangle(net):
    # 1 -> 2 -> 3

    net.add_edge(1, 2)
    net.add_edge(2, 3)
    net.add_edge(3, 1)

    assert net.has_node(1) and net.has_node(2) and net.has_node(3)
    assert len(net.neighbors(1)) == 2
    assert len(net.successors(1)) == 1
    assert len(net.predecessors(1)) == 1

    net.remove_edge(1, 2)
    net.remove_edge(2, 3)
    net.remove_edge(3, 1)


def test_main_net():
    net = ukv.Network()
    line(net)
    triangle(net)


def test_named_nets():
    net = ukv.Network(
        relation_name='views',
        source_attributes='people',
        target_attributes='movies',
        multi=False,
        attributed_relations=True,
    )
    line(net)
    triangle(net)
    
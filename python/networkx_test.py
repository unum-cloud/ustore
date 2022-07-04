import networkx as ukv


def test_simple():
    # 1 -> 2
    net = ukv.DiGraph()
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


def test_triangle():
    # 1 -> 2 -> 3
    net = ukv.DiGraph()

    net.add_edge(1, 2)
    net.add_edge(2, 3)
    net.add_edge(3, 1)

    assert net.has_node(1) and net.has_node(2) and net.has_node(3)
    assert len(list(net.successors(1))) == 1
    assert len(list(net.predecessors(1))) == 1

    net.remove_edge(1, 2)
    net.remove_edge(2, 3)
    net.remove_edge(3, 1)


def test_batch():
    nodes_count = 100
    edges = []
    for i in range(nodes_count-1):
        edges.append((i, i+1))

    net = ukv.DiGraph()
    net.add_edges_from(edges)

    for i in range(nodes_count-1):
        assert net.has_edge(i, i+1)
        assert net.has_node(i)

    middle_index = round(len(edges) / 2)

    net.remove_edges_from(edges[:middle_index])

    for edge in edges[:middle_index]:
        assert not net.has_edge(edge[0], edge[1])

    for edge in edges[middle_index:]:
        assert net.has_edge(edge[0], edge[1])


def test_clear():
    nodes_count = 100
    edges = []
    for i in range(nodes_count-1):
        edges.append((i, i+1))

    net = ukv.DiGraph()
    net.add_edges_from(edges)

    assert list(net.edges()) == edges
    assert list(net.nodes) == list(range(nodes_count))

    for edge in edges:
        assert net.has_edge(edge[0], edge[1])

    net.clear_edges()

    for edge in edges:
        assert not net.has_edge(edge[0], edge[1])

    for node in range(nodes_count):
        assert net.has_node(node)

    net.clear()

    for node in range(nodes_count):
        assert not net.has_node(node)


def test_degree():
    nodes_count = 100
    net = ukv.Graph()
    edges = []
    for i in range(nodes_count):
        for j in range(i+1, nodes_count):
            edges.append((i, j))

    net = ukv.DiGraph()
    net.add_edges_from(edges)

    for node in range(nodes_count):
        assert net.degree[node] == nodes_count-1

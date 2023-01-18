from typing import Mapping, Tuple, List

import networkx as nx

def node_affiliation(
        graph,
        partition: Mapping[int, int],
        node: int, node_community: int, neighbor_community: int, is_weighted:bool) -> Tuple[int, int]:
    """
    Simultaneously computes the number of edges between `node` and
    members of `node_community` as well as `neighbor_community`.
    """
    node_community_degree = 0
    neighbor_community_degree = 0
    if is_weighted:
        for neighbor,datas in graph[node].items():
            if (partition[neighbor] == node_community):
                node_community_degree += datas.get('weight', 1)
            elif (partition[neighbor] == neighbor_community):
                neighbor_community_degree += datas.get('weight', 1)
    else:
        for neighbor in graph[node]:
            if (partition[neighbor] == node_community):
                node_community_degree += 1
            elif (partition[neighbor] == neighbor_community):
                neighbor_community_degree += 1
    return (node_community_degree, neighbor_community_degree)


def try_moving(
        graph,
        partition: Mapping[int, int], community_degrees: Mapping[int, int], count_edges: int,
        node: int, node_degree: int, neighbor: int, min_modularity_growth: float, is_weighted:bool) -> bool:
    """
    Attempts to move `node` to the parent community of its `neighbor`.
    """

    node_community = partition[node]
    neighbor_community = partition[neighbor]

    node_com_tot_degree = community_degrees[node_community]
    neighbor_com_tot_degree = community_degrees[neighbor_community]

    degree_in_node_com, degree_in_neighbor_com = node_affiliation(
        graph, partition, node, node_community, neighbor_community, is_weighted)

    delta_modularity = (1 / count_edges) * (degree_in_neighbor_com - degree_in_node_com) - \
        (node_degree / (2 * (count_edges ** 2))) * \
        (node_degree + neighbor_com_tot_degree - node_com_tot_degree)

    if delta_modularity > min_modularity_growth:
        community_degrees[node_community] -= (node_degree - degree_in_node_com)
        community_degrees[neighbor_community] += (
            node_degree - degree_in_neighbor_com)
        partition[node] = partition[neighbor]
        return True

    return False


def one_level(graph, min_modularity_growth: float, is_weighted:bool) -> Mapping[int, int]:
    """
    Compute one level communities of graph.
    """

    partition = {}
    degrees = {}
    # For every partition, stores the accumulated number of degrees of all of its members
    community_degrees = {}

    if is_weighted:
        for node in graph.nodes:
            degrees[node] = graph.degree(weight='weight')[node]
            partition[node] = node
    else:
        for node in graph.nodes:
            degrees[node] = graph.degree[node]
            partition[node] = node

    community_degrees = degrees.copy()
    count_edges = graph.number_of_edges()

    modified = 1
    while modified:
        modified = 0
        for v1, v2 in graph.edges:
            if partition[v1] != partition[v2]:
                modified += \
                    try_moving(graph, partition, community_degrees, count_edges, v1, degrees[v1], v2, min_modularity_growth, is_weighted) or \
                    try_moving(graph, partition, community_degrees,
                               count_edges, v2, degrees[v2], v1, min_modularity_growth, is_weighted)

    return partition


def final_partition(partitions: List[Mapping[int, int]]) -> Mapping[int, int]:
    """
    Compute top level communities from all partitions.
    """

    partition = partitions[0]
    for index in range(1, len(partitions)):
        for node,com in partitions[index].items():
            partitions[index][node] = partition[com]
        partition = partitions[index]

    return partition


def super_node_graph(graph, partition: Mapping[int, int], is_weighted: bool):
    """
    Create super node graph, where nodes are communities and edges are degrees between communities.
    """
    G = nx.Graph()
    G.add_nodes_from(partition.values())

    if is_weighted:
        for node1, node2,datas in graph.edges(data=True):
            com1 = partition[node1]
            com2 = partition[node2]
            if com1 == com2:
                continue
            edge_weight = datas.get('weight', 1)
            w_prec = G.get_edge_data(com1, com2, {"weight": 0}).get("weight", 1)
            G.add_edge(com1, com2, **{"weight": w_prec + edge_weight})
    else:
        for node1, node2 in graph.edges:
            com1 = partition[node1]
            com2 = partition[node2]
            if com1 == com2:
                continue
            w_prec = G.get_edge_data(com1, com2, {"weight": 0}).get("weight", 1)
            G.add_edge(com1, com2, **{"weight": w_prec + 1})

    return G

def best_partition(graph,  min_modularity_growth: float = 0.0000001) -> Mapping[int, int]:
    """
    Compute the partition of the graph nodes which maximises the modularity.
    """

    partitions = []
    partition = one_level(graph, min_modularity_growth, is_weighted = False)
    partitions.insert(0, partition)
    graph = super_node_graph(graph, partition, False)

    while True:
        partition = one_level(graph, min_modularity_growth, is_weighted = True)

        modularity_changed = False
        for key,value in partition.items():
            if key != value:
                modularity_changed = True
                break
        if not modularity_changed:
            break

        partitions.insert(0,partition)
        graph = super_node_graph(graph, partition, is_weighted = True)


    return final_partition(partitions)
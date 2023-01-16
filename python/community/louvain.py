from typing import Mapping, Tuple


def node_affiliation(
        graph,
        partition: Mapping[int, int],
        node: int, node_community: int, neighbor_community: int) -> Tuple[int, int]:
    """
    Simultaneously computes the number of edges between `node` and
    members of `node_community` as well as `neighbor_community`.
    """
    node_community_degree = 0
    neighbor_community_degree = 0
    for neighbor in graph.neighbors(node):
        if (partition[neighbor] == node_community):
            node_community_degree += 1
        elif (partition[neighbor] == neighbor_community):
            neighbor_community_degree += 1
    return (node_community_degree, neighbor_community_degree)


def try_moving(
        graph,
        partition: Mapping[int, int], community_degrees: Mapping[int, int], count_edges: int,
        node: int, node_degree: int, neighbor: int, min_modularity_growth: float) -> bool:
    """
    Attempts to move `node` to the parent community of its `neighbor`.
    """

    node_community = partition[node]
    neighbor_community = partition[neighbor]

    node_com_tot_degree = community_degrees[node_community]
    neighbor_com_tot_degree = community_degrees[neighbor_community]

    degree_in_node_com, degree_in_neighbor_com = node_affiliation(
        graph, partition, node, node_community, neighbor_community)

    delta_modularity = (1 / count_edges) * (degree_in_neighbor_com - degree_in_node_com) - \
        (node_degree / (2 * (count_edges**2))) * \
        (node_degree + neighbor_com_tot_degree - node_com_tot_degree)

    if delta_modularity > min_modularity_growth:
        community_degrees[node_community] -= (node_degree - degree_in_node_com)
        community_degrees[neighbor_community] += (
            node_degree - degree_in_neighbor_com)
        partition[node] = partition[neighbor]
        return True

    return False


def best_partition(graph, min_modularity_growth: float = 0.0000001) -> Mapping[int, int]:
    partition = {}
    degrees = {}
    # For every partition, stores the accumulated number of degrees of all of its members
    community_degrees = {}

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
                    try_moving(graph, partition, community_degrees, count_edges, v1, degrees[v1], v2, min_modularity_growth) or \
                    try_moving(graph, partition, community_degrees,
                               count_edges, v2, degrees[v2], v1, min_modularity_growth)

    return partition

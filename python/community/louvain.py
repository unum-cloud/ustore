def get_degree_of_node_in_community(graph,node, partition,community):
    result=0
    for neighbor in graph.neighbors(node):
        if partition[neighbor] == community:
            result+=1
    return result

def best_partition(graph):
    partition = {}
    degrees = {}
    com_in_degree = {}
    com_tot_degree = {}

    for node in graph.nodes:
        partition[node] = node
        degrees[node] = graph.degree(node)
        com_in_degree[node] = 0
     
    com_tot_degree=degrees.copy()
    m2 = graph.number_of_edges() * 2

    for node,neighbor in graph.edges:
        if partition[node] == partition[neighbor]:
            continue
        community = partition[node]
        new_community = partition[neighbor]

        K_i = degrees[node]
        E_in = com_in_degree[new_community]
        E_tot = com_tot_degree[new_community]
        K_i_in = get_degree_of_node_in_community(graph,node,partition,new_community)
        Q_After = (E_in + K_i_in)/m2 - ((E_tot + K_i)/m2 ** 2)
        Q_Before = (E_in/m2) - ((E_tot/m2)**2) - ((K_i/m2) ** 2)
        delta_merge = Q_After - Q_Before

        E_in = com_in_degree[community]
        E_tot = com_tot_degree[community]
        K_i_in = get_degree_of_node_in_community(graph,node,partition,community)
        Q_After = (E_in + K_i_in)/m2 - ((E_tot + K_i)/m2) ** 2
        Q_Before = (E_in/m2) - (E_tot/m2)**2
        delta_remove = Q_After - Q_Before

        delta = delta_remove + delta_merge
        if delta>0:
            deg = degrees[node] 
            com_tot_degree[community] -= deg
            com_tot_degree[new_community] += deg
            for n in graph.neighbors(node):
                if partition[n] == community:
                    com_in_degree[community]-=1
                if partition[n] == new_community:   
                    com_in_degree[new_community]+=1
            partition[node] = partition[neighbor]
    return partition


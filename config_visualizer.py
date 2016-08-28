'''
Create a visual representation of the various DAGs defined
'''

import sys
import networkx as nx
import matplotlib.pyplot as plt

import dag_proxy.dag


def Dagyr_to_graph(g, labels, node):
    g.add_node(node)
    labels['nodes'][node] = (node.node_config['func'], node.args)

    # add all my children + edges + recurse
    for key, child in node.children.iteritems():
        g.add_node(child)
        g.add_edge(node, child, key=key)
        labels['edges'][(node, child)] = key
        # recurse!
        Dagyr_to_graph(g, labels, child)


if __name__ == '__main__':
    print 'Creating a visualization of config %s' % sys.argv[1]
    dag_config = dag_proxy.dag.Dagyr.from_file(sys.argv[1])

    control_g = nx.DiGraph()
    labels = {
        'edges': {},
        'nodes': {},
    }
    Dagyr_to_graph(control_g, labels, dag_config.control_dag)

    # TODO: embed into Dagyr (or DAG class internally)
    print nx.algorithms.is_directed_acyclic_graph(control_g)


    pos = nx.spring_layout(control_g)
    nx.draw(control_g, pos=pos)

    # add labels
    nx.draw_networkx_labels(control_g, pos, labels['nodes'])
    nx.draw_networkx_edge_labels(control_g, pos, labels['edges'])

    # write out the graph
    plt.savefig('dag.png')
    plt.show()  # in case people have the required libraries to make it happen

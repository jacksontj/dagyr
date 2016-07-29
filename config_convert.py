'''
Utility to convert the "new" config format to a more readable format with depth
instead of all of the linking
'''

import yaml

def convert_processing_nodes_to_dag(node_types, nodes, starting_node):
    ret = nodes[starting_node]
    type_id = ret.pop('type_id')
    ret.update(node_types[type_id])
    for outlet_k, outlet_node_id in ret.get('outlet', {}).items():
        ret['outlet'][outlet_k] = convert_processing_nodes_to_dag(node_types, nodes, outlet_node_id)

    return ret


if __name__ == '__main__':
    base = None
    with open('TODO_config.yaml') as fh:
        base = yaml.load(fh)

    #print yaml.dump(base, default_flow_style=False)

    KEYS_TO_COPY = (
        'options_data',
        'execution_context',
    )

    new = {
        'dags': {},
    }
    for k in KEYS_TO_COPY:
        new[k] = base[k]

    # convert the dags
    for dag_key, dag_config in base['dags'].iteritems():
        new['dags'][dag_key] = convert_processing_nodes_to_dag(
            base['processing_node_types'],
            dag_config['processing_nodes'],
            dag_config['starting_node'],
        )

    print yaml.dump(new, default_flow_style=False)

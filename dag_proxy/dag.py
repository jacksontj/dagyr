import yaml
import logging


import fragments


log = logging.getLogger(__name__)

class DagNodeType(object):
    '''A node template for the DAG
    '''
    def __init__(self, node_type_config):
        self.node_type_config = node_type_config

        self.func = getattr(fragments, node_type_config['fragment_func']).fragment
        self.spec = node_type_config['fragment_spec']


class DagNode(object):
    '''A node in the DAG

    A node in the dag-- otherwise known as a Fragment
    '''
    def __init__(self, dag_key, node_id, node_config, node_types):
        self.dag_key = dag_key
        self.node_id = node_id
        self.outlets = node_config.get('outlets', {})
        self.node_type = node_types[node_config['type_id']]

        # TODO: get these
        self.fragment_args = node_config['args']

    def link_children(self, nodes):
        '''called after all nodes are created to link the together
        '''
        self.children = {}
        for k, child_id in self.outlets.iteritems():
            self.children[k] = nodes[child_id]

    def __repr__(self):
        return str((self.dag_key, self.node_id))

    def __call__(self, state):
        '''Run the node and return
        '''
        return self.node_type.func(state, self.node_type.spec, self.fragment_args)

    def get_child(self, key):
        '''Return the next node (if there is one)
        '''
        if not self.children:
            return None
        else:
            return self.children[key]


# TODO rename to dag_proxy_config or something like that, since its actually the whole config
# holder of the whole DAG config
class DagConfig(object):
    '''Holder of the Dags!
    This holds a control_dag (effectively a global one) and a mapping of
        namespace -> name -> DAG
    for all the dynamic_dags
    '''
    def __init__(self, config):
        self.config = config

        self.processing_node_types = {}
        for k, cfg in self.config['processing_node_types'].iteritems():
            self.processing_node_types[k] = DagNodeType(cfg)

        self.dags = {}
        for dag_key, dag_meta in self.config['dags'].iteritems():
            # temp space for the nodes as we need to create them
            dag_nodes = {}
            for node_id, node in dag_meta['processing_nodes'].iteritems():
                dag_nodes[node_id] = DagNode(dag_key, node_id, node, self.processing_node_types)

            # link the children together
            for n in dag_nodes.itervalues():
                n.link_children(dag_nodes)
            self.dags[dag_key] = dag_nodes[dag_meta['starting_node']]

    @staticmethod
    def from_file(filepath):
        with open(filepath, 'r') as fh:
            cfg = yaml.load(fh)
            return DagConfig(cfg)


class DagExecutor(object):
    '''This object is responsible for stepping a transaction through a DagConfig
    '''
    HOOKS = (
        'ingress',
        'egress',
    )
    def __init__(self, dag_config, ctx):
        self.dag_config = dag_config
        self.context = ctx

    def call_hook(self, hook_name):
        if hook_name not in self.HOOKS:
            raise RuntimeError('InvalidHook!! {0} not in {1}'.format(hook_name, self.HOOKS))

        execution_context = self.dag_config.config['execution_context'][hook_name]
        self.context.options = self.dag_config.config['options_data'][execution_context['options_data']]
        self.context.dag_path[hook_name] = []
        self._call_dag(
            self.dag_config.dags[execution_context['dag_name']],
            self.context.dag_path[hook_name],
        )

    def _call_dag(self, node, path=None):
        # call the control_dag
        while True:
            try:
                node_ret = node(self.context)
                next_node = node.get_child(node_ret)
                # TODO: change to namedtuple?
                path.append({
                    'node': str(node),
                    'node_ret': node_ret,
                    'next_node': str(next_node),
                })

                # if the node we executed has no children, we need to see if we
                # should run another DAG, if not then we are all done
                if next_node is None:
                    if self.context.state.next_dag is not None:
                        node = self.dag_config.dags[self.context.state.next_dag]
                        # TODO: consolidate into a step() method?
                        self.context.state.next_dag = None
                        continue
                    break
                node = next_node
            except Exception as e:
                log.error('Error executing DAG %s' % node, exc_info=True)
                break

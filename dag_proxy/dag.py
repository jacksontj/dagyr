import copy
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

        self.fragment_args = node_config['args']


    def link_children(self, nodes):
        '''called after all nodes are created to link the together
        '''
        self.children = {}
        for k, child_id in self.outlets.iteritems():
            self.children[k] = nodes[child_id]

    def __repr__(self):
        return str((self.dag_key, self.node_id))

    def __call__(self, ctx):
        '''Run the node and return
        '''
        # TODO: more efficient?
        # If is_option_data, we need to do the lookup at execution time
        # since the value could be something on the transaction -- which
        # means we can't resolve it at init time.
        args = copy.deepcopy(self.fragment_args)
        for argname, spec in self.node_type.spec.iteritems():
            if spec.get('is_option_data', False):
                args[argname] = ctx.options[self.fragment_args[argname]]
        return self.node_type.func(ctx, self.node_type.spec, args)

    def get_child(self, key):
        '''Return the next node (if there is one)
        '''
        if not self.children:
            return None
        else:
            return self.children[key]


# TODO: validation methods
class Dag(object):
    '''Object to represent the whole dag
    '''
    def __init__(self, name, config, processing_node_types=None):
        self.option_data = config.get('option_data', {})
        self.nodes = {}
        self.edges = set()

        # temp space for the nodes as we need to create them
        for node_id, node in config['processing_nodes'].iteritems():
            self.nodes[node_id] = DagNode(
                name,
                node_id,
                node,
                processing_node_types,
            )
            # TODO: populate edges, ensure no loops?
            #print node_id, self.nodes[node_id].outlets.values()

        # link the children together
        for n in self.nodes.itervalues():
            n.link_children(self.nodes)
        self.starting_node = self.nodes[config['starting_node']]


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
            self.dags[dag_key] = Dag(
                dag_key,
                dag_meta,
                processing_node_types=self.processing_node_types,
            )

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

        # name of the dag to run for the given hook
        hook_dag_name = self.dag_config.config['hook_dag_map'][hook_name]['dag_name']
        dag = self.dag_config.dags[hook_dag_name]
        self.context.options = dag.option_data
        self.context.dag_path[hook_name] = []
        self._call_dag(
            dag.starting_node,
            self.context.dag_path[hook_name],
        )

    # TODO: change arg to DAG, and have the Dag object handle traversing the nodes?
    # this way it'll be easier to keep track of the path
    def _call_dag(self, node, path=None):
        # call the control_dag
        while True:
            try:
                node_ret = node(self.context)
                next_node = node.get_child(node_ret)
                # TODO: keep track of DAG name?
                # TODO: change to namedtuple?
                path.append({
                    'dag': str(node.dag_key),
                    'node': str(node),
                    'node_ret': node_ret,
                    'next_node': str(next_node),
                })

                # if the node we executed has no children, we need to see if we
                # should run another DAG, if not then we are all done
                if next_node is None:
                    if self.context.state.next_dag is not None:
                        node = self.dag_config.dags[self.context.state.next_dag].starting_node
                        # TODO: consolidate into a step() method?
                        self.context.state.next_dag = None
                        continue
                    break
                node = next_node
            except Exception as e:
                log.error('Error executing DAG %s' % node, exc_info=True)
                break

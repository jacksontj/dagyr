import copy
import yaml
import logging


import state
import conversion
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

    def __call__(self, context):
        '''Run the node and return
        '''
        # TODO: more efficient?
        # If is_option_data, we need to do the lookup at execution time
        # since the value could be something on the transaction -- which
        # means we can't resolve it at init time.
        args = copy.deepcopy(self.fragment_args)
        for argname, spec in self.node_type.spec.iteritems():
            if spec.get('is_option_data', False):
                args[argname] = context.options[self.fragment_args[argname]]
        return self.node_type.func(context, self.node_type.spec, args)

    def get_child(self, key):
        '''Return the next node (if there is one)
        '''
        if not self.children:
            return None
        else:
            return self.children[key]


def is_acyclic(path, node):
    '''Check that all given paths from node (given path) are acyclic
    '''
    ret = True
    if node in path:
        return False
    path.append(node)
    for child in node.children.itervalues():
        ret &= is_acyclic(path, child)
    return ret


# TODO: validation methods
class Dag(object):
    '''Object to represent the whole dag
    '''
    def __init__(self, name, config, processing_node_types=None):
        self.name = name
        self.option_data = config.get('option_data', {})
        self.nodes = {}

        # temp space for the nodes as we need to create them
        for node_id, node in config['processing_nodes'].iteritems():
            self.nodes[node_id] = DagNode(
                name,
                node_id,
                node,
                processing_node_types,
            )

        # link the children together
        for n in self.nodes.itervalues():
            n.link_children(self.nodes)
        self.starting_node = self.nodes[config['starting_node']]

        if not is_acyclic([], self.starting_node):
            raise Exception("configured DAG {0} is cyclic!!!".format(name))

    def __call__(self, context):
        node = self.starting_node
        path = []
        # call the control_dag
        while True:
            node_ret = node(context)
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

                break
            node = next_node
        return path


# TODO rename to dag_proxy_config or something like that, since its actually the whole config
# holder of the whole DAG config
class DagConfig(object):
    '''Holder of the Dags!
    This holds a control_dag (effectively a global one) and a mapping of
        namespace -> name -> DAG
    for all the dynamic_dags
    '''
    CONVERSION_FUNCS = {
        'trie': conversion.make_trie,
    }
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

        # will access the same data
        # cache of data converted to various types
        # this map will be converted_type -> orig_item -> converted_item
        self._data_cache = {}

    def convert_item(self, convert_type, orig_item):
        '''Convert a hashable item `orig_item` to `convert_type`
        '''
        if convert_type not in self._data_cache:
            self._data_cache[convert_type] = {}

        if orig_item not in self._data_cache[convert_type]:
            self._data_cache[convert_type][orig_item] = self.CONVERSION_FUNCS[convert_type](orig_item)

        return self._data_cache[convert_type][orig_item]

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
    def __init__(self, dag_config, req_state):
        self.dag_config = dag_config

        self.context = state.Context(
            dag_config,
            req_state,
        )

    def call_hook(self, hook_name):
        if hook_name not in self.HOOKS:
            raise RuntimeError('InvalidHook!! {0} not in {1}'.format(hook_name, self.HOOKS))

        # name of the dag to run for the given hook
        hook_meta = self.dag_config.config['hook_dag_map'][hook_name]
        dag = self.dag_config.dags[hook_meta['dag_name']]
        if 'option_data_key' in hook_meta:
            self.context.options = self.dag_config.config['option_data'][hook_meta['option_data_key']]
        else:
            self.context.options = {}

        # TODO: ordereddict?
        # list of tuple (dag, (node, node, ...))
        self.dag_path = []
        # map of dag -> path
        self.dag_path_map = {}

        while dag:
            if dag.name in self.dag_path_map:
                log.error("hook execution for {0} looping on dag {1}, path={2}".format(
                    hook_name,
                    dag.name,
                    path=self.dag_path,
                ))
                # TODO: better exception?
                raise Exception()
            node_path = dag(self.context)
            self.dag_path.append((dag.name, node_path))
            self.dag_path_map[dag.name] = node_path

            # set the next DAG to run
            if self.context.next_dag is not None:
                dag = self.dag_config.dags[self.context.next_dag]
                # TODO: consolidate into a step() method?
                self.context.next_dag = None
            else:
                dag = None

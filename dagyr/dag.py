import copy
import yaml
import logging
import pyrsistent
import collections


import state
import conversion
import processing_nodes


log = logging.getLogger(__name__)

ARG_TYPES = {
    'string': str,
    'int': int,
    # since we are using immutable types, we need a more basic check for python
    'list': collections.Sequence,
    'dict': collections.Mapping,
}


class DagNodeType(object):
    '''A node template for the DAG
    '''
    def __init__(self, node_type_config):
        self.node_type_config = node_type_config

        self.func = getattr(processing_nodes, node_type_config['func']).processing_node
        self.arg_spec = node_type_config['arg_spec']


class DagNode(object):
    '''A node in the DAG

    A node in a dag-- otherwise known as a processing_node
    '''
    def __init__(self, dag_key, node_id, node_config, node_types):
        self.dag_key = dag_key
        self.node_id = node_id
        self.outlets = node_config.get('outlets', {})
        self.node_type = node_types[node_config['type_id']]

        self.args = node_config['args']

    def link_children(self, nodes):
        '''called after all nodes are created to link them together
        '''
        self.children = {}
        for k, child_id in self.outlets.iteritems():
            self.children[k] = nodes[child_id]

    def __repr__(self):
        return str((self.dag_key, self.node_id))

    def __call__(self, context):
        '''Run the node and return
        '''
        resolved_args = {}
        for arg_name, arg_spec in self.node_type.arg_spec.iteritems():
            if 'global_option_data_key' in arg_spec:
                resolved_args[arg_name] = context.options[arg_spec['global_option_data_key']][self.args[arg_name]]
            else:
                resolved_args[arg_name] = self.args[arg_name]

            # TODO: only the `global_option_data_key` args need to have their type
            #   checked before calling
            # validate that the resolved values are of the type defined in the arg_spec
            if 'type' in arg_spec and not isinstance(resolved_args[arg_name], ARG_TYPES[arg_spec['type']]):
                raise Exception('Arg {0}={1} type={2} expected_type={3}'.format(
                    arg_name,
                    resolved_args[arg_name],
                    type(resolved_args[arg_name]),
                    ARG_TYPES[arg_spec['type']],
                ))

        return self.node_type.func(context, self.node_type.arg_spec, self.args, resolved_args)

    def get_child(self, key):
        '''Return the next node (if there is one)
        '''
        if not self.children:
            return None
        else:
            return self.children[key]


class Dag(object):
    '''Object to represent the whole dag

    This class includes all of the DAG validation, node creation, and the
    method for actually executing the DAG.
    '''
    def __init__(self, name, config, processing_node_types=None):
        self.name = name
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

        # verify that the graph is acyclig (thereby making it a DAG)
        if not Dag.is_acyclic([], self.starting_node):
            raise Exception("configured DAG {0} is cyclic!!!".format(name))

        # As a nicety, we'll check that all nodes defined in the list are actually
        # in use in the DAG, if not we'll log a warning, it is not fatal but if
        # the config is generated this might indicate a problem
        used_nodes = Dag.dag_nodes(self.starting_node)
        all_nodes = set(self.nodes.iterkeys())
        if used_nodes != all_nodes:
            log.warning('The following nodes in DAG {0} are not linked: {1}'.format(
                self.name,
                all_nodes - used_nodes,
            ))

    def __call__(self, context):
        '''Execute the DAG with the given context
        '''
        node = self.starting_node
        path = []
        # call the control_dag
        while True:
            node_ret = node(context)
            next_node = node.get_child(node_ret)
            # TODO: change to namedtuple?
            path.append({
                'node': node,
                'node_ret': node_ret,
                'next_node': next_node,
            })

            # if the node we executed has no children, we need to see if we
            # should run another DAG, if not then we are all done
            if next_node is None:
                break
            node = next_node
        return path

    @staticmethod
    def all_paths(node):
        '''Return a list of all paths in the DAG attached to `node`
        '''
        # fast path
        if not node.children:
            return [[node.node_id]]

        paths = []
        for child in node.children.itervalues():
            for p in Dag.all_paths(child):
                p.insert(0, node.node_id)
                paths.append(p)
        return paths

    @staticmethod
    def is_acyclic(path, node):
        '''Check that all given paths from node (given path) are acyclic
        '''
        ret = True
        if node in path:
            return False
        path.append(node)
        for child in node.children.itervalues():
            ret &= Dag.is_acyclic(path, child)
        return ret

    @staticmethod
    def dag_nodes(node, node_set=None):
        '''Return the list of nodes that are linked in the DAG
        '''
        if node_set is None:
            node_set = set()
        node_set.add(node.node_id)
        for child in node.children.itervalues():
            Dag.dag_nodes(child, node_set=node_set)
        return node_set


# TODO rename to dag_set? or something like that, since its actually the whole config
# holder of the whole DAG config
class DagConfig(object):
    '''Holder of configuration

    This is the top level object that contains all objects within the config file.
    '''
    # TODO: make pluggable
    CONVERSION_FUNCS = {
        'trie': conversion.make_trie,
    }
    def __init__(self, config):
        # freeze config -- we don't want anything changing it-- so we'll just
        # freeze it now
        self.config = pyrsistent.freeze(config)

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
        self.data_cache = {}

    def convert_item(self, convert_type, orig_item):
        '''Convert a hashable item `orig_item` to `convert_type`
        '''
        if convert_type not in self.data_cache:
            self.data_cache[convert_type] = {}

        if orig_item not in self.data_cache[convert_type]:
            self.data_cache[convert_type][orig_item] = self.CONVERSION_FUNCS[convert_type](orig_item)

        return self.data_cache[convert_type][orig_item]

    @staticmethod
    def from_file(filepath):
        with open(filepath, 'r') as fh:
            cfg = yaml.load(fh)
            return DagConfig(cfg)

    def get_executor(self, state):
        return DagExecutor(self, state)


class DagExecutor(object):
    '''This object is responsible for stepping a transaction through a DagConfig
    '''
    def __init__(self, dag_config, req_state):
        self.dag_config = dag_config

        self.context = state.DagExecutionContext(
            dag_config,
            req_state,
        )

    def call_hook(self, hook_name):
        '''Call DAG defined for `hook_name` following the DAG chain as it goes

        This method will alter the `context.options` to match the values for the
        hook we are executing, then it will simply call the DAG, check if another
        DAG should be called (and continue calling DAGs if necessary) until it has
        completed.
        '''
        if hook_name not in self.dag_config.config['hook_dag_map']:
            raise RuntimeError('InvalidHook!! {0} not in {1}'.format(hook_name, self.dag_config.config['hook_dag_map'].keys()))

        # name of the dag to run for the given hook
        hook_meta = self.dag_config.config['hook_dag_map'][hook_name]
        dag = self.dag_config.dags[hook_meta['dag_name']]
        if 'global_option_data_key' in hook_meta:
            self.context.options = self.dag_config.config['global_option_data'][hook_meta['global_option_data_key']]
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

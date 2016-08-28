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
    'bool': bool,
    # since we are using immutable types, we need a more basic check for python
    'list': collections.Sequence,
    'dict': collections.Mapping,
}


class DagNodeType(object):
    '''A node template for the DAG
    '''
    def __init__(self, node_type_config, funcs):
        self.node_type_config = node_type_config

        self.func = funcs[node_type_config['func']]
        self.arg_spec = node_type_config['arg_spec']

    def validate_arg_type(self, arg_name, arg_val):
        # if there is no type defined, we don't check anything
        if 'type' not in self.arg_spec[arg_name]:
            return True
        return isinstance(arg_val, ARG_TYPES[self.arg_spec[arg_name]['type']])


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

        # a dict of all the args that don't need to be resolved each execution
        self.base_resolved_args = {}
        # list of argnames to resolve later
        self.to_resolve_args = []
        for arg_name, arg_spec in self.node_type.arg_spec.iteritems():
            if 'global_option_data_key' in arg_spec:
                self.to_resolve_args.append(arg_name)
            else:
                if not self.node_type.validate_arg_type(arg_name, self.args[arg_name]):
                    raise Exception('{0}: arg {1} not the correct type expected={2} actual={3}'.format(
                        self.__repr__(),
                        arg_name,
                        arg_spec['type'],
                        type(self.args[arg_name]),
                    ))
                self.base_resolved_args[arg_name] = self.args[arg_name]

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
        resolved_args = dict(self.base_resolved_args)
        for arg_name in self.to_resolve_args:
            arg_spec = self.node_type.arg_spec[arg_name]
            resolved_args[arg_name] = context.options[arg_spec['global_option_data_key']][self.args[arg_name]]
            self.node_type.validate_arg_type(arg_name, resolved_args[arg_name])

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
        # TODO: rename to `key`
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

    def __repr__(self):
        return '%s: %s' % (self.__class__.__name__, self.name)

    def __call__(self, context):
        '''Execute the DAG with the given context
        '''
        try:
            context.execution_stack.append(self)
            node = self.starting_node
            # call the control_dag
            path = []
            while True:
                node_ret = node(context)
                # TODO: decide what to do here when the child doesn't exist
                # it'll throw a KeyError, and we can either assume execution
                # ends or raise an exception-- we may want to make this configurable
                # per processing_node
                next_node = node.get_child(node_ret)
                # TODO: change to namedtuple?
                step_ret = {
                    'node': node,
                    'node_ret': node_ret,
                    'next_node': next_node,
                }
                context.execution_path.append(((self.name, node.node_id), step_ret))
                path.append(step_ret)

                # if the node we executed has no children, we need to see if we
                # should run another DAG, if not then we are all done
                if next_node is None:
                    break
                node = next_node
            return path
        finally:
            assert context.execution_stack.pop() == self

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

        # TODO better names...
        self.processing_node_funcs = processing_nodes.load()

        self.processing_node_types = {}
        for k, cfg in self.config['processing_node_types'].iteritems():
            self.processing_node_types[k] = DagNodeType(cfg, self.processing_node_funcs)

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

        # create the context with which we'll execute the DAG
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
        # check that this is a valid hook
        if hook_name not in self.dag_config.config['hook_dag_map']:
            raise RuntimeError('InvalidHook!! {0} not in {1}'.format(hook_name, self.dag_config.config['hook_dag_map'].keys()))

        # name of the dag to run for the given hook
        hook_meta = self.dag_config.config['hook_dag_map'][hook_name]
        dag = self.dag_config.dags[hook_meta['dag_name']]
        if 'global_option_data_key' in hook_meta:
            self.context.options = self.dag_config.config['global_option_data'][hook_meta['global_option_data_key']]
        else:
            self.context.options = {}

        # TODO: check that we haven't run this hook before? at least warn about it?

        return dag(self.context)

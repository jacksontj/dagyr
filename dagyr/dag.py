import copy
import yaml
import logging
import pyrsistent
import collections


import state
import conversion
import processing_function_types


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
    def __init__(self, key, node_type_config, processing_function_types):
        self.key = key
        self.processing_function_type = processing_function_types[node_type_config['processing_function_type']]

        # map of id -> metadatadict
        # inlets can optionally have an "allowed_incoming" section-- to define
        # what things are allowed to connect to the given inlet
        self.inlets = node_type_config['inlets']
        # map of id -> metdatadict (name, returns)
        self.outlets = node_type_config['outlets']

        # this processing_node_type is based on the processing_function_type
        self.node_type_config = copy.deepcopy(self.processing_function_type)
        # we then merge in our configuration on top
        for k, v in node_type_config.iteritems():
            if k in ('arg_spec', 'processing_function_type'):
                continue
            self.node_type_config[k] = v

        self.arg_spec = self.node_type_config['arg_spec']

        # merge the arg_spec
        # we don't allow any new args to be defined, and types cannot be changed
        # if they are defined in the processing_function_type
        if 'arg_spec' in node_type_config:
            # verify that we havent defined new args
            if set(node_type_config['arg_spec']).issubset(set(self.arg_spec)) is not True:
                raise Exception()
            for arg_name, arg_dict in node_type_config['arg_spec'].iteritems():
                for arg_spec_key, arg_spec_val in arg_dict.iteritems():
                    if arg_spec_key == 'type' and 'type' in self.arg_spec[arg_name]:
                        raise Exception()
                    self.arg_spec[arg_name][arg_spec_key] = arg_spec_val

        pyrsistent.freeze(self.node_type_config)
        pyrsistent.freeze(self.arg_spec)

    def validate_arg_type(self, arg_name, arg_val):
        # if there is no type defined, we don't check anything
        if 'type' not in self.arg_spec[arg_name]:
            return True
        return isinstance(arg_val, ARG_TYPES[self.arg_spec[arg_name]['type']])

    # determine if the incoming DAG connection is allowed
    # this is a combination of upstream (node_id, outlet_id) and our (node_id, inlet_id)
    def allowed_incoming(self, other_node, other_outlet_id, inlet_id):
        # if we are attempting to connect to an inlet that doesn't exist-- error
        if inlet_id not in self.inlets:
            raise Exception('Node {0}.{1} is attempting to connect to {3} -- invalid inlet'.format(
                other_node.node_id,
                other_outlet_id,
                inlet_id,
            ))

        if 'allowed_incoming' not in self.inlets[inlet_id]:
            return True

        # Ensure that these nodes are allowed to connect on the inlet/outlet pair
        for incoming_type_id, incoming_outlet in self.inlets[inlet_id]['allowed_incoming']:
            if incoming_type_id == other_node.node_type.key and ((incoming_outlet is None) or (incoming_outlet == other_outlet_id)):
                return True
        return False


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
        # map of return -> (child_id, inlet_id)
        self.children = {}
        for outlet_id, (child_id, inlet_id) in self.outlets.iteritems():
            # check that the types are allowed to connect
            if not nodes[child_id].node_type.allowed_incoming(self, outlet_id, inlet_id):
                raise Exception('Dag {0} node {1}.{2} not allowed to connect to {3}.{4}'.format(
                    self.dag_key,
                    self.node_id,
                    outlet_id,
                    child_id,
                    inlet_id,
                ))
            for r in self.node_type.outlets[outlet_id]['returns']:
                self.children[r] = (nodes[child_id], inlet_id)


    def __repr__(self):
        return str((self.dag_key, self.node_id))

    def __call__(self, context, inlet_id):
        '''Run the node and return
        '''
        resolved_args = dict(self.base_resolved_args)
        for arg_name in self.to_resolve_args:
            arg_spec = self.node_type.arg_spec[arg_name]
            resolved_args[arg_name] = context.options[arg_spec['global_option_data_key']][self.args[arg_name]]
            self.node_type.validate_arg_type(arg_name, resolved_args[arg_name])

        # actually call the processing_function_type
        return self.node_type.processing_function_type['func'](
            context,                    # DagExecutionContext (which includes user context
            {'inlet_id': inlet_id},     # node_context
            self.node_type.arg_spec,    # arg_spec
            self.args,                  # args
            resolved_args,              # resolved_args
        )

    # return pair of (node, inlet_id)
    def get_child(self, key):
        '''Return the next node (if there is one)
        '''
        if not self.children:
            return None, None
        else:
            return self.children[key]


class Dag(object):
    '''Object to represent the whole dag

    This class includes all of the DAG validation, node creation, and the
    method for actually executing the DAG.
    '''
    def __init__(self, key, config, processing_node_types=None):
        self.key = key
        self.nodes = {}

        # temp space for the nodes as we need to create them
        for node_id, node in config['processing_nodes'].iteritems():
            self.nodes[node_id] = DagNode(
                key,
                node_id,
                node,
                processing_node_types,
            )

        # link the children together
        for n in self.nodes.itervalues():
            n.link_children(self.nodes)
        starting_node_id, self.starting_node_inlet = config['starting_node']
        self.starting_node = self.nodes[starting_node_id]

        # verify that the graph is acyclig (thereby making it a DAG)
        if not Dag.is_acyclic([], self.starting_node):
            raise Exception("configured DAG {0} is cyclic!!!".format(key))

        # As a nicety, we'll check that all nodes defined in the list are actually
        # in use in the DAG, if not we'll log a warning, it is not fatal but if
        # the config is generated this might indicate a problem
        used_nodes = Dag.dag_nodes(self.starting_node)
        all_nodes = set(self.nodes.iterkeys())
        if used_nodes != all_nodes:
            log.warning('The following nodes in DAG {0} are not linked: {1}'.format(
                self.key,
                all_nodes - used_nodes,
            ))

    def __repr__(self):
        return '%s: %s' % (self.__class__.__name__, self.key)

    def __call__(self, context):
        '''Execute the DAG with the given context
        '''
        try:
            context.execution_stack.append(self)
            node = self.starting_node
            inlet_id = self.starting_node_inlet
            # call the control_dag
            path = []
            while True:
                node_ret = node(context, inlet_id)
                # TODO: decide what to do here when the child doesn't exist
                # it'll throw a KeyError, and we can either assume execution
                # ends or raise an exception-- we may want to make this configurable
                # per processing_node
                next_node, next_inlet_id = node.get_child(node_ret)
                # TODO: change to namedtuple?
                step_ret = {
                    'node': node,
                    'node_inlet_id': inlet_id,
                    'node_ret': node_ret,
                    'next_node': next_node,
                    'next_node_inlet_id': next_inlet_id,
                }
                context.execution_path.append(((self.key, node.node_id), step_ret))
                path.append(step_ret)

                # if the node we executed has no children, we need to see if we
                # should run another DAG, if not then we are all done
                if next_node is None:
                    break
                node = next_node
                inlet_id = next_inlet_id
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
        for child, _ in node.children.itervalues():
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
        for child, _ in node.children.itervalues():
            ret &= Dag.is_acyclic(path, child)
        return ret

    @staticmethod
    def dag_nodes(node, node_set=None):
        '''Return the list of nodes that are linked in the DAG
        '''
        if node_set is None:
            node_set = set()
        node_set.add(node.node_id)
        for child, _ in node.children.itervalues():
            Dag.dag_nodes(child, node_set=node_set)
        return node_set


# TODO rename to dag_set? or something like that, since its actually the whole config
# holder of the whole DAG config
class Dagyr(object):
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
        self.processing_function_types = processing_function_types.load()

        self.processing_node_types = {}
        for k, cfg in self.config['processing_node_types'].iteritems():
            self.processing_node_types[k] = DagNodeType(k, cfg, self.processing_function_types)

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
            return Dagyr(cfg)

    def get_executor(self, state):
        return DagExecutor(self, state)


class DagExecutor(object):
    '''This object is responsible for stepping a transaction through a Dagyr
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
        try:
            orig_options = self.context.options
            if 'global_option_data_key' in hook_meta:
                self.context.options = self.dag_config.config['global_option_data'][hook_meta['global_option_data_key']]
            else:
                self.context.options = {}

            # TODO: check that we haven't run this hook before? at least warn about it?

            return dag(self.context)
        finally:
            self.context.options = orig_options

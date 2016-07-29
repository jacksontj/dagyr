import yaml
import logging


import fragments


log = logging.getLogger(__name__)


class DagNode(object):
    '''A node in the DAG

    A node in the dag-- otherwise known as a Fragment
    '''
    def __init__(self, node_config):
        # TODO: don't point to the whole thing?
        self.node_config = node_config

        # TODO: get these
        self.fragment_func = getattr(fragments, node_config['fragment_func']).fragment
        self.fragment_spec = node_config['fragment_spec']
        self.fragment_args = node_config['fragment_args']
        self.children = {}
        for k, child_config in node_config.pop('outlet', {}).items():
            self.children[k] = DagNode(child_config)

    def __repr__(self):
        return str(self.node_config)

    # TODO: return None if no children
    def __call__(self, state):
        '''Do whatever it is that you do, and return the next node to execute
        '''
        return self.children[self.fragment_func(state, self.fragment_spec, self.fragment_args)]


# holder of the whole DAG config
class DagConfig(object):
    '''Holder of the Dags!
    This holds a control_dag (effectively a global one) and a mapping of
        namespace -> name -> DAG
    for all the dynamic_dags
    '''
    def __init__(self, config):
        self.config = config

        self.control_dag = DagNode(self.config['control_dag'])
        self.dynamic_dags = {}
        for dynamic_dag_key, dynamic_dag_map in self.config['dynamic_dags'].iteritems():
            self.dynamic_dags[dynamic_dag_key] = {}
            for k, dag_config in dynamic_dag_map.iteritems():
                self.dynamic_dags[dynamic_dag_key][k] = DagNode(dag_config)

    @staticmethod
    def from_file(filepath):
        with open(filepath, 'r') as fh:
            cfg = yaml.load(fh)
            return DagConfig(cfg)


class DagExecutor(object):
    '''This object is responsible for stepping a transaction through a DagConfig
    '''
    def __init__(self, dag_config, request_state):
        self.dag_config = dag_config
        self.request_state = request_state

        self.path = []

    def __call__(self):
        # which node we are executing
        node = self.dag_config.control_dag
        # call the control_dag
        while True:
            try:
                self.path.append(node)
                node = node(self.request_state)
                # TODO: some sort of UUID per transaction to make this log helpful
                log.debug(node)
            except Exception as e:
                # if this DAG points at another one, lets pull that one up and run it
                if self.request_state.next_dag is not None:
                    node = self.dag_config.dynamic_dags[self.request_state.next_dag[0]][self.request_state.next_dag[1]]
                    # TODO: consolidate into a step() method?
                    self.request_state.next_dag = None
                    continue
                # TODO trace level log with the return etc
                if not node.children:
                    break
                log.error('Error executing DAG %s' % node, exc_info=True)
                print self.path
                break

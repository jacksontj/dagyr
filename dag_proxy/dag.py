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

    def __call__(self, state):
        '''Do whatever it is that you do, and return the next node to execute
        if one exists (otherwise return None)
        '''
        ret = self.fragment_func(state, self.fragment_spec, self.fragment_args)
        if not self.children:
            return None
        else:
            return self.children[ret]


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
    def __init__(self, dag_config, ctx):
        self.dag_config = dag_config
        self.context = ctx

        self.path = []

    def __call__(self):
        # which node we are executing
        node = self.dag_config.control_dag
        # call the control_dag
        while True:
            try:
                self.path.append(node)
                ret = node(self.context)
                # if the node we executed has no children, we need to see if we
                # should run another DAG, if not then we are all done
                if ret is None:
                    if self.context.state.next_dag is not None:
                        node = self.dag_config.dynamic_dags[self.context.state.next_dag[0]][self.context.state.next_dag[1]]
                        # TODO: consolidate into a step() method?
                        self.context.state.next_dag = None
                        continue
                    break
                node = ret
                # TODO: some sort of UUID per transaction to make this log helpful
            except Exception as e:
                log.error('Error executing DAG %s' % node, exc_info=True)
                print self.path
                break

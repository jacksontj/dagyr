import pyrsistent
import copy


class DagExecutionContext(object):
    '''Context available to the DagExecutor

    This is the context used by the DagExecutor and passed to the processing_nodes
    '''

    def __init__(self, dag_config, state):
        # TODO: remove? We only want this so that the data caching is attached
        # to the version of config, might be better to not give access to the
        # whole thing since anything here or down is in the getattr/setattr through
        # dotted notation
        self.dag_config = dag_config

        # what DAG we are executing
        self.dag = None

        # which node we are on
        self.node = None

        # TODO: some sort of switching thing?? this is set on a per-hook basis,
        # only in here since we have the get_dotted stuff
        self.options = {}
        # request state
        self.state = state
        # temporary storage space (attached to this transaction)
        self.tmp = {}

        # if not None, this is the next DAG to run
        self.next_dag = None

    def getattr_dotted(self, ident):
        '''Return the value for something in our namespace given dot notation
        '''
        ident_parts = ident.split('.')

        thing = self
        for p in ident_parts:
            try:
                thing = getattr(thing, p)
            except AttributeError:
                thing = thing[p]

        return thing

    def setattr_dotted(self, ident, val):
        '''Set the value for something in our namespace given dot notation
        '''
        ident_parts = ident.split('.')

        # TODO: put in some protected namespace? frozen.dag_config ??
        # some very VERY basic checking, attempting to not let the DAG set
        # immutable things
        if ident_parts[0] in ('dag_config', 'options', 'dag', 'node'):
            raise Exception('Not allowed to set those!!')

        thing = self
        # get to the correct thing, so we can do appropriate sets
        for p in ident_parts[:-1]:
            try:
                thing = getattr(thing, p)
            except AttributeError:
                thing = thing[p]

        try:
            setattr(thing, ident_parts[-1], val)
        except AttributeError:
            thing[ident_parts[-1]] = val

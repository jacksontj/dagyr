'''
The request/response state that is passed to all fragments
'''
import copy


import tornado.httpclient


class Context(object):
    '''Context available to processing_nodes
    '''
    def __init__(self, options, state):
        # pointer to the global config
        self.options = options
        self.state = state
        self.tmp = {}

    def getattr_dotted(self, ident):
        '''Return the value for something in our namespace given dot notation
        '''
        ident_parts = ident.split('.')

        thing = self
        # get to the correc thing, so we can do appropriate sets
        for p in ident_parts[:-1]:
            try:
                thing = getattr(thing, p)
            except AttributeError:
                thing = thing[p]

        return thing[ident_parts[-1]]

    def setattr_dotted(self, ident, val):
        '''Set the value for something in our namespace given dot notation
        '''
        ident_parts = ident.split('.')

        thing = self
        # get to the correc thing, so we can do appropriate sets
        for p in ident_parts[:-1]:
            try:
                thing = getattr(thing, p)
            except AttributeError:
                thing = thing[p]

        thing[ident_parts[-1]] = val


class RequestState(object):
    '''Encapsulate the proxy state

    This includes:
        - pristine_request
        - request
        - pristine_response
        - response
    '''
    def __init__(self, request):
        self.pristine_request = {
            'header': dict(request.headers),
            'path': request.path,
        }
        self.request = copy.deepcopy(self.pristine_request)

        self.pristine_response = {}
        self.response = {}

        # TODO: move to some other state object?
        # if set, it is (dag_namespace, dag_key)
        self.next_dag = None

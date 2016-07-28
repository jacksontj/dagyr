'''
The request/response state that is passed to all fragments
'''
import copy


import tornado.httpclient


class RequestState(object):
    '''Encapsulate the proxy state

    This includes:
        - pristine_request
        - request
        - pristine_response
        - response
        - tmp storage space
    '''
    def __init__(self, request):
        self.pristine_request = {
            'header': dict(request.headers),
            'path': request.path,
        }
        self.request = copy.deepcopy(self.pristine_request)

        self.pristine_response = {}
        self.response = {}

        # TODO: reset on each node?
        self.tmp = {}

        # if set, it is (dag_namespace, dag_key)
        self.next_dag = None

    def getsubattr(self, ident):
        ident_parts = ident.split('.')

        thing = self
        # get to the correc thing, so we can do appropriate sets
        for p in ident_parts[:-1]:
            try:
                thing = getattr(thing, p)
            except AttributeError:
                thing = thing[p]

        return thing[ident_parts[-1]]

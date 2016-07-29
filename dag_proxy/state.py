'''
The request/response state that is passed to all fragments
'''
import tornado.httpclient
import copy

class Context(object):
    '''Context available to processing_nodes
    '''
    def __init__(self, options, state):
        # pointer to the global config
        self.options = options
        self.state = state
        self.tmp = {}

        # TODO: ordereddict (so that hook execution order isn't assumed)
        # hook_name -> path
        self.dag_path = {}

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

        try:
            setattr(thing, ident_parts[-1], val)
        except AttributeError:
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
            'headers': dict(request.headers),
            'path': request.path,
        }
        # TODO: namespace? also want to include options such as
        # follow redirects, timeouts, etc. (basically transaction overrideable
        # configuration)
        self.request = copy.deepcopy(self.pristine_request)

        self.pristine_response = {}
        self.response = {}

        # TODO: move to some other state object?
        # if set, it is (dag_namespace, dag_key)
        self.next_dag = None

    def get_request(self):
        '''Return tornado.httpclient.HTTPRequest version of `request`
        '''
        return tornado.httpclient.HTTPRequest(
            'http://{host}{path}'.format(
                host=self.request['headers']['Host'],
                path=self.request['path'],
            ),
            headers=self.request['headers'],
        )

    def set_response(self, response):
        if self.pristine_response != {}:
            raise Exception('Response already set???')

        self.pristine_response = {
            'code': response.code,
            'headers': dict(response.headers),
            'body': response.body,  # TODO: stream?
        }
        self.response = copy.deepcopy(self.pristine_response)

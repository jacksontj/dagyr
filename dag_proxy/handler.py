import yaml
import logging

import tornado.ioloop
import tornado.web
import tornado.gen
import tornado.httpclient

import dag
import state

log = logging.getLogger(__name__)


class DagHandler(tornado.web.RequestHandler):
    '''Proxy handler that just executes the DAGs associated to make the request
    '''

    @tornado.gen.coroutine
    def prepare(self):
        '''Create our own DAGRunner, which will point at a dag config
        '''
        req_state = state.RequestState(self.request)
        ctx = state.Context(
            {},  # TODO: pointer to options
            req_state,
        )

        # get an executor
        dag_executor = dag.DagExecutor(
            self.application.dag_config,
            ctx,
        )
        # execute it!
        # TODO: change to each "hook" ingress/egress
        dag_executor.call_hook('ingress')

        # TODO: better conversion
        # if the response is set, return it
        if req_state.response != {}:
            self.serve_state(req_state.response)
            return

        # TODO: make downstream request
        # CONTINUE!!
        http_client = tornado.httpclient.AsyncHTTPClient()
        ret = yield http_client.fetch(dag_executor.context.state.get_request())

        dag_executor.context.state.set_response(ret)

        # call egress hook
        dag_executor.call_hook('egress')

        # set context.state.response as response
        self.serve_state(req_state.response)
        return

    def serve_state(self, state):
        if 'code' in state:
            self.set_status(state['code'])

        if 'headers' in state:
            for k, v in state['headers'].iteritems():
                if k in ('Content-Length',):
                    continue
                self.set_header(k, v)

        if 'body' in state:
            self.write(state['body'])

        self.finish()

    # TODO: make the request defined in request_state
    @tornado.gen.coroutine
    def get(self):
        http_client = tornado.httpclient.AsyncHTTPClient()
        resp = yield http_client.fetch("http://www.google.com/")
        self.write("Got a %d from origin" % resp.code)

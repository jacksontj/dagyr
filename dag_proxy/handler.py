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

    def prepare(self):
        '''Create our own DAGRunner, which will point at a dag config
        '''
        req_state = state.RequestState(self.request)
        # get an executor
        dag_executor = dag.DagExecutor(
            self.application.dag_config,
            req_state,
        )
        # execute it!
        # TODO: change to each "hook" ingress/egress
        dag_executor()

        # TODO: better conversion
        # if the response is set, return it
        if req_state.response != {}:
            if 'body' in req_state.response:
                self.write(req_state.response['body'])

            if 'code' in req_state.response:
                self.set_status(req_state.response['code'])

            self.finish()

        # TODO: make downstream request
        # CONTINUE!!


    # TODO: make the request defined in request_state
    @tornado.gen.coroutine
    def get(self):
        http_client = tornado.httpclient.AsyncHTTPClient()
        resp = yield http_client.fetch("http://www.google.com/")
        self.write("Got a %d from origin" % resp.code)

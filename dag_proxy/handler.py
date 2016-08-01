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
        dag_executor.call_hook('ingress')

        # TODO: run egress DAG
        # TODO: better conversion
        # if the response is set, return it
        if req_state.response != {}:
            self.serve_state(req_state.response)
            return

        # make downstream request
        http_client = tornado.httpclient.AsyncHTTPClient()
        try:
            ret = yield http_client.fetch(ctx.state.get_request())
        except tornado.httpclient.HTTPError as e:
            ret = e.response

        ctx.state.set_response(ret)

        # call egress hook
        dag_executor.call_hook('egress')

        # set context.state.response as response
        self.serve_state(req_state.response)
        return  # so we don't call other handlers

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

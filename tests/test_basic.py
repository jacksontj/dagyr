import tornado.web
import tornado.ioloop

import tornado.testing


import dag_proxy.dag
import dag_proxy.handler


class TestBasic(tornado.testing.AsyncHTTPTestCase):
    # TODO: consolidate into a method in the lib
    def get_app(self):
        # only a single handler-- that does all the DAG magic
        app = tornado.web.Application(
            [(r"/.*", dag_proxy.handler.DagHandler)],
            debug=True,
        )

        # TODO: config? autoreload is only nice for development and debugging
        config_file = 'config.yaml'
        # load initial config file
        app.dag_config = dag_proxy.dag.DagConfig.from_file(config_file)
        return app

    def test_not_matching_domain(self):
        response = self.fetch(
            '/',
            headers={'Host': 'foo.com'},
        )
        self.assertEqual(response.code, 404)
        self.assertEqual(response.body, 'not found!')

    def test_matching_domain_not_matching_path(self):
        response = self.fetch(
            '/',
            headers={'Host': 'localhost:8888'},
        )
        self.assertEqual(response.code, 404)
        self.assertEqual(response.body, 'not found!')

    def test_matching_domain_matching_path(self):
        response = self.fetch(
            '/foo',
            headers={'Host': 'localhost:8888'},
        )
        self.assertEqual(response.code, 200)
        self.assertEqual(response.body, 'found!')

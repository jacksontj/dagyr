import yaml

import logging
logging.basicConfig(level=logging.INFO)

import tornado.web

import dag_proxy.handler
import dag_proxy.dag


if __name__ == "__main__":

    # only a single handler-- that does all the DAG magic
    app = tornado.web.Application(
        [(r"/.*", dag_proxy.handler.DagHandler)],
        debug=True,
    )

    # TODO: out of band
    # load config file
    with open('config.yaml', 'r') as fh:
        cfg = yaml.load(fh)
        app.dag_config = dag_proxy.dag.DagConfig(cfg)


    app.listen(8888)
    tornado.ioloop.IOLoop.current().start()

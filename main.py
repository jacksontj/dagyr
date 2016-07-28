import yaml
import functools
import os

import logging
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

import tornado.web
import tornado.ioloop

import dag_proxy.handler
import dag_proxy.dag


def _reload_config_on_update(app, filepath, modify_times):
    try:
        modified = os.stat(filepath)
    except Exception:
        return

    if filepath not in modify_times or modified != modify_times[filepath]:
        log.debug("config change, reloading config")
        app.dag_config = dag_proxy.dag.DagConfig.from_file(config_file)
        modify_times[filepath] = modified


if __name__ == "__main__":
    ioloop = tornado.ioloop.IOLoop.current()


    # only a single handler-- that does all the DAG magic
    app = tornado.web.Application(
        [(r"/.*", dag_proxy.handler.DagHandler)],
        debug=True,
    )

    # TODO: config? autoreload is only nice for development and debugging
    config_file = 'config.yaml'
    modify_times = {}
    scheduler = tornado.ioloop.PeriodicCallback(
        functools.partial(_reload_config_on_update, app, config_file, modify_times),
        500,  # how often to call this
        io_loop=ioloop,
    )
    scheduler.start()

    # load initial config file
    app.dag_config = dag_proxy.dag.DagConfig.from_file(config_file)


    app.listen(8888)
    ioloop.start()

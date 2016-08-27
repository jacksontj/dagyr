import unittest

import dagyr.dag


class TestBasic(unittest.TestCase):
    def setUp(self):
        # TODO: helper method to load files
        self.dagyr_config = dagyr.dag.DagConfig.from_file(
            '/home/jacksontj/src/dagyr/tests/files/basic.yaml',
        )

    def executor_path(self, e, hook='ingress'):
        '''Condense the DAG path into a list of [(hook, [idlist])]
        '''
        path = []
        for hook, dag_path in e.hook_path_map[hook].iteritems():
            hook_path = []
            for h in dag_path:
                hook_path.append(h['node'].node_id)
            path.append((hook, hook_path))
        return path

    def test_paths(self):
        # TODO: this doesn't get paths across dags-- just the one
        r = dagyr.dag.Dag.all_paths(self.dagyr_config.dags['ingress'].starting_node)
        self.assertEqual(
            r,
            [[1,3,4,5], [1,2]],
        )

    def test_12(self):
        state = {
            'host': 'b.com',
        }

        # get an executor
        dag_executor = self.dagyr_config.get_executor(state)
        # execute it!
        dag_executor.call_hook('ingress')

        # make sure we got the results we wanted
        self.assertEqual(
            state,
            {
                'response_body': 'found!',
                'host': 'b.com',
                'response_code': 200,
            },
        )

        # ensure we went the path we expected
        self.assertEqual(
            self.executor_path(dag_executor),
            [('ingress', [1, 2]), ('dynamic_domain_b.com', [1,2])],
        )

    def test_134(self):
        state = {
            'host': 'notavalidhostname',
        }

        # get an executor
        dag_executor = self.dagyr_config.get_executor(state)
        # execute it!
        dag_executor.call_hook('ingress')

        # make sure we got the results we wanted
        self.assertEqual(
            state,
            {
                'ebool_count': 3,
                'response_body': 'not found!',
                'host': 'notavalidhostname',
                'response_code': 404,
            },
        )

        # ensure we went the path we expected
        self.assertEqual(
            self.executor_path(dag_executor),
            [('ingress', [1, 3, 4, 5])],
        )

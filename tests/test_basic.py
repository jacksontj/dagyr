import unittest

import dagyr.dag


class TestBasic(unittest.TestCase):
    def setUp(self):
        # TODO: helper method to load files
        self.dagyr_config = dagyr.dag.Dagyr.from_file(
            '/home/jacksontj/src/dagyr/tests/files/basic.yaml',
        )

    def _condense_path(self, path):
        '''Condense the DAG path into a list of [(hook, [idlist])]
        '''
        condensed_path = []
        for node_map in path:
            condensed_path.append(node_map['node'].node_id)
        return condensed_path

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
        dag_path = dag_executor.call_hook('ingress')

        # make sure we got the results we wanted
        self.assertEqual(
            state,
            {
                'response_body': 'found!',
                'host': 'b.com',
                'response_code': 200,
            },
        )

        self.assertEqual(
            self._condense_path(dag_path),
            [1,2],
        )
        # ensure we went the path we expected
        self.assertEqual(
            [x[0] for x in dag_executor.context.execution_path],
            [('ingress', 1), ('dynamic_domain_b.com', 1), ('dynamic_domain_b.com', 2), ('ingress', 2)],
        )

    def test_1345(self):
        state = {
            'host': 'notavalidhostname',
        }

        # get an executor
        dag_executor = self.dagyr_config.get_executor(state)
        # execute it!
        dag_path = dag_executor.call_hook('ingress')

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
            [x[0] for x in dag_executor.context.execution_path],
            [('ingress', 1), ('ingress', 3), ('eventual_bool', 1), ('eventual_bool', 1), ('eventual_bool', 1), ('eventual_bool', 1), ('ingress', 4), ('ingress', 5)],
        )

        self.assertEqual(
            self._condense_path(dag_path),
            [1,3,4,5],
        )

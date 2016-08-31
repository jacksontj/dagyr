'''An example to repeatedly call a processing_node up to N times waiting for a specific return
'''

def iterate_until(context, arg_spec, raw_args, resolved_args):
    '''A processing_node

    Arguments:
        context: object that encapsulates context (this is Dagyr context as well as user-context)
        arg_spec: the specification for what each argument is (including name, type, etc.)
        raw_args: raw arguments from configuration, not resolved through global_option_data
        resolved_args: arguments after being resolved through global_option_data (if applicable)
    '''
    for x in xrange(0, raw_args['iterate_limit']):
        # TODO: less messy... this is a bit convoluted
        dag_path = context.dag_config.dags[raw_args['dag_key']](context)
        # TODO: do something with the path
        r = dag_path[-1]['node_ret']
        if r == raw_args['expected_value']:
            return True
    return False

processing_function_type = {
    'name': 'iterate_until',
    'info': 'iterate_until',
    'func': iterate_until,
    'arg_spec': {
        # this is a thing that will be passed a key to lookup the value
        'dag_key': {
            'name': 'key of DAG to run',
            'info': 'key of DAG to run',
        },
        'iterate_limit': {
            'name': 'max times we\'ll call',
            'info': 'max times we\'ll call',
            'type': 'int',
        },
        'expected_value': {
            'name': 'what return we want',
            'info': 'what return we want',
        },
    },
}

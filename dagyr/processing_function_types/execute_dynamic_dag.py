'''Set the next DAG to execute
'''

def execute_dynamic_dag(context, arg_spec, raw_args, resolved_args):
    key = '{0}_{1}'.format(
        resolved_args['dag_prefix'],
        context.getattr_dotted(resolved_args['suffix_key']),
    )
    dag_path = context.dag_config.dags[key](context)

processing_function_type = {
    'name': 'execute_dynamic_dag',
    'info': 'execute another dag',
    'func': execute_dynamic_dag,
    'arg_spec': {
        # this is a thing that will be passed a key to lookup the value
        'dag_prefix': {
            'name': 'a readable name',
            'info': 'string prefix',
            'type': 'string',
        },
        # this just gets a value-- just use it
        'suffix_key': {
            'name': 'a readable name',
            'info': 'list of values to match against',
            'type': 'string',
        },
    },
}

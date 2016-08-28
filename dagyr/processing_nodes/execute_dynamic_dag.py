'''Set the next DAG to execute
'''

def processing_node(context, arg_spec, raw_args, resolved_args):
    key = '{0}_{1}'.format(
        resolved_args['dag_prefix'],
        context.getattr_dotted(resolved_args['suffix_key']),
    )
    dag_path = context.dag_config.dags[key](context)

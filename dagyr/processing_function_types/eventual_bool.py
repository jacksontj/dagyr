'''Change values after a certain number of calls

This node will return `starting_value` until it has been called `change_count` times
at which point it will start returning `eventual_value`. The counter storing how
many times this node has been called will be stored in `counter_location`

'''

def eventual_bool(context, node_context, arg_spec, raw_args, resolved_args):
    count = raw_args['starting_value']
    try:
        count = context.getattr_dotted(raw_args['counter_location'])
    except:
        pass
    if count < raw_args['change_count']:
        count += 1
        context.setattr_dotted(raw_args['counter_location'], count)
        return raw_args['starting_value']
    else:
        return raw_args['eventual_value']

# spec of what this processing_function does
processing_function_type = {
    'name': 'eventual_bool',
    'info': 'eventual_bool',
    'func': eventual_bool,
    'arg_spec': {
        'counter_location': {
            'name': 'where to store the counter',
            'info': 'where to store the counter',
            'type': 'string',
        },
        'starting_value': {
            'name': 'starting_value',
            'info': 'what value to start as',
            'type': 'bool',
        },
        'eventual_value': {
            'name': 'eventual_value',
            'info': 'what value to end up as',
        },
        'change_count': {
            'name': 'change count',
            'info': 'what count to change at',
            'type': 'int',
        },
    },
}

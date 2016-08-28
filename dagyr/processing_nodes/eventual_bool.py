'''Change values after a certain number of calls

This node will return `starting_value` until it has been called `change_count` times
at which point it will start returning `eventual_value`. The counter storing how
many times this node has been called will be stored in `counter_location`

'''

def processing_node(context, arg_spec, raw_args, resolved_args):
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

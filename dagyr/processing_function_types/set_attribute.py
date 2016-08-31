'''Set `attribute` (in context) to `value`
'''
import logging

def set_attribute(context, arg_spec, raw_args, resolved_args):
    try:
        context.setattr_dotted(resolved_args['attribute'], resolved_args['value'])
        return True
    except:
        logging.error('Unable to set attribute', exc_info=True)
        return False

processing_function_type = {
    'name': 'set_attribute',
    'info': 'set an attribute',
    'func': set_attribute,
    'arg_spec': {
        # this is a thing that will be passed a key to lookup the value
        'attribute': {
            'name': 'a readable name',
            'info': 'name of the attribute to match',
            'type': 'string',
        },
        # this just gets a value-- just use it
        'value': {
            'name': 'what to set it to',
            'info': 'what value to set',
        },
    },
}

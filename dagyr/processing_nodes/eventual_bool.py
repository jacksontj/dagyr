'''Be false until called a few times
'''

KEY = 'state.ebool_count'

# TODO: get args to this!
def processing_node(context, arg_spec, raw_args, resolved_args):
    count = 0
    try:
        count = context.getattr_dotted(KEY)
    except:
        pass
    if count < 3:
        count += 1
        context.setattr_dotted(KEY, count)
        print 'false', count
        return False
    else:
        print 'true', count
        return True

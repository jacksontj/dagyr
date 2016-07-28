

def fragment(request_state, frag_spec, frag_args):
    '''A fragment
    request_state: object that encapsulates request, response, pristine versions, and some temp space
    frag_spec: some metadata that defines what the frag_args might be (lookasides, types, etc.)
    frag_args: arguments
    '''
    attr_parts = frag_args['attribute'].split('.')
    # we only allow setting specific things (to be good citizens)
    if attr_parts[0] != 'state':
        return False

    # if we are setting pristine things, bail
    if len(attr_parts) >= 2 and attr_parts[1] in ('pristine_request', 'pristine_response'):
        return False

    # otherwise we'll assume all is well
    thing = request_state
    # get to the correc thing, so we can do appropriate sets
    for p in attr_parts[1:-1]:
        thing = getattr(thing, p)
    # set the value we want
    thing[attr_parts[-1]] = frag_args['value']
    # since we succeeded-- return true
    return True

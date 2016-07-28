

def fragment(request_state, frag_spec, frag_args):
    '''A fragment
    request_state: object that encapsulates request, response, pristine versions, and some temp space
    frag_spec: some metadata that defines what the frag_args might be (lookasides, types, etc.)
    frag_args: arguments
    '''
    request_state.next_dag = (
        frag_args['dynamic_dag_namespace'],
        request_state.getsubattr(frag_args['dynamic_dag_key']),
    )

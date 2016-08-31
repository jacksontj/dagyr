import os
import os.path
import imp


def load():
    '''Load all of the modules, and return a dict of key -> {}
    '''
    plugins = {}
    plugin_dir = os.path.dirname(__file__)
    for filename in os.listdir(plugin_dir):
        if not filename.endswith('.py'):
            continue
        if filename.startswith('__'):
            continue
        file_path = os.path.join(
            plugin_dir,
            filename,
        )
        module_name = filename.rstrip('.py')
        mod = imp.load_source(module_name, file_path)
        try:
            plugins[module_name] = mod.processing_function_type
        except AttributeError:
            continue

    return plugins

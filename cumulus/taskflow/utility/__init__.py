import os
import pkgutil
import pkg_resources as pr
import sys

import cumulus

def find_modules(paths, prefix=''):
    for (loader, name, pkg) in pkgutil.iter_modules(paths):
        if pkg:
            package_dir = os.path.join(loader.path, name)
            for module in find_modules([package_dir], prefix='%s%s.' % (prefix, name)):
                yield module
        else:
            yield '%s%s' % (prefix,  name)

def find_taskflow_modules():
    # There has to be a better way!
    base_path = os.path.dirname(
                    pr.resource_filename(cumulus.__name__, '__init__.py'))
    base_path = os.path.abspath(
                    os.path.join(base_path, '..'))
    paths = []
    if 'taskflow' not in cumulus.config:
        print 'WARN: No taskflow path set.'
    else:
        for path in cumulus.config.taskflow.path:
            # If we are not dealing with full path treat as relative to install
            # tree
            if path[0] != '/':
                path = os.path.abspath(
                    os.path.join(base_path, path))

            paths.append(path)
            sys.path.append(path)
    return find_modules(paths)

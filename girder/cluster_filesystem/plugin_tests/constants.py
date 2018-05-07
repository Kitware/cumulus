import stat

CLUSTER_LOAD = {
    '_id': 'dummy',
    'type': 'newt',
    'name': 'eyeofnewt',
    'config': {
        'host': 'cori'
    }
}

PATH = '/a/b/c'

DIR_ID_FIELDS = {
    'clusterId': 'dummy',
    'path': PATH
}

CURR_DIR = {
    'name': '.',
    'group': 'group',
    'user': 'user',
    'mode': stat.S_IFDIR,
    'date': 'Feb  7 16:26',
    'size': 0
}

PARENT_DIR = {
    'name': '..',
    'group': 'group',
    'user': 'user',
    'mode': stat.S_IFDIR,
    'date': 'Feb  7 16:26',
    'size': 0
}

DIR1 = {
    'name': 'dir1',
    'group': 'group',
    'user': 'user',
    'mode': stat.S_IFDIR,
    'date': 'Feb  7 16:26',
    'size': 0
}

DIR2 = {
    'name': 'dir2',
    'group': 'group',
    'user': 'user',
    'mode': stat.S_IFDIR,
    'date': 'Feb  7 16:26',
    'size': 0
}

DIR3 = {
    'name': 'dir3',
    'group': 'group',
    'user': 'user',
    'mode': stat.S_IFDIR,
    'date': 'Feb  7 16:26',
    'size': 0
}

FILE1 = {
    'name': 'file1',
    'group': 'group',
    'user': 'user',
    'mode': stat.S_IFREG,
    'date': 'Feb  7 16:26',
    'size': 123
}

FILE2 = {
    'name': 'file2',
    'group': 'group',
    'user': 'user',
    'mode': stat.S_IFREG,
    'date': 'Feb  7 16:26',
    'size': 456
}

FILE3 = {
    'name': 'file3',
    'group': 'group',
    'user': 'user',
    'mode': stat.S_IFREG,
    'date': 'Feb  7 16:26',
    'size': 78910
}

FILE_ID_FIELDS = {
    'clusterId': 'dummy',
    'path': "%s/%s" % (PATH, FILE1['name'])
}

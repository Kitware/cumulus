from jsonpath_rw import parse


def get_property(path, doc, default=None):
    prop = parse(path).find(doc)
    if prop:
        prop = prop[0].value
    else:
        prop = default

    return prop

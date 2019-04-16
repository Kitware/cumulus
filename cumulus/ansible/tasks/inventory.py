import re
import tempfile
import os
import json
from contextlib import contextmanager
from collections import OrderedDict
import six


class AnsibleInventoryHost(object):
    """
    Represents an Ansible inventory host and its associated variables.
    Implements low level functions for reading and writing the host to
    an inventory file. Note:  This does NOT model an actual host,  but
    rather a host line in an Ansible inventory file. It may represent
    multiple hosts in contexts where pattern matching is used (e.g.
    where a host declared as 'www[01:50].example.com').
    """
    def __init__(self, host, **kwargs):
        self.host = host
        self.variables = OrderedDict(sorted(kwargs.items()))

    def to_string(self):
        s = self.host
        if self.variables:
            s += ' '
            s += ' '.join(['%s=%s' % (k, v)
                           for k, v in self.variables.items()])
        return s

    @staticmethod
    def from_string(s):
        parts = s.rstrip().split()
        host = parts[0]
        kwargs = {}
        for part in parts[1:]:
            try:
                key = part.split('=')[0]
                value = part.split('=')[1]
                if bool(key) and bool(value):
                    kwargs[key] = value
                else:
                    raise RuntimeError('Could not parse %s for host %s' %
                                       (part, host))
            except IndexError:
                raise RuntimeError('Could not parse %s for host %s' %
                                   (part, host))
        return AnsibleInventoryHost(host, **kwargs)

    def __eq__(self, other):
        return self.host == other.host and self.variables == other.variables


class AnsibleInventorySection(object):
    """
    Abstract class that represents a config section in an Ansible inventory
    script.
    """
    def __init__(self, heading, items=None):
        heading = heading.strip()
        heading = '[' + heading if heading[0] != '[' else heading
        heading = heading + ']' if heading[-1] != ']' else heading

        self.heading = heading

        self.items = items if items is not None else []

    @property
    def name(self):
        raise NotImplementedError('Must be implemented by subclass')

    @name.setter
    def name(self, value):
        raise NotImplementedError('Must be implemented by subclass')

    @staticmethod
    def treat(line):
        raise NotImplementedError('Must be implemented by subclass')

    def append(self, item):
        self.items.append(item)


class AnsibleInventoryGroup(AnsibleInventorySection):
    """
    Class that represents a group section in an Ansible inventory script
    """

    def __init__(self, heading, items=None):
        super(AnsibleInventoryGroup, self).__init__(heading, items)
        self.items = [AnsibleInventory.as_host(item) for item in self.items]

    @staticmethod
    def treat(line):
        return True if line.startswith('[') \
            and ':' not in line else False

    @property
    def name(self):
        return self.heading[1:-1]

    @name.setter
    def name(self, value):
        self.heading = '[%s]' % value

    def to_string(self):
        s = '%s\n' % self.heading
        s += '\n'.join([i.to_string() for i in self.items]) + '\n'
        return s


# Note:  does not currently implement group vars,  or groups of groups
#        See: http://docs.ansible.com/ansible/intro_inventory.html for more
#        info on these features.
class AnsibleInventory(object):
    """
    Represents an Ansible inventory script. It reads and writes an ini-like
    file in the style of an Ansible inventory.
    """

    # Could add classes for AnsibleInventoryGroupVars and
    # AnsibleInventoryGroupOfGroups here if these features become
    # importaint
    section_classes = [AnsibleInventoryGroup]

    # Empty line or whitespace or starts with #
    ignore_lines = re.compile(r'^\s+$|^#')

    @staticmethod
    def as_host(val):
        try:
            val.to_string()
            return val
        except AttributeError:
            return AnsibleInventoryHost.from_string(val)

    def __init__(self, global_hosts,  sections=None):
        self.global_hosts = [AnsibleInventory.as_host(h)
                             for h in global_hosts]
        self.sections = sections if sections is not None else []

    @staticmethod
    def from_string(inventory):
        sections = []
        current = global_hosts = []

        for line in inventory.split('\n'):
            # Ignore comments and empty lines
            if line == '' or AnsibleInventory.ignore_lines.match(line):
                continue

            # Sentinal that asks 'are we on a new section heading?'
            # Assume for each line that this is false
            treated = False

            # Ask easy of our section classes if it can treat this
            # particular line
            for section_class in AnsibleInventory.section_classes:
                if section_class.treat(line):
                    treated = True
                    break

            # If we have a treatable line
            if treated:

                # If current isn't currently equal to global_hosts
                # Then current is a section and should be appended
                # to sections.
                if id(current) != id(global_hosts):
                    sections.append(current)

                # Set current to the new section
                current = section_class(line)
            else:
                # we've just got content
                current.append(
                    AnsibleInventoryHost.from_string(line.rstrip()))

        # Make sure we append the last section (assuming it is
        # not global_hosts)
        if id(current) != id(global_hosts):
            sections.append(current)

        return AnsibleInventory(global_hosts, sections)

    @staticmethod
    def from_file(path):
        with open(path, 'rb') as fh:
            inventory = fh.read()
        return AnsibleInventory.from_string(inventory)

    def to_string(self):
        s = ''
        for host in self.global_hosts:
            s += host.to_string() + '\n'

        if self.sections:
            s += '\n'
            s += '\n'.join([section.to_string()
                            for section in self.sections]) + '\n'

        return s

    def to_file(self, path):
        with open(path, 'wb') as fh:
            fh.write(self.to_string())

    # Note: from_json expects json to be in a format that is ammenable
    #       to dynamic inventories. e.g:
    #       http://docs.ansible.com/ansible/developing_inventory.html
    #       This is fundimentally less expressive than the ini style
    #       config - this means converting from an ini config to json,
    #       then from json back to an ini style config IS NOT LOSSLESS
    @staticmethod
    def from_json(json_string):
        sections = []
        global_hosts = []

        d = json.loads(json_string)

        for key, items in sorted(d.items()):
            if key != '_meta':
                g = AnsibleInventoryGroup(key)
                for host in items:
                    g.items.append(AnsibleInventoryHost(host))
                sections.append(g)
            else:
                for host, variables in sorted(items['hostvars'].items()):
                    global_hosts.append(
                        AnsibleInventoryHost(host, **variables))

        return AnsibleInventory(global_hosts, sections)

    def to_json(self, with_meta=True):
        d = OrderedDict(_meta=OrderedDict(hostvars=OrderedDict())) \
            if with_meta else OrderedDict()
        for host in self.global_hosts:
            if with_meta and host.variables:
                d['_meta']['hostvars'][host.host] = host.variables

        for sec in self.sections:
            d[sec.name] = []
            if isinstance(sec, AnsibleInventoryGroup):
                for host in sec.items:
                    # Note: Ignores variables here
                    #       just appends the host name
                    d[sec.name].append(host.host)
                    if with_meta:
                        if host.host in d['_meta']['hostvars']:
                            # Its not clear whether this should be an update
                            # or an overwrite from the ansible documentation.
                            # It doesn't appear that the dynamic inventory
                            # allows for hosts in specific groups to override
                            # or change hostvars based on a specific group,
                            # e.g.:
                            # ------
                            # localhost foo=bar
                            #
                            # [section1]
                            # localhost foo=baz
                            #
                            # [section2]
                            # locahost bar=baz
                            #
                            # ------
                            # Should localhost's vars be:
                            #
                            #  {'foo': 'bar'},  <= Only accept global variables
                            #  {'foo': 'bar', 'bar': 'baz'} <= Add missing but
                            #                                    do not update.
                            #  {'foo': 'baz', 'bar': 'baz'} <= Merge w/ update
                            #
                            # This script is implemented with a merge w/update
                            # strategy (because it is easy to implement), the
                            # 'correct' strategy is unclear when generating
                            # dynamic inventories.
                            d['_meta']['hostvars'][host.host].update(
                                host.variables)
                        else:
                            d['_meta']['hostvars'][host.host] = host.variables

        return json.dumps(d)

    @contextmanager
    def to_tempfile(self):
        _, path = tempfile.mkstemp()

        with open(path, 'wb') as fh:
            fh.write(self.to_string().encode('utf8'))

        yield path

        os.remove(path)


def simple_inventory(a, b=None):
    """Generate an inventory object from a list of arguments

    This is a utility function designed to generate an AnsibleInventory
    object from simple python built-in types. It is intended to cover
    common use cases.  If simple_inventory does not meet your needs please
    use the AnsibleInventory object and related classes (e.g.
    AnsibleInventoryGroup) instead. simple_inventory takes one required
    argument (a) and one optional argument (b).

    If 'a' is a string and 'b' is not included we assume a single global host

    if 'a' is a list and 'b' is not included we assume a list of global hosts

    if 'a' is a dict and 'b' is not included we assume keys are group names
        and that values are lists of hosts to include in that group

    if 'a' is a string and 'b' is a dict we assume that 'a' is a single global
        host and that 'b' is a dict where keys are groups,  and values are
        lists of hosts in those groups.

    if 'a' is a list and 'b' is a dict we assume that 'a' is a list of global
        hosts and that 'b' is a dict where keys are groups, and values are
        lists of hosts in those groups.

    :param a: first argument
    :type a: string, list, dict
    :param b: list, dict, NoneType
    :returns: an ansible inventory object
    :rtype: AnsibleInventoryObject

    """
    # Simple string,  assume a single global host
    if isinstance(a, six.string_types) and b is None:
        return AnsibleInventory([a])

    # List of items,  assume a list of global hosts
    if isinstance(a, list) and b is None:
        return AnsibleInventory(a)

    # Assume a dict of group:list(hosts)
    if isinstance(a, dict):
        return AnsibleInventory(
            [], sections=[AnsibleInventoryGroup(group, hosts)
                          for group, hosts in a.items()])

    # Assume a list of global hosts and group:list(hosts)
    if isinstance(a, list) and isinstance(b, dict):
        return AnsibleInventory(
            a, sections=[AnsibleInventoryGroup(group, hosts)
                         for group, hosts in b.items()])

    if isinstance(a, six.string_types) and isinstance(b, dict):
        return AnsibleInventory(
            [a], sections=[AnsibleInventoryGroup(group, hosts)
                           for group, hosts in b.items()])

    raise Exception('simple_inventory could not parse arguments, '
                    'maybe you want something more complicate?')

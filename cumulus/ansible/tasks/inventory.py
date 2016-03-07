import re


class AnsibleInventoryHost(object):
    """
    Represents an Ansible inventory host and its associated variables.
    Implements low level functions for reading and writing the host to
    an inventory file. Note:  This does NOT model an actual host,  but
    rather a host line in an Ansible inventory file. It may represent
    multiple hosts in contexts where pattern matching is used (e.g.
    where a host declared as "www[01:50].example.com").
    """
    def __init__(self, host, **kwargs):
        self.host = host
        self.variables = kwargs

    def to_string(self):
        s = self.host
        if self.variables:
            s += " "
            s += " ".join(["%s=%s" % (k, v)
                           for k, v in self.variables.items()])
            s += "\n"

        return s

    @staticmethod
    def from_string(s):
        parts = s.rstrip().split()
        host = parts[0]
        kwargs = {}
        for part in parts[1:]:
            try:
                key = part.split("=")[0]
                value = part.split("=")[1]
                if bool(key) and bool(value):
                    kwargs[key] = value
                else:
                    raise RuntimeError("Could not parse %s for host %s" %
                                       (part, host))
            except IndexError:
                raise RuntimeError("Could not parse %s for host %s" %
                                   (part, host))
        return AnsibleInventoryHost(host, **kwargs)


class AnsibleInventorySection(object):
    """
    Abstract class that represents a config section in an Ansible inventory
    script.
    """
    def __init__(self, heading, items=None):
        self.heading = heading.rstrip()
        self.items = items if items is not None else []

    @property
    def name(self):
        raise NotImplemented("Must be implemented by subclass")

    @name.setter
    def name(self, value):
        raise NotImplemented("Must be implemented by subclass")

    @staticmethod
    def treat(line):
        raise NotImplemented("Must be implemented by subclass")

    def append(self, item):
        self.items.append(item)


class AnsibleInventoryGroup(AnsibleInventorySection):
    """
    Class that represents a group section in an Ansible inventory script
    """

    @staticmethod
    def treat(line):
        return True if line.startswith("[") \
            and ":" not in line else False

    @property
    def name(self):
        return self.heading[1:-1]

    @name.setter
    def name(self, value):
        self.heading = "[%s]" % value

    def to_string(self):
        s = "%s" % self.head
        s += "".join([i.to_string() for i in self.items])
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
    ignore_lines = re.compile("|^\s+$|^#")

    def __init__(self, global_hosts,  sections=None):
        self.global_hosts = []
        self.sections = sections if sections is not None else []

    @staticmethod
    def from_string(inventory):
        sections = []
        current = global_hosts = []

        for line in inventory.split("\n"):
            # Ignore comments and empty lines
            if AnsibleInventory.ignore_lines.match(line):
                continue

            for section_class in AnsibleInventory.section_classes:
                if section_class.treat(line):
                    sections.append(current)
                    current = section_class(line)
                else:
                    current.append(
                        AnsibleInventoryHost.from_string(line.rstrip()))

        return AnsibleInventory(global_hosts, sections)

    @staticmethod
    def from_file(path):
        with open(path, "rb") as fh:
            inventory = fh.read()
        return AnsibleInventory.from_string(inventory)

    def to_string(self):
        s = ""
        for host in self.global_hosts:
            s += host.to_string()

        s += "\n"

        for section in self.sections:
            s += section.to_string()
            s += "\n"

        return s

    def to_file(self, path):
        with open(path, "wb") as fh:
            fh.write(self.to_string())

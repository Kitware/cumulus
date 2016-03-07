import unittest
import mock
import os
import ansible
import cumulus.ansible.tasks.inventory as inventory


class AnsibleTestCase(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_ansible_host_basic(self):
        case = "localhost"
        h = inventory.AnsibleInventoryHost(case)
        self.assertEquals(case, h.host)
        self.assertEquals(case, h.to_string())

    def test_ansible_host_basic_read_from_string(self):
        case = "localhost"
        h = inventory.AnsibleInventoryHost.from_string(case)
        self.assertEquals(case, h.host)
        self.assertEquals(case, h.to_string())

    def test_ansible_host_variables_to_string(self):
        target = "localhost foo=bar baz=bar\n"
        h = inventory.AnsibleInventoryHost("localhost", foo="bar", baz="bar")

        # Ensure variables have been set on host
        self.assertTrue('foo' in h.variables)
        self.assertEquals(h.variables['foo'], 'bar')
        self.assertTrue('baz' in h.variables)
        self.assertEquals(h.variables['baz'], 'bar')

        # Test to_string
        self.assertEquals(h.to_string(), target)

    def test_ansible_malformed_host(self):
        cases = ["localhost foo",
                 "localhost foo=",
                 "localhost foo= bar",
                 "localhost foo==bar",
                 "localhost foo = bar"]
        for case in cases:
            with self.assertRaises(RuntimeError):
                inventory.AnsibleInventoryHost.from_string(case)

    def test_ansible_inventory_group_name(self):
        g = inventory.AnsibleInventoryGroup("[foobar]")
        self.assertEquals(g.heading, "[foobar]")
        self.assertEquals(g.name, "foobar")

    def test_ansible_inventory_group_set_name(self):
        g = inventory.AnsibleInventoryGroup("[foobar]")
        g.name = "FOOBAR"
        self.assertEquals(g.name, "FOOBAR")
        self.assertEquals(g.heading, "[FOOBAR]")

    def test_ansible_inventory_group_items(self):
        g = inventory.AnsibleInventoryGroup("[foobar]", ["an item"])
        self.assertEquals(len(g.items), 1)
        self.assertEquals(g.items[0], "an item")

    def test_ansible_inventory_group_treat(self):
        self.assertTrue(inventory.AnsibleInventoryGroup.treat("[foobar]"))
        self.assertFalse(inventory.AnsibleInventoryGroup.treat("[foobar:vars]"))
        self.assertFalse(inventory.AnsibleInventoryGroup.treat("[foobar:children]"))
        self.assertFalse(inventory.AnsibleInventoryGroup.treat("foobar"))

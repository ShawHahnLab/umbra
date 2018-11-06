#!/usr/bin/env python
"""
Tests for config module functions.
"""

from .test_common import *
from umbra import config

class TestConfig(unittest.TestCase):

    def setUp(self):
        self.configs = ["", "install", "report"]
        self.config_root = Path(umbra.config.__file__).parent / "data"

    def test_path_for_config(self):
        """path_for_config should give a path to a config file.
        
        The path should be absolute.  It is not checked for existence."""
        # With no arguments (or None or "") we should get the main config
        path = config.path_for_config()
        self.assertTrue(path.is_absolute())
        self.assertEqual(
                path.relative_to(self.config_root),
                Path("config.yml")
                )
        self.assertTrue(path.exists())
        path2 = config.path_for_config(None)
        self.assertEqual(path, path2)
        # For other cases it should given a sub-config
        for name in self.configs:
            path = config.path_for_config(name)
            path_obs = path.relative_to(self.config_root)
            if name:
                path_exp = Path("config_%s.yml" % name)
            else:
                path_exp = Path("config.yml")
            self.assertEqual(path_obs, path_exp)
            self.assertTrue(path.is_absolute())
        self.assertTrue(path.exists())
        # Other names produce paths just fine, but they don't exist.
        path = config.path_for_config("nonexistent")
        self.assertFalse(path.exists())

    def test_layer_configs(self):
        """layer_configs should combine the data from a list of config paths.
        
        The later entries take priority over the earlier ones.  Dicts are
        updated recursively, not replaced outright like dict.update does."""
        paths = [config.path_for_config(c) for c in self.configs]
        # the combined dict should have the last entry's readonly setting
        config_obs = config.layer_configs(paths)
        self.assertTrue(config_obs["readonly"])
        # Order matters; if we flip the order we get the first entry's readonly
        # setting.
        config_obs = config.layer_configs(paths[::-1])
        self.assertFalse(config_obs["readonly"])
        # No entries gives an empty dict.
        config_obs = config.layer_configs([])
        self.assertEqual(config_obs, {})
        # Missing entries are skipped. 
        config_obs = config.layer_configs([None] + paths + [""])
        self.assertTrue(config_obs["readonly"])

    def test_update_tree(self):
        """update_tree should combine dictionaries deeply.
        
        Dicts should be updated recursively, not replaced outright like
        dict.update does."""
        tree_old = {
                "something": {
                    "level2": {
                        "attr_a": 1,
                        "attr_b": 2
                        },
                    "shallow": True
                    }
                }
        tree_new = {
                "something": {
                    "level2": {
                        "attr_b": 4,
                        "attr_c": 3
                        }
                    }
                }
        # Here attr_b should be replaced and attr_c added, but attr_a should
        # still be present, too.
        tree_exp = {
                "something": {
                    "level2": {
                        "attr_a": 1,
                        "attr_b": 4,
                        "attr_c": 3
                        },
                    "shallow": True
                    }
                }
        tree_obs = config.update_tree(tree_old, tree_new)
        self.assertEqual(tree_obs, tree_exp)


if __name__ == '__main__':
    unittest.main()

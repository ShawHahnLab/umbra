"""
Tests for ProjectData instances linked to a single Task.

These are largely redundant as unit tests go so I'm disabling them in automatic
test discovery at the package level.
"""

from unittest import TestSuite
def load_tests(*_):
    """Ignore tests in this package."""
    suite = TestSuite()
    return suite

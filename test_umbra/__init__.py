"""
Unit tests and supporting files for umbra.

In general the package structure of test_umbra mirrors the package structure of
umbra, with one or more test cases per class and sometimes dedicated cases for
modules or helper functions.  When test cases need supporting files (input used
to run a test, or expected output for comparison with results) they refer to a
path within test_umbra/data/<path> where <path> corresponds to the location of
the test case code.  This is handled by TestBase.

The first iteration of test_umbra defined something more like integration tests
than true unit tests and grew horribly convoluted.  Remaining test cases from
this subclass TestBaseHeavy instead of TestBase and rely on more "live" objects
and directory trees to run.  These should all go away eventually.

There's a largely undocumented test_config.yml that can control some aspects of
the testing.  More intensive/invasive tests like true (but slow) contig
assembly or uploads to Box.com can be enabled but are off by default.
"""

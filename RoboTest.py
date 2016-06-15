
from __future__ import print_function
from cx_Oracle import connect
from subprocess import call, check_call
from sys import argv, stderr, __stderr__
from os import unlink, environ
from os.path import exists, join
from csv import reader, writer
from re import split
from robot.api import logger
from robot.libraries.BuiltIn import BuiltIn


TARGETS = environ['CATS_DATA_ROBOT_TARGETS']  # target files directory
RESULTS = environ['CATS_DATA_ROBOT_RESULTS']  # result files directory
DIFF_OUT = environ['CATS_DATA_ROBOT_RESULTS'] + '/diff.txt'  # temp file


class SkippedException(Exception):

    def __repr__(self):
        return 'SKIPPED'


class RoboTest:

    """A Robot plugin that provides some basic tests against databases
       and files.  The main feature is that the expected results are not
       saved in the test specification, but in separate files in a target
       files directory.  Result files are compared against those target
       files.

       If a test fails, and the new values are correct, then deleting
       the target file will mean that the next time the test is run, the
       results will be taken as future targets.
    """

    ROBOT_LIBRARY_SCOPE = "GLOBAL"

    def __init__(self, cnxn='cats_idcx/password@XE', debug=False):
        self.cnxn = cnxn
        self.debug = debug
        self._con = None # database state - cleaned up in self.close()
        self._cur = None # database state - cleaned up in self.close()
        self._out = None # output stream - cleaned up in self.close()
        self._previous = {}  # True for success, False for failure
        self._cache = {}
        self._init_db()

    # tests -------------------------------------------------------------------

    def count_lines(self, table, file, depends_on=None):
        """Count the lines in the given table.
           Write the result to the given file.
           Compare the written file with the target (if present)
           or save as target (if no current target).
        """
        self._skip(depends_on)
        try:
            self._clean(file)
            self._init_file(file)
            try:
                data = self._read_cache(table)
                self._record_sql('line count for %s' % table,
                                 {'COUNT(*)': [len(data[data.keys()[0]])]},
                                 ['COUNT(*)'], [])
            finally:
                self._close()
            if self._target_exists(file):
                self._compare_csv(file, result_name=table)
            else:
                self._copy_new(file)
        except Exception as e:
            self._record_failure(e)

    def select_fields(self, table, file, fields, orderby, delta=0, depends_on=None):
        """Select the given column(s) from a table, sorting them.
           Write the column to the given file.
           Compare the written file with the target (if present)
           or save as target (if no current target).
           Comparison of floats uses a configurable relative threshold.
        """
        self._skip(depends_on)
        if delta == "":
            delta = 0
        try:
            cols = map(lambda x: x.upper(), split(r'[, ]+', fields))
            ocols = map(lambda x: x.upper(), split(r'[, ]+', orderby))
            self._clean(file)
            self._init_file(file)
            try:
                self._record_sql('%s for %s ordered by %s' % 
                                 (fields, table, orderby),
                                 self._read_cache(table), cols, ocols)
            finally:
                self._close()
            if self._target_exists(file):
                self._compare_csv(file, delta=float(delta), result_name=table)
            else:
                self._copy_new(file)
        except Exception as e:
            self._record_failure(e)

    def grep_file_and_compare(self, infile, file, field, depends_on=None):
        """Extract lines from the file that contain the given text.
           Write the lines to the given file.
           Compare the written file with the target (if present)
           or save as target (if no current target).
        """
        self._skip(depends_on)
        try:
            self._clean(file)
            self._init_file(file)
            inp = open(infile, 'rb')
            for line in inp:
                if field.lower() in line.lower():
                    print(line.strip(), file=self._out)
            inp.close()
            self._close()
            if self._target_exists(file):
                self._compare_diff(file)
            else:
                self._copy_new(file)
        except Exception as e:
            self._record_failure(e)

    # support -----------------------------------------------------------------

    def _test_name(self):
        return BuiltIn().replace_variables('${TEST_NAME}')

    def _skip(self, depends_on):
        """Throw SkippedException if depends_on failed."""
        name = self._test_name()
        if name in self._previous:
            raise Exception('Repeated test name: %s' % name)
        self._previous[name] = True  # assume this test will succeed
        if depends_on:
            if depends_on not in self._previous:
                raise Exception('Bad dependency %s in %s' %
                                (depends_on, name))
            if not self._previous[depends_on]:
                raise SkippedException()

    def _record_failure(self, e):
        """Record failure and re-throw exception."""
        name = self._test_name()
        self._previous[name] = False  # this test failed
        raise e

    def _read_cache(self, table):
        """Read table into cache."""
        if table not in self._cache:
            self._cache[table] = {}
            self._cur.execute('select * from %s' % table)
            cols = [d[0] for d in self._cur.description]
            for col in cols:
                self._cache[table][col] = []
            for row in self._cur:
                for i, col in enumerate(cols):
                    self._cache[table][col].append(row[i])
        return self._cache[table]

    def _log(self, string):
        """Log as info to reobot."""
        if self.debug: logger.info(string)

    def _target_exists(self, file):
        """Does the file exist as a target?"""
        return exists(join(TARGETS, file))

    def _clean(self, file):
        """Delete the files used for this test."""
        if exists(join(RESULTS, file)): unlink(join(RESULTS, file))
        if exists(DIFF_OUT): unlink(DIFF_OUT)

    def _close(self):
        """Close the resources used in this test."""
        if self._out: self._out.close()

    def _init_db(self):
        """Open a connection to the database."""
        self._con = connect(self.cnxn)
        self._cur = self._con.cursor()

    def _init_file(self, file):
        """Prepare output."""
        self._out = open(join(RESULTS, file), 'w')
        
    def _record_sql(self, label, data, cols, ocols):
        """Write the data to the file.  The format duplicates how
           SQL was written directly, before we used a cache."""
        w = writer(self._out)
        w.writerow([label])
        toorder = []
        for ocol in ocols: toorder.append(data[ocol])
        for col in cols: toorder.append(data[col])
        ordered = map(lambda x: x[len(ocols):], sorted(zip(*toorder)))
        for (j, vals) in enumerate(ordered, start=1):
            row = [j]
            for (col, val) in zip(cols, vals):
                row.extend([col, val])
            w.writerow(row)
                
    def _compare_diff(self, file):
        """Compare target and result files using diff."""
        self._log('comparing %s %s' % 
                  (join(RESULTS, file), join(TARGETS, file)))
        try:
            check_call('diff -y --suppress-common-lines %s %s > %s' % 
                       (join(TARGETS, file), 
                        join(RESULTS, file), 
                        DIFF_OUT), shell=True)
            print("Test passed", file=stderr)
        except:
            print("Test failed", file=stderr)
            text = 'Could not read %s' % DIFF_OUT
            inp = None
            try:
                inp = open(DIFF_OUT, 'r')
                lines = inp.readlines()
                if len(lines) > 5:
                    text = ''.join(lines[:4]) + "..."
                else:
                    text = ''.join(lines)
            finally:
                if inp: inp.close()
                raise Exception(text)

    def _compare_csv(self, file, delta=0.0, result_name=None):
        """Compare target and result CSV files, entry by entry, with 
           floats using a relative threshold."""
        if result_name is None: result_name = join(RESULTS, file)
        target_name = join(TARGETS, file)
        self._log('comparing %s %s' % (result_name, target_name))
        with open(join(TARGETS, file), "r") as target:
            t = reader(target)
            with open(join(RESULTS, file), "r") as result:
                r = reader(result)
                for (trow, rrow) in map(None, t, r):
                    if trow is None: 
                        raise Exception("No target in %s matching %s in %s" % 
                                        (target_name, rrow, result_name))
                    if rrow is None: 
                        raise Exception("No result in %s matching %s in %s" % 
                                        (result_name, trow, target_name))
                    if len(trow) != len(rrow):
                        raise Exception("Row length changed in %s or %s" % 
                                        (target_name, result_name))
                    for (tval, rval) in zip(trow, rrow):
                        if tval != rval:
                            try:
                                # if we can convert to float and they are
                                # close enough, skip any error
                                tval = float(tval)
                                rval = float(rval)
                                big = max(abs(tval), abs(rval))
                                if abs(tval - rval) / big < delta:
                                    continue
                            except:
                                pass
                            raise Exception("(target) %s != %s (result in %s)" % 
                                            (tval, rval, result_name))

    def _copy_new(self, file):
        """Copy results to target (for future use as target)."""
        self._log('saving %s as new reference' % join(RESULTS, file))
        check_call('cp -p %s %s &> /dev/null' % 
                   (join(RESULTS, file), join(TARGETS, file)), 
                   shell=True)



if __name__ == '__main__':
    kargs = {}
    if len(argv) > 2: kargs['cnxn'] = argv[2]
    if len(argv) > 3: kargs['debug'] = argv[3]
    test = RoboTest(**kargs)
    test.test()

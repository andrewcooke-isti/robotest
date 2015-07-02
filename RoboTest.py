
from __future__ import print_function
from cx_Oracle import connect
from subprocess import call, check_call
from sys import argv, stderr
from os import unlink
from os.path import exists, join
from csv import reader, writer


REMOTE_TARGET = '/apps/data/robot/global-target-dir'   # master target dir
LOCAL_TARGET = '/apps/data/robot/local-target-dir'     # local copy of above
LOCAL_RESULT = '/apps/data/robot/local-result-dir'     # local results
DIFF_OUT = '/apps/data/robot/diff.txt'                 # temp file


class RoboTest:

    """A Robot plugin that provides some basic tests against databases
       and files.  The main feature is that the expected results are not
       saved in the test specification, but in separate files on the
       Jenkins master.  Results are compared against those files.

       If a test fails, and the new values are correct, then deleting
       the target file on master will mean that the next time the test
       is run, the results will be taken as future targets.
    """

    def __init__(self, master='dlv020', cnxn='cats_idcx/password@XE', 
                 debug=False):
        self.master = master
        self.cnxn = cnxn
        self.debug = debug
        # database state - cleaned up in self.close()
        self.con = None
        self.cur = None
        # output stream - cleaned up in self.close()
        self.out = None


    # tests -------------------------------------------------------------------

    def count_lines(self, table, file):
        """Count the lines in the given table.
           Write the result to the given file.
           Compare the written file with the target (if present)
           or save as target (if no current target).
        """
        self.clean(file)
        self.init_file(file)
        self.init_db()
        self.record_sql('line count for %s' % table,
                        'select count(*) from %s' % table)
        self.close()
        if self.target_exists(file):
            self.compare_csv(file)
        else:
            self.copy_new(file)

    def select_fields(self, table, file, *fields):
        """Select the given column(s) from a table, sorting them.
           Write the columns to the given file.
           Compare the written file with the target (if present)
           or save as target (if no current target).
        """
        self.clean(file)
        self.init_file(file)
        self.init_db()
        field_names = ','.join(fields)
        self.record_sql('%s for %s' % (field_names, table),
                        'select %s from %s order by %s' % 
                        (field_names, table, field_names))
        self.close()
        if self.target_exists(file):
            self.compare_csv(file)
        else:
            self.copy_new(file)

    def select_field(self, table, file, field, delta):
        """Select the given column from a table, sorting the values.
           Write the column to the given file.
           Compare the written file with the target (if present)
           or save as target (if no current target).
           Comparison of floats uses a configurable relative threshold.
        """
        self.clean(file)
        self.init_file(file)
        self.init_db()
        self.record_sql('%s for %s' % (field, table),
                        'select %s from %s order by %s' % 
                        (field, table, field))
        self.close()
        if self.target_exists(file):
            self.compare_csv(file, delta)
        else:
            self.copy_new(file)

    def grep_file(self, infile, file, field):
        """Extract lines from the file that contain the given text.
           Write the lines to the given file.
           Compare the written file with the target (if present)
           or save as target (if no current target).
        """
        self.clean(file)
        self.init_file(file)
        inp = open(infile, 'r')
        for line in inp:
            if field.lower() in line.lower():
                print(line.strip(), file=self.out)
        inp.close()
        self.close()
        if self.target_exists(file):
            self.compare_diff(file)
        else:
            self.copy_new(file)


    # support -----------------------------------------------------------------

    def log(self, string):
        """Unfortunately, Robot seems to swallow this."""
        if self.debug: print(string, file=stderr)

    def target_exists(self, file):
        """Does the file exist as a local target?"""
        return exists(join(LOCAL_TARGET, file))

    def clean(self, file):
        """Delete the files used for this test."""
        if exists(join(LOCAL_RESULT, file)): unlink(join(LOCAL_RESULT, file))
        if exists(join(LOCAL_TARGET, file)): unlink(join(LOCAL_TARGET, file))
        if exists(DIFF_OUT): unlink(DIFF_OUT)

    def close(self):
        """Close the resources used in this test."""
        if self.out: self.out.close()
        if self.cur: self.cur.close()
        if self.con: self.con.close()

    def init_db(self):
        """Open a connection to the database."""
        self.con = connect(self.cnxn)
        self.cur = self.con.cursor()

    def init_file(self, file):
        """Copy the target from the master machine."""
        self.log('retrieving %s from %s:%s' % 
                 (file, self.master, join(REMOTE_TARGET, file)))
        try:
            check_call('scp %s:%s %s &> /dev/null' % 
                       (self.master, join(REMOTE_TARGET, file), 
                        join(LOCAL_TARGET, file)),
                       shell=True)
        except:
            self.log('could not copy %s:%s - assuming starting from zero' %
                     (self.master, join(REMOTE_TARGET, file)))
        self.out = open(join(LOCAL_RESULT, file), 'w')
        
    def record_sql(self, label, sql):
        """Execute the given SQL and write the results, with column names,
           in CSV format."""
        w = writer(self.out)
        w.writerow([label])
        try:
            self.cur.execute(sql)
            cols = [d[0] for d in self.cur.description]
            for j, row in enumerate(self.cur, start=1):
                line = [j]
                for i, col in enumerate(cols):
                    line.extend([col, row[i]])
                w.writerow(line)
        except e:
            w.writerow([e])
                
    def compare_diff(self, file):
        """Compare target and result files using diff."""
        self.log('comparing %s %s' % 
                 (join(LOCAL_RESULT, file), join(LOCAL_TARGET, file)))
        try:
            check_call('diff -y --suppress-common-lines %s %s > %s' % 
                       (join(LOCAL_TARGET, file), 
                        join(LOCAL_RESULT, file), 
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

    def compare_csv(self, file, delta=0.001):
        """Compare target and result CSV files, entry by entry, with 
           floats using a relative threshold."""
        self.log('comparing %s %s' % 
                 (join(LOCAL_RESULT, file), join(LOCAL_TARGET, file)))
        with open(join(LOCAL_TARGET, file), "r") as target:
            t = reader(target)
            with open(join(LOCAL_RESULT, file), "r") as result:
                r = reader(result)
                for (trow, rrow) in map(None, t, r):
                    if trow is None: raise Exception("no target for %s", rrow)
                    if rrow is None: raise Exception("no result for %s", trow)
                    if len(trow) != len(rrow):
                        raise Exception("result file format changed")
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
                            raise Exception("%s != %s" % (tval, rval))

    def copy_new(self, file):
        """Copy results to target on the master machine (for future use
           as target)."""
        self.log('saving %s as new reference' % join(LOCAL_RESULT, file))
        check_call('scp %s %s:%s &> /dev/null' % 
                   (join(LOCAL_RESULT, file), self.master, 
                    join(REMOTE_TARGET, file)), 
                   shell=True)



if __name__ == '__main__':
    kargs = {}
    if len(argv) > 1: kargs['master'] = argv[1]
    if len(argv) > 2: kargs['cnxn'] = argv[2]
    if len(argv) > 3: kargs['debug'] = argv[3]
    test = RoboTest(**kargs)
    test.test()

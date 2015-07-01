
from __future__ import print_function
from cx_Oracle import connect
from subprocess import call, check_call
from sys import argv, stderr
from os import unlink
from os.path import exists, join
from csv import reader, writer

REMOTE_TARGET = '/apps/data/robot/global-target-dir'
LOCAL_TARGET = '/apps/data/robot/local-target-dir'
LOCAL_RESULT = '/apps/data/robot/local-result-dir'
DIFF_OUT = '/apps/data/robot/diff.txt'


class RoboTest:


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

    def grep_file(self, infile, file, field):
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
        if self.debug: print(string, file=stderr)

    def target_exists(self, file):
        return exists(join(LOCAL_TARGET, file))

    def clean(self, file):
        if exists(join(LOCAL_RESULT, file)): unlink(join(LOCAL_RESULT, file))
        if exists(join(LOCAL_TARGET, file)): unlink(join(LOCAL_TARGET, file))
        if exists(DIFF_OUT): unlink(DIFF_OUT)

    def close(self):
        if self.out: self.out.close()
        if self.cur: self.cur.close()
        if self.con: self.con.close()

    def init_db(self):
        self.con = connect(self.cnxn)
        self.cur = self.con.cursor()

    def init_file(self, file):
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
                        if type(tval) != type(rval):
                            raise Exception("type changed for %s/%s" % (tval, rval))
                        if tval != rval:
                            if isinstance(tval, float):
                                big = max(abs(tval), abs(rval))
                                if abs(tval - rval) / big < delta:
                                    continue
                            raise Exception("%s != %s" % (tval, rval))

    def copy_new(self, file):
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

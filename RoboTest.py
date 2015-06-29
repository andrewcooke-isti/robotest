
from __future__ import print_function
from cx_Oracle import connect
from subprocess import call, check_call
from sys import argv, stderr
from os import unlink
from os.path import exists, join

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
        try:
            self.init_file(file)
            self.init_db()
            self.record_sql('line count for %s' % table,
                            'select count(*) from %s' % table)
            self.close()
            if self.target_exists(file):
                self.compare(file)
            else:
                self.copy_new(file)
        finally:
            self.clean(file)

    def select_fields(self, table, file, *fields):
        self.clean(file)
        try:
            self.init_file(file)
            self.init_db()
            field_names = ','.join(fields)
            self.record_sql('%s for %s' % (field_names, table),
                            'select %s from %s' % (field_names, table))
            self.close()
            if self.target_exists(file):
                self.compare(file)
            else:
                self.copy_new(file)
        finally:
            self.clean(file)


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
        result = self.cur.execute(sql)
        print('label: %s' % result, file=self.out)
                
    def compare(self, file):
        self.log('comparing %s %s' % 
                 (join(LOCAL_RESULT, file), join(LOCAL_TARGET, file)))
        try:
            check_call('diff %s %s > %s' % 
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
                text = inp.read()
            finally:
                if inp: inp.close()
                raise Exception(text)
        
    def copy_new(self, file):
        self.log('saving %s as new reference' % join(LOCAL_RESULT, file))
        check_call('scp %s %s:%s &> /dev/null' % 
                   (join(LOCAL_RESULT, file), self.master, 
                    join(REMOTE_TARGET, file)), 
                   shell=True)
        raise Exception('Copied results as new reference')



if __name__ == '__main__':
    kargs = {}
    if len(argv) > 1: kargs['master'] = argv[1]
    if len(argv) > 2: kargs['cnxn'] = argv[2]
    if len(argv) > 3: kargs['debug'] = argv[3]
    test = RoboTest(**kargs)
    test.test()

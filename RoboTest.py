
from __future__ import print_function
from cx_Oracle import connect
from subprocess import call, check_call
from sys import argv, stderr
from os import unlink
from os.path import exists

REMOTE_TARGET = '/apps/data/robot/global-target.txt'
LOCAL_TARGET = '/apps/data/robot/local-target.txt'
LOCAL_RESULT = '/apps/data/robot/local-result.txt'
DIFF_OUT = '/apps/data/robot/diff.txt'

class RoboTest:

    def __init__(self, master='dlv020', cnxn='cats_idcx/password@XE', 
                 debug=False):
        self.cnxn = cnxn
        self.master = master
        self.debug = debug

    def test(self):
        self.clean()
        try:
            self.copy_prev()
            target_exists = exists(LOCAL_TARGET)
            self.measure_db()
            if target_exists:
                self.compare()
            else:
                self.copy_new()
        finally:
            self.clean()

    def log(self, string):
        if self.debug: print(string, file=stderr)

    def clean(self):
        if exists(LOCAL_RESULT): unlink(LOCAL_RESULT)
        if exists(LOCAL_TARGET): unlink(LOCAL_TARGET)
        if exists(DIFF_OUT): unlink(DIFF_OUT)
        
    def measure_db(self):
        self.log('writing database stats to %s' % LOCAL_RESULT)
        out, con, cur = None, None, None
        try:
            out = open(LOCAL_RESULT, 'w')
            con = connect(self.cnxn)
            cur = con.cursor()
            result = cur.execute('select count(*) from arrival').fetchall()
            print('count(arrival): %s' % result, file=out)
        finally:
            if cur: cur.close()
            if con: con.close()
            if out: out.close()

    def compare(self):
        self.log('comparing %s %s' % (LOCAL_RESULT, LOCAL_TARGET))
        try:
            check_call('diff %s %s > %s' % 
                       (LOCAL_TARGET, LOCAL_RESULT, DIFF_OUT), shell=True)
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
        
    def copy_prev(self):
        self.log('retrieving %s from %s:%s' % 
                 (LOCAL_TARGET, self.master, REMOTE_TARGET))
        try:
            check_call('scp %s:%s %s &> /dev/null' % 
                       (self.master, REMOTE_TARGET, LOCAL_TARGET),
                       shell=True)
        except:
            self.log('could not copy %s:%s - assuming starting from zero' %
                     (self.master, REMOTE_TARGET))
        
    def copy_new(self):
        self.log('saving %s as new reference' % LOCAL_RESULT)
        check_call('scp %s %s:%s &> /dev/null' % 
                   (LOCAL_RESULT, self.master, REMOTE_TARGET), 
                   shell=True)
        raise Exception('Copied results as new reference')

if __name__ == '__main__':
    kargs = {}
    if len(argv) > 1: kargs['master'] = argv[1]
    if len(argv) > 2: kargs['cnxn'] = argv[2]
    if len(argv) > 3: kargs['debug'] = argv[3]
    test = RoboTest(**kargs)
    test.test()

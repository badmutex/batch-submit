import re
import sys
import os
import os.path
import subprocess
import time
import shutil
import glob
import random


class BackendError (Exception): pass



class Backend (object):
    JOB_PREFIX     = 'job_'
    JOB_SUFFIX     = '.sh'
    WORKAREA_ID    = None
    WORKAREA_RANGE = (0,9999)
    WITHENV        = 'with-env'
    GOODENV        = 'env.sh'


    def __init__(self, withenv=None, environment=None, workarea=None, overwrite_workarea=False):
        """
        Create backend for submitting batch jobs and waiting for them to finish.
        This is side-effect-free.

        Key word args:
          *withenv*     : path to the environment wrapper script.
                          Default = None
          *environment* : path to the environment script to pass to the *withenv* option.
                          Default = None
          *workarea*    : location to generate jobs.
                          Default = $PWD/workarea_<backend name>

        """

        Backend.WORKAREA_ID = random.randint(*Backend.WORKAREA_RANGE)

        if withenv and environment:
            self.withenv            = dict()
            self.withenv['withenv'] = os.path.abspath(os.path.expanduser(withenv))
            self.withenv['env']     = os.path.abspath(os.path.expanduser(environment))
        else:
            self.withenv = None
        self.workarea    = os.path.abspath(workarea or \
                                               ('workarea_' +
                                                self.__class__.__name__ +
                                                '-' + str(Backend.WORKAREA_ID)))
        self.overwrite_workarea = overwrite_workarea

    def setup(self):
        """
        Setup all the requirements to run jobs (ie: create the workarea if needed, etc).
        May have side-effects.
        """

        if not os.path.exists(self.workarea):
            print 'Creating workarea:', self.workarea
            os.makedirs(self.workarea)
        elif not self.overwrite_workarea:
            raise BackendError, 'Workarea %s already exists' % self.workarea

        print 'Setting up workarea:', self.workarea

        os.chmod(self.workarea, 0777) # drwxrwxrwx
        if self.withenv:
            print 'Adding environment script:', self.withenv['env']

            withenv = os.path.join(self.workarea, Backend.WITHENV)
            shutil.copy(self.withenv['withenv'], withenv)
            os.chmod(withenv, 0755)
            shutil.copy(self.withenv['env'], os.path.join(self.workarea, Backend.GOODENV))


    def create_jobs_generator(self, commands):
        """
        This returns a generator that when evaluated create a job file.
        Each iteration yields: (jobid, jobfile_path)
        """
        for _jid, _cmd in enumerate(commands):

            jid = _jid + 1

            if self.withenv:
                cmd = './%(withenv)s %(goodenv)s %(cmd)s' % {'withenv' : Backend.WITHENV,
                                                             'goodenv' : Backend.GOODENV,
                                                             'cmd'     : _cmd}
            else:
                cmd = _cmd


            jobfile = os.path.join(self.workarea, '%s%d%s' % (Backend.JOB_PREFIX, jid, Backend.JOB_SUFFIX))
            with open(jobfile, 'w') as fd_job:
                fd_job.write(self.job_preamble(jobfile=jobfile) + '\n')
                fd_job.write(cmd + '\n')
                fd_job.write(self.job_conclusion(jobfile=jobfile) + '\n')
            os.chmod(jobfile, 0755) # -rwxr-xr-x
            yield jid, jobfile


    def submit_jobs(self, jobfiles):
        """
        Signature: [path] -> IO job_id
        Function: generate any extra files and submit the job

        Implemented by the subclass.
        """

        raise NotImplemented

    def job_preamble(self, **kwargs):
        """
        Signature: **kws -> String
        Function: returns the string to write before the command to the job file

        Implemented by the subclass.
        """
        raise NotImplemented

    def job_conclusion(self, **kwargs):
        """
        Signature: **kws -> String
        Function: return the string to write after the command in the job file

        Implemented by the subclass.
        """
        raise NotImplemented

    def wait(self, **kws):
        """
        Signature: **kws -> IO ()
        Function: Wait for the submitted job to finish

        Implemented by the subclass.
        """
        raise NotImplemented


    def submit(self, commands, **kwargs):
        """
        Calls:
          self.setup()
          self.create_jobs_generator(commands)
          self.submit_jobs(jobfiles, **kwargs)
        """

        self.setup()
        jobfiles = list()
        for jid, jobfile in self.create_jobs_generator(commands):
            print 'Creating job:', jobfile
            jobfiles.append(jobfile)
        return self.submit_jobs(jobfiles, **kwargs)

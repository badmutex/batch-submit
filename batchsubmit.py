#!/usr/bin/env python


import re
import sys
import os
import os.path
import subprocess
import time
from optparse import OptionParser


class BackendError (Exception): pass



class Backend (object):
    JOB_PREFIX = 'job_'
    JOB_SUFFIX = '.sh'
    COUNTER    = 0

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

        Backend.COUNTER += 1

        self.withenv     = withenv or ''
        self.environment = environment or ''
        self.workarea    = os.path.abspath(workarea or \
                                               ('workarea_' +
                                                self.__class__.__name__ +
                                                '-' + str(Backend.COUNTER)))
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

        os.chmod(self.workarea, 0777) # drwxrwxrwx


    def create_jobs_generator(self, commands):
        """
        This returns a generator that when evaluated create a job file.
        Each iteration yields: (jobid, jobfile_path)
        """
        for _jid, _cmd in enumerate(commands):

            jid = _jid + 1

            cmd = (' '.join([self.withenv, self.environment]) + cmd) \
                if self.withenv and self.environment else \
                _cmd

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

class SGE (Backend):
    JOBID_RE = r'(?P<jid>\d+)'

    def __init__(self, *args, **kws):
        Backend.__init__(self, *args, **kws)

        sge_jid_re = kws.get('sge_jid_re', SGE.JOBID_RE)
        self.sge_jid_regex = re.compile(sge_jid_re)

        self.job_id = None


    def parse_time_units(self, duration):
        t = int(duration[:-1])
        u  = duration[-1] 
        if u == 's':
            sleeptime = t
        elif u == 'm':
            sleeptime = t * 60
        elif u == 'h':
            sleeptime = t * 60 * 60
        elif u == 'd':
            sleeptime = t * 60 * 60 * 24
        elif u == 'w':
            sleeptime = t * 60 * 60 * 24 * 7
        else:
            raise BackendError, 'unknown time units: %s' % units

        return sleeptime


    def wait(self, **kws):
        """
        Wait for an SGE job to finish. Raises a *BackendError* if *max_tries* is exceeded.
        Key words:
          *poll_interval* : how long to wait between tries. Format: <time><units> where <units>
                            can be one of s, m, h, d, w for seconds, minutes, hours, days, weeks respectively
                            Default = 1m
          *max_tries*     : number of iterations to wait before giving up.
                            Default = infinity
        """

        poll_interval = kws.get('poll_interval', '1m')
        max_tries     = kws.get('max_tries', float('inf'))

        sleeptime     = self.parse_time_units(poll_interval)

        tries = 0
        print 'Checking SGE job id:', self.job_id, 'polling every', poll_interval, 'for', max_tries, 'attempts'
        while True:
            with open('/dev/null', 'w') as devnull:
                retcode = subprocess.call(('qstat -j %d' % self.job_id).split(),
                                          stdout = devnull,
                                          stderr = devnull)
            job_finished = not retcode == 0
            print '[', tries, '] Is job finished?:', job_finished

            if job_finished:
                break
            else:
                tries += 1
                if tries >= max_tries:
                    raise BackendError, 'Timeout: exceeded max tries: %d' % max_tries
                time.sleep(sleeptime)


    def prepare_scripts(self, jobfiles, **kws):
        """
        *jobfiles*: paths to scripts

        Available key word arguments:
        *begin*
        *end*
        *step*
        *qsubargs*
        """

        begin    = kws.get('begin', 1)
        end      = kws.get('end', len(jobfiles))
        step     = kws.get('step', 1)
        qsubargs = kws.get('qsubargs', '')


        ### write the worker script ###

        worker = os.path.join(self.workarea, 'worker.sh')
        print 'Writing worker:', worker
        with open(worker, 'w') as fd_wrk:
            fd_wrk.write("""\
#!/usr/bin/env bash
./%s${SGE_TASK_ID}%s
""" % (Backend.JOB_PREFIX, Backend.JOB_SUFFIX))
        os.chmod(worker, 0755)



        ### write the script to submit to qsub ###

        submitter = os.path.join(self.workarea, 'submit.sh')
        print 'Writing submitter', submitter
        with open(submitter, 'w') as fd_sub:
            fd_sub.write("""\
#!/usr/bin/env bash
qsub %(extra args)s -t %(begin)d-%(end)d:%(step)d %(worker)s
""" % { 'extra args' : qsubargs or '',
        'begin'      : begin,
        'end'        : end,
        'step'       : step, 
        'worker'     : os.path.basename(worker)})
        os.chmod(submitter, 0755)

        return submitter



    def submit_jobs(self, jobfiles, **kws):
        """
        *jobfiles*: paths to the work scripts

        Available key word arguments:
        *begin*
        *end*
        *step*
        *qsubargs*
        """

        submitter = self.prepare_scripts(jobfiles, **kws)

        ### submit the job and capture the SGE job id and return it ###

        pwd = os.getcwd()
        os.chdir(self.workarea)
        try:
            output = subprocess.check_output(['./' + os.path.basename(submitter)])
            match  = self.sge_jid_regex.search(output)

            if match:
                sge_jid = int(match.group(0))
            else:
                raise BackendError, 'Cannot find SGE job id in: %s' % output

            self.job_id = sge_jid
            return sge_jid
        finally:
            os.chdir(pwd)


    def job_preamble(self, **kws):
        return """\
#!/usr/bin/env bash

myname=%(jobfile)s
done=$myname.done

if [ -f "$done" ]; then
    echo "$myname already successfully ran"
    exit 0
fi
""" % {'jobfile' : os.path.abspath(kws['jobfile'])}


    def job_conclusion(self, **kws):
        return """\
ecode=$?

if [ $ecode -eq 0 ]; then
    echo DONE
    touch "$done"
fi
"""




# class BS (object):

#     def __init__(self, command=None, args=None, data=None, outfile=None, reducer=None):
#         self.command = command
#         self.args    = args
#         self.data    = data
#         self.outfile = outfile
#         self.reducer = reducer 




def example():
    cmds = ['echo hello','echo world;sleep 2']

    b = SGE()
    b.submit(cmds)
    b.wait(poll_interval = '2s', max_tries=float('inf'))



# traj_assignments = database.query()
# samples = bootstrap(traj_assignments)
# for lagTime in lagTimes:
#     all_timescales = []
#     for s in samples:
#         eigenvalues, eigenvectors = transition_matrix(s, lagTime)
#         timescales = compute_timescales(eigenvalues)
#         all_timescales.append(timescales)
#     error_bar = bootstrap_statistics(all_timescales)
# make_plot(error_bar, output=myoutdir).sumbit(wait=True)

from backend import *


class SGE (Backend):
    JOBID_RE  = r'(?P<jid>\d+)'
    WORKER    = 'worker.sh'
    SUBMITTER = 'submit.sh'

    def __init__(self, *args, **kws):
        Backend.__init__(self, *args, **kws)

        sge_jid_re = kws.get('sge_jid_re', SGE.JOBID_RE)
        self.sge_jid_regex = re.compile(sge_jid_re)

        self.running_job_id = None
        self.job_ids = list()

        self.worker    = os.path.join(self.workarea, SGE.WORKER)
        self.submitter = os.path.join(self.workarea, SGE.SUBMITTER)


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


    def is_job_running(self):
        with open('/dev/null', 'w') as devnull:
            retcode = subprocess.call(('qstat -j %d' % self.running_job_id).split(),
                                      stdout = devnull,
                                      stderr = devnull)
            running = retcode == 0
            return running


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

        print 'Checking SGE job id:', \
               self.running_job_id, 'polling every', poll_interval, 'for', max_tries, 'attempts'
        while True:
            job_finished = not self.is_job_running()
            print '[', tries, '] Is job finished?:', job_finished

            if job_finished:
                self.running_job_id = None
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

        print 'Writing worker:', self.worker
        with open(self.worker, 'w') as fd_wrk:
            fd_wrk.write("""\
#!/usr/bin/env bash
./%s${SGE_TASK_ID}%s
""" % (Backend.JOB_PREFIX, Backend.JOB_SUFFIX))
        os.chmod(self.worker, 0755)



        ### write the script to submit to qsub ###

        print 'Writing submitter', self.submitter
        with open(self.submitter, 'w') as fd_sub:
            fd_sub.write("""\
#!/usr/bin/env bash
qsub %(extra args)s -t %(begin)d-%(end)d:%(step)d %(worker)s
""" % { 'extra args' : qsubargs or '',
        'begin'      : begin,
        'end'        : end,
        'step'       : step, 
        'worker'     : os.path.basename(self.worker)})
        os.chmod(self.submitter, 0755)

        return self.submitter



    def submit_jobs(self, jobfiles, **kws):
        """
        *jobfiles*: paths to the work scripts

        Available key word arguments:
        *retry* :: Boolean
        *begin* :: Int
        *end*   :: Int
        *step*  :: Int
        *qsubargs* :: String
        """

        retry = kws.pop('retry', False)

        if not retry:
            self.prepare_scripts(jobfiles, **kws)

        ### submit the job and capture the SGE job id and return it ###

        pwd = os.getcwd()
        os.chdir(self.workarea)
        try:
            output = subprocess.check_output(['./' + os.path.basename(self.submitter)])
            match  = self.sge_jid_regex.search(output)

            if match:
                sge_jid = int(match.group(0))
            else:
                raise BackendError, 'Cannot find SGE job id in: %s' % output

            self.running_job_id = sge_jid
            self.job_ids.append(sge_jid)
            return sge_jid
        finally:
            os.chdir(pwd)


    def resubmit(self):
        """
        Resubmits the jobs
        """
        return self.submit_jobs(None, retry=True)


    def stop(self):
        """
        Kills the job by executing 'qdel -j <job id>'
        """
        if self.is_job_running():
            with open('/dev/null', 'w') as devnull:
                cmd = 'qdel -j %d' % self.running_job_id
                retcode = subprocess.call(cmd.split(),
                                          stdout = devnull,
                                          stderr = devnull)


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


    def iglob_results(self):
        """
        returns a generator over the files created by SGE to captures STDOUT
        """

        if self.running_job_id is not None:
            raise BackendError, 'No job running'


        globs = list()
        for job_id in self.job_ids:

            pattern = '%(workarea)s/%(worker)s*%(jid)d.*' % {'workarea' : self.workarea,
                                                             'worker'   : SGE.WORKER,
                                                             'jid'      : job_id }

            print 'Globbing for:', pattern
            globs.append(glob.iglob(pattern))

        return lazy_concat(*globs)


    def result_lines(self):
        """
        returns a generator over the data contained in the files generated by SGE
        """

        for path in self.iglob_results():
            with open(path) as fd:
                for line in fd:
                    yield line


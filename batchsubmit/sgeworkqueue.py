
import backend
import sge
from workqueue import WorkQueue, Task
from workqueue import WORK_QUEUE_SCHEDULE_FCFS, WORK_QUEUE_SCHEDULE_FILES

import os



class SGEWorkQueue(sge.SGE):
    """
    Generates a workflow using for SGE, but uses WorkQueue to run the jobs.
    The master user can then have multiple accounts submit workers for the jobs.
    """


    def __init__(self, *args, **kws):

        port = kws.pop('port', 9123)
        master_name = kws.pop('master_name', 'bs.sge.wq')
        wq_alg = kws.pop('wq_alg', WORK_QUEUE_SCHEDULE_FCFS)

        backend.Backend.__init__(self, *args, **kws)

        self.workqueue = WorkQueue(port, name=master_name, **kws)
        self.workqueue.specify_algorithm(wq_alg)

        
    def create_task(self, jobfile):
        """
        Creates a Task to execute the specified jobfile
        """

        job = os.path.basename(jobfile)

        cmd = 'cd %(workarea)s;./%(jobfile)s' % {
            'workarea' : self.workarea,
            'jobfile'  : job }

        print 'Task Command:', cmd

        t = Task(cmd)
        t.tag = job

        return t

    def submit_jobs(self, jobfiles, **kws):
        """
        Creates Tasks for the jobfiles and submits them to the WorkQueue
        """

        for job in jobfiles:
            task = self.create_task(job)
            print 'Submitting', job
            self.workqueue.submit(task)


    def is_job_running(self):
        return not self.workqueue.empty()

    def wait(self, **kws):
        """
        Wait for an SGE job to finish. Raises a *BackendError* if *max_tries* is exceeded.
        Key words:
          *poll_interval* : how long to wait between tries. Format: <time><units> where <units>
                            can be one of s, m, h, d, w for seconds, minutes, hours, days, weeks respectively
                            Default = 1m
        """

        poll_interval = kws.get('poll_interval', '1m')

        sleeptime     = self.parse_time_units(poll_interval)

        success = True

        while True:
            if not self.is_job_running():
                break

            task = self.workqueue.wait(sleeptime)

            print '\tinit:', self.workqueue.stats.workers_init
            print '\tready:', self.workqueue.stats.workers_ready
            print '\tbusy:', self.workqueue.stats.workers_busy
            print '\trunning:', self.workqueue.stats.tasks_running
            print '\twaiting:', self.workqueue.stats.tasks_waiting
            print '\tcomplete:', self.workqueue.stats.tasks_complete

            # self.print_wq_stats()
            if task:
                print 'Job %s finished with %s' % (task.tag, task.return_status)
                success = success and task.return_status == 0

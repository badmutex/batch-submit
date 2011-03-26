Overview
--------

The purpose of this simple module is to allow for transparent
generation and submission of jobs to a batch system such as SGE or
Condor.


Example Usage
-------------

In python::

	from batchsubmit import SGE
	cmds = ['echo hello','echo world']
    b = SGE()
    b.submit(cmds)
    b.wait(poll_interval = '2s', max_tries=float('inf'))

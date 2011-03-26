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


In bash::

    $ cat <<EOF>/tmp/test.in
    hello
    world
    EOF

    $ with-bs -W -P 2s /tmp/test.in echo

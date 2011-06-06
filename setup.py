from distutils.core import setup

setup(name='batchsubmit',
      version='0.2',
      py_modules= ['batchsubmit.backend',
                   'batchsubmit.sge',
                   'batchsubmit.sgeworkqueue',
                   ],
      # data_files = ['share/with-env'],
      scripts = ['scripts/with-bs']
      )

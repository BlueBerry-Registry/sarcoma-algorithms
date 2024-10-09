How to use
==========

Input arguments
---------------

.. describe the input arguments:
.. ['database_label', 'cohort_id', 'cohort_task_id']

Python client example
---------------------

To understand the information below, you should be familiar with the vantage6
framework. If you are not, please read the `documentation <https://docs.vantage6.ai>`_
first, especially the part about the
`Python client <https://docs.vantage6.ai/en/main/user/pyclient.html>`_.

.. TODO Some explanation of the code below

.. code-block:: python

  from vantage6.client import Client

  server = 'http://localhost'
  port = 5000
  api_path = '/api'
  private_key = None
  username = 'root'
  password = 'password'

  # Create connection with the vantage6 server
  client = Client(server, port, api_path)
  client.setup_encryption(private_key)
  client.authenticate(username, password)

  input_ = {
    'master': True,
    'method': 'central',
    'args': [],
    'kwargs': {
        'database_label': 'my_value',
        'cohort_id': 'my_value',
        'cohort_task_id': 'my_value',
    },
    'output_format': 'json'
  }

  my_task = client.task.create(
      collaboration=1,
      organizations=[1],
      name='ohdsi-update-csv',
      description='This will extract a dataset from a cohort in the OMOP database and overwrites the CSV attached to the vantage6-node. The user can determine which database to overwrite.',
      image='ohdsi-update-csv',
      input=input_,
      data_format='json'
  )

  task_id = my_task.get('id')
  results = client.wait_for_results(task_id)
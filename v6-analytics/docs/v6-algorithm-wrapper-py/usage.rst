How to use
==========

Input arguments
---------------

.. describe the input arguments:
.. ['arg1']

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
        'arg1': 'my_value',
    },
    'output_format': 'json'
  }

  my_task = client.task.create(
      collaboration=1,
      organizations=[1],
      name='v6-algorithm-wrapper-py',
      description='This package wraps already existing algorithms so that we can use vanilla algorithms in the sarcoma registry',
      image='v6-algorithm-wrapper-py',
      input=input_,
      data_format='json'
  )

  task_id = my_task.get('id')
  results = client.wait_for_results(task_id)
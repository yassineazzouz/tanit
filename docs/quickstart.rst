Quickstart
==========

.. currentmodule:: tanit

This Quickstart guide describe how to install a very basic Tanit configuration to get you up and and running with Tanit very quickly.

.. note::
    You can perform the installation steps in a single machine.

Install Tanit
-------------------

The installation is quick and straightforward.

.. code-block::

    # Tanit needs a configuration directory, /etc/tanit/conf is the default,
    # but you can lay foundation somewhere else if you prefer
    # (optional)
    export TANIT_CONF_DIR="/etc/tanit/conf"

    # install from pypi using pip
    pip install tanit


Configuration
-------------------

The master and master service needs a configuration file 'tanit.conf' to be created under TANIT_CONF_DIR.

A very basic configuration for a single machine cluster:

.. code-block::

    [master]
    bind_address = 0.0.0.0
    worker-service_address = localhost:9091
    client-service_address = localhost:9090
    thrift_threads = 25

    [worker]
    bind_address = 0.0.0.0
    bind_port = 9093

.. note::
    In the above example both the master and worker configurations are merged within a single file, because the master and worker services will be running in the same machine.


Start the master
-------------------

.. code-block::

    tanit master

The above wil start the master process attached to the current shell, you can run the process in the background and use nohup to attach it to init, for instance :


.. code-block::

    nohup tanit master > tanit-master.log 2>&1 &


Start the worker
-------------------

.. code-block::

    tanit worker


Or you can also run the worker as background process :

.. code-block::

    nohup tanit worker > tanit-worker.log 2>&1 &


Upon a successful start of the worker service, the master will output the below log lines:

.. code-block::

    INFO	Registering new Worker [ tanit-worker-hostname-9093 ].
    INFO	Worker [ tanit-worker-hostname-9093 ] registered.
    INFO	Activating Worker [ tanit-worker-hostname-9093 ]
    INFO	Transitioning worker from state ALIVE to ACTIVE
    INFO	Worker [ tanit-worker-hostname-9093 ] Activated
    INFO	Registering new filesystem [ local:hostname ].
    INFO	Registered filesystem 'local:hostname'
    INFO	Filesystem [ local:hostname ] registered.

In the above example 'hostname' refer to the hostname of the machine, that you get by running `hostname` in linux.


List Workers
-------------------

You can use the Tanit command line tool to verify that the worker have been properly registered :

.. code-block::

    [root@edge /]# tanit workers list
    INFO	Instantiated configuration from /etc/tanit/conf/tanit.conf.
    Worker { id: tanit-worker-hostname-9093, address: hostname, port: 9093}
    [root@edge /]#
    [root@edge /]# tanit workers status tanit-worker-hostname-9093
    INFO	Instantiated configuration from /etc/tanit/conf/tanit.conf.
    WorkerStats { id: tanit-worker-hostname-9093, state: ACTIVE, last_heartbeat: 01112020160116, running_tasks: 0, pending_tasks: 0, available_cores: 4 }


Register your first Filesystem
-------------------------------

You can use the tanit command line tool to register filesystems :

.. code-block::

    tanit filesystems register "{ \"name\": \"s3-test-bucket\", \"type\": \"s3\", \"bucket\" : \"kraken-test\" }"

Run your first Tanit job
-------------------------------

.. code-block::

    tanit jobs submit { \"type\": \"COPY\", \"params\": { \"src\": \"s3-test-bucket\", \"dst\": \"local:edge\", \"src_path\": "/some_source_path\", \"dest_path\": "some_destination_path\"} }

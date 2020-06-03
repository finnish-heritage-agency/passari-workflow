Configuration
=============

Requirements
----------------

- PostgreSQL server
- Redis server
- File system with plenty of free space for SIPs under processing

  - If running multiple worker servers, you will need to use a distributed file system. For example, you can use cloud storage mounted into the local system using SMB/Samba.

Setup
-----

Once you have installed *Passari* and *Passari Workflow*, run a command such as the following to generate a configuration file:

.. code-block:: console

   $ pas-shell --help

The configuration file can be found in `~/.config/passari-workflow/config.toml`. You can also copy the configuration file to `/etc/passari-workflow/config.toml`.

.. note::

   System-wide configuration file in `/etc/passari-workflow/config.toml` will take precedence over the local configuration file.

The configuration file should look similar to this:

.. code-block::

   [logging]
   # different logging levels:
   # 50 = critical
   # 40 = error
   # 30 = warning
   # 20 = info
   # 10 = debug
   level=10

   [db]
   # PostgreSQL server credentials
   user=''
   password=''
   host='127.0.0.1'
   port='5432'
   name='passari'

   [redis]
   # Redis server credentials
   host='127.0.0.1'
   port='6379'
   password=''

   [package]
   # Directory used for packages under processing.
   # It is recommended to use a high performance and high capacity storage
   # for this directory, as the workflow cannot know the size of individual
   # files before downloading them.
   # This should *not* be located in the same filesystem as the Redis and
   # PostgreSQL, as the filesystem can otherwise get full at random due to the
   # aforementioned reason.
   package_dir=''

   # Directory used for storing preservation reports for each processed SIP.
   # These files are not read automatically by the workflow and are accessible
   # only through the web UI, meaning it is recommended to use a storage
   # designed for infrequent reads.
   archive_dir=''

   # Delay before a new package will enter preservation.
   # Default is 30 days (2592000 seconds)
   preservation_delay=2592000

   # Delay before a preserved package will be updated if changed.
   # Default is 30 days (2592000 seconds)
   update_delay=2592000


Once you have filled the configuration file, you can create the Passari database tables by running the following command:

.. code-block:: console

   $ alembic upgrade head

If you have configured everything correctly, the command should work successfully.

Configuring automated tasks
---------------------------

*Passari Workflow* relies on automated tasks to periodically synchronize with the MuseumPlus service and determine which objects should be preserved.

These automated tasks are implemented as the scripts ``sync-objects``, ``sync-attachments``, ``sync-hashes`` and ``sync-processed-sips``.

The commands ``sync-objects`` and ``sync-attachments`` will likely take a long time to perform the first-time synchronization with MuseumPlus. For this reason, those scripts have the ``--save-progress`` flag which will periodically save the synchronization progress and continue from the same position on later runs.

For example, you could run the scripts off-hours using the following schedule:

- On odd-numbered days, start ``. <venv_dir>/bin/activate; sync-objects --save-progress`` at 8 PM and stop the script at 4 AM.
- On even-numbered days, start ``. <venv_dir>/bin/activate; sync-attachments --save-progress`` at 8 PM and stop the script at 4 AM.
- Every day at 5 AM, run the script ``. <venv_dir>/bin/activate; sync-hashes`` until its completion.
- Once a hour, run the script ``. <venv_dir>/bin/activate; sync-processed-sips`` until its completion.

.. note::

   The three scripts ``sync-objects``, ``sync-attachments`` and ``sync-hashes`` cannot be run simultaneously! For example, you can't have ``sync-objects`` and ``sync-attachments`` running at the same time.

Configuring RQ workers
----------------------

*Passari Workflow* uses a job queue called `RQ <https://python-rq.org/>`_ to automatically process objects. The job queue consists of four different queues: ``download_object``, ``create_sip``, ``submit_sip`` and ``confirm_sip``.

You can get started by creating a RQ configuration file called `worker_config.py` in the same directory as *passari-workflow* with the following contents:

.. code-block:: console

   REDIS_HOST = "<host>"
   REDIS_PORT = 6379
   REDIS_DB = 0
   REDIS_PASSWORD = "<password>"

With the virtualenv active, you can start a worker with the following command:

.. code-block:: console

   $ rq worker -c worker_config --name submit-sip-1 --queue-class "passari_workflow.queue.queues.WorkflowQueue" download_object

.. note::

   Note that the last parameter -- ``download_object`` -- uses an underscore instead of a dash.

You can start multiple workers for each queue -- make sure to use an unique ``--name`` for each worker. For example, if you want to validate and package more objects in parallel, you can launch more ``create_sip`` workers.

It is recommended to service manager such as *systemd* to manage RQ workers. You can use the following systemd `download-object-worker@.service` file as an example:

.. code-block::

   [Unit]
   Description=download-object RQ worker %i
   After=network.target

   [Service]
   Type=simple
   WorkingDirectory=/home/passari/passari-workflow
   Environment=LANG=en_US.UTF-8
   Environment=LC_ALL=en_US.UTF-8
   Environment=LC_LANG=en_US.UTF-8
   ExecStart=/home/passari/passari-workflow/venv/bin/rq worker -c worker_config --name download-object-%i --queue-class "passari_workflow.queue.queues.WorkflowQueue" download_object
   ExecReload=/bin/kill -s HUP $MAINPID
   ExecStop=/bin/kill -s TERM $MAINPID
   # Give each worker 20 minutes to finish the current task before forcing
   # shutdown
   TimeoutStopSec=1200
   PrivateTmp=true
   Restart=always
   # Wait 5 seconds before restarting to ensure the old worker isn't registered
   # anymore
   RestartSec=5
   User=passari
   Group=passari

   [Install]
   WantedBy=multi-user.target

Using the service file, you can now launch and stop workers easily:

.. code-block:: console

   # systemctl start download-object-worker@0 # Launch download-object worker #0
   # systemctl start download-object-worker@1 # Launch download-object worker #1
   # systemctl stop download-object-worker@0  # Stop download-object worker #0

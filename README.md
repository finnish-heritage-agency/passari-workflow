passari-workflow
================

Components to implement a workflow for digital preservation components
in `passari` using Redis for the job queue and PostgreSQL
as data storage.

Installation
------------

```
sudo dnf install python3 python3-virtualenv
python3 -mvenv venv
source venv/bin/activate
# Install Passari first. Replace 1.0 with newer version if tagged.
pip install --upgrade git+https://github.com/finnish-heritage-agency/passari.git@1.0#egg=passari
# Install Passari Workflow 1.0. Replace 1.0 with newer version if tagged.
pip install --upgrade git+https://github.com/finnish-heritage-agency/passari-workflow.git@1.0#egg=passari-workflow
```

See the documentation for further details on configuring the workflow.

Documentation
-------------

Documentation can be generated using Sphinx by running the following command:

```
python setup.py build_sphinx
```

Workflow process
----------------

The workflow consists of the following scripts in the described order. A few of the steps are performed manually by an administrator, while most are automated with varying schedules; some steps begin execution immediately after the previous one, while other steps are performed later and scheduled on a hourly or a weekly basis.

* `sync_objects`
  * The script scrapes the MuseumPlus database for all objects and their metadata. This information is inserted or updated into the local database.
  * The script is designed to be called **automatically (eg. multiple times a week)** when the MuseumPlus database is least active (eg. starting in the evening).
  * The first run of the script can take dozens of hours to complete. Because of this, the script can be halted so that it continues from the same state in the next run.
* `sync_attachments`
  * Very similar to `sync_objects` but scrapes attachment metadata instead of object metadata.
  * Should **not** be run simultaneously with `sync_objects`.
* `sync_hashes`
  * The script updates the database with data collected using `sync_objects` and `sync_attachments`, and determine which objects can be enqueued into the preservation workflow.
  * The script is designed to be called **automatically (eg. once a day)**.
* `enqueue_objects` or `deferred_enqueue_objects`
  * The script checks the local database for objects and their modification dates. If there are objects that haven't been preserved, or objects that have been preserved and updated recently, preservation tasks will be enqueued for those objects.
  * The script is designed to be called **manually by an administrator**, who will also determine how many objects to enqueue at once. For example, the administrator might start with a small number of objects to determine the amount of time it takes to submit those objects to the digital preservation service, how many of them fail in the preservation process and how long it takes for the digital preservation service to process the submitted packages.
  * The script enqueues a given number of `download_object` tasks.
* `download_object`
  * The workflow task downloads the object and the related files from MuseumPlus database.
  * The task is executed **automatically by a free RQ worker**.
  * The task automatically enqueues the next task `create_sip` if successful. Otherwise, the task will fail and related error information is collected by RQ.
* `create_sip`
  * The workflow task collects the downloaded object and packages it into a SIP.
  * The task is executed **automatically by a free RQ worker**.
  * The task automatically enqueues the next task `submit_sip` if successful. Otherwise, the task will fail and related error information is collected by RQ. Additional logs are also available in the object directory.
* `submit_sip`
  * The workflow task uploads the packaged SIP into the digital preservation service. The locally packaged SIP is deleted after the SIP has been uploaded.
  * The task is executed **automatically by a free RQ worker**.
  * The task doesn't automatically enqueue the next task immediately.
* `sync_processed_sips`
  * The script checks for accepted and rejected SIPs using the digital preservation service REST API. The submission report is downloaded.
  * The script is designed to be called **automatically (eg. once every hour)**.
  * The script automatically enqueues the next task `confirm_sip`, using different parameters depending on whether the SIP was accepted or rejected.
* `confirm_sip`
  * The workflow task confirms the SIP submission. The local database is updated, the submission report and local logs are archived. If the SIP submission failed, administrator will have to restart the process using `redownload_script`.
  * The task is executed **automatically by a free RQ worker**.
  * The script is the **final step in the workflow** if the SIP was accepted.
* `reenqueue_object`
  * The script restarts the preservation process from the `download_object` task.
  * The script is designed to be called **manually by an administrator**, who will determine when the preservation process can be retried. This may involve waiting for the digital preservation service to fix a bug that is preventing an object from being accepted into the service.
  * The script automatically enqueues the next task `download_object`.
* `freeze_objects`
  * The script freezes object(s), preventing them from entering the preservation workflow. This can be done if an object can't be packaged into a SIP for the time being due to some issue (eg. issue with a certain file).
  * The script is designed to be called **manually by an administrator**. In addition, some common situations (eg. file format is not supported for preservation) are handled automatically by the workflow.

Workflow components
-------------------

The workflow consists of the following servers

* Database server
  * Database (PostgreSQL)
    * Contains information about MuseumPlus objects to determine which objects have been preserved and which objects are candidates for preservation.
  * Redis
    * In-memory data structure store. Used to process data for the RQ job queue due to its simplicity and low requirements.
    * Also used for distributed locking.

* Worker server
  * RQ worker
    * Polls the Redis server on the database server for tasks. Amount of workers per task (eg. `download_object`, `create_sip`) can be configured. For example, more workers can be assigned for `create_sip` if the server is downloading objects fast enough to saturate the download bandwidth and there are leftover CPU resources for creating SIPs.

At the moment, database server and worker server can be separate.
Only one database server can exist (and is probably ever necessary due to low storage and performance requirements).
Multiple worker servers can be used if the servers use the same network storage for downloading and processing objects. For example, a SMB mount could be used for this.

Database schema
---------------

Alembic is used for tracking database schema changes and creating automatic database migrations.

For example, after changing the database tables and/or fields, you can run the following command to create a new migration:

```
alembic revision --autogenerate -m 'add MuseumObject.new_field'
```

Note that the auto-generated migrations may not work as-is. Make sure to check the created migration and update it as necessary.

Usage
=====

Enqueuing objects
-----------------

Once *Passari Workflow* has started synchronizing objects from MuseumPlus and discovered objects that are pending preservation, you can start enqueuing objects into the preservation workflow.

For example, to enqueue 200 objects into the workflow, run the following command:

.. code-block:: console

   $ enqueue-objects --object-count 200

This will queue RQ jobs for execution, which will automatically download each object, package them, submit them into the DPRES service and eventually confirm the SIP as either accepted or rejected.

Database migrations
-------------------

After updating your *Passari Workflow* installation, you might need to perform a database migration. This is done by activating the virtualenv you created in the same location as the original repository, and running the following command:

.. code-block:: console

   $ alembic upgrade head

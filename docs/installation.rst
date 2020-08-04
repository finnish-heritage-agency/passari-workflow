Installation
============

Installation using virtualenv
-----------------------------

To get started, ensure that Python 3.6+ is installed. On CentOS 7, you can usually get started by installing the required tools using `yum`:

.. code-block:: console

    $ yum install python36-libs python36-devel

Clone the *passari-workflow* repository and create a Python 3.6 virtualenv. Install *Passari* first and configure it; after this you can install *Passari Workflow* in the same *virtualenv*.

.. warning::

   You can install *Passari Workflow* with pip without cloning the repository, but you won't be able to perform database migrations without it.

.. code-block:: console

   $ git clone https://github.com/finnish-heritage-agency/passari-workflow.git
   $ cd passari-workflow
   $ git checkout 1.1 # newer version may be available, check with `git tag`
   $ python3.6 -mvenv venv
   $ source venv/bin/activate
   # Install Passari 1.1. Replace 1.1 with newer version if available.
   $ pip install --upgrade git+https://github.com/finnish-heritage-agency/passari.git@1.1#egg=passari

Configure *Passari* as detailed in the documentation for that application; *Passari Workflow* itself is only responsible for handling the workflow. After this is done, you can install *Passari Workflow*:

.. code-block:: console

   $ pip install .

After this, you can continue by configuring and setting up the workflow.

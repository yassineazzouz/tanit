.. rst-class:: hide-header

Welcome to Tanit
================

.. image:: _static/tanit-logo.png
    :align: center
    :scale: 50%

Tanit is a distributed, fast, and reliable data transfer service for efficiently moving large amounts of data across different storage technologies. It uses a distributed master worker architecture, to allow very fast data transfer speed and horizontal scaling, while implementing multiple failover, recovery and data consistency checks mechanisms to ensure robustness, fault tolerance and reliability.

Tanit aims to address the multiple challenges related to moving data between different storage technologies :

- How to workaround the heterogeneity of the different systems ?
- How to scale the process to large amounts of data ?
- How to ensure the correctness and consistency of the data ?
- How to reduce the cost of the transfer ?
- How to mirror datasets, and transfer data only when necessary ?
- ...

Basic use cases include but are not limited to :

- Data migration (across storage technologies).
- Data Replication and mirroring.
- Backup and Disaster recovery.
- Data life cycle management.

Documentation
-------------

.. toctree::
   :maxdepth: 2

   architecture
   quickstart
   setup
   configuration
   
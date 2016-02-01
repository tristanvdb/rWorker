rWorker
=======

This is yet another framework to distribute computation accross mutiple machines.
It uses Python's multiprocessing module.
At its core is an extension of Pyhton's multiprocessing.managers which can take two different roles:
 * master: server receiving Job from clients and exposing them to workers
 * slave: associated to a worker or a client it connects to the master

The manager launches workers on one or more nodes (each worker can have multiple processes).
Clients submit Jobs to the manager.

### TODO

 * Next:
    * packaging
    * clean termination of workers
    * manager launched workers on remote hosts
    * making Job's function available to worker live (might get really hard)
 * Features:
    * ...


## Learn integration patterns with NATS ##

Master/Slave Pattern

To run:

1. Setup 

    $ make up 
    
2. Display how components work together, open 3 terminals 

    $ tail-worker1

    $ tail-master 

    $ tail-fileserver

3. To repeat the transactions, restart the master and worker

    $ make restart

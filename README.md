qpid-spf
========

Shortest-Path Federation for Apache Qpid

This project is a proof-of-concept for automatic routing of messages using the
Apach Qpid message broker.

This code repository is a snapshot of the Apache Qpid C++ broker at release 0.22
with the Shortest-Path-Federation additions included.  Because this project was
intended to be a proof-of-concept, it was never contributed upstream to the Qpid
project.  Rather, the work has been continued under Apache Qpid as a subproject
called [Qpid Dispatch Router](http://qpid.apache.org/components/dispatch-router/index.html).


Building the Code
=================

For prerequisites to the build, please refer to the
[Qpid INSTALL File](http://svn.apache.org/repos/asf/qpid/tags/0.22/qpid/cpp/INSTALL)
from the Qpid project.

From the qpid-spf directory:

 - mkdir build
 - cd build
 - cmake -DBUILD_AMQP=OFF \
         -DBUILD_HA=OFF \
         -DBUILD_LEGACYSTORE=OFF \
         -DBUILD_RDMA=OFF \
         -DBUILD_SSL=OFF \
         -DBUILD_XML=OFF \
         ../cpp
 - make


Running the Demonstration
=========================


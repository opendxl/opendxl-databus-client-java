Welcome to OpenDXL Databus Java Client Library
----------------------------------------------

Overview
--------

The OpenDXL Databus Java client library is used to consume and produce
records from/to a `Data Exchange
Layer <http://www.mcafee.com/us/solutions/data-exchange-layer.aspx>`__
(DXL) Streaming Service.

The client requires Java Development Kit 8 (JDK 8) or later.

Installation
------------

To start using the OpenDXL Databus Java Client Library:

-  Download the `Latest
   Release <https://github.com/opendxl/opendxl-databus-client-java/releases/latest>`__
-  Extract the release .zip file
-  View the ``README.html`` file located at the root of the extracted
   files.
-  The ``README`` links to the documentation which includes installation
   instructions, API details, and samples.
-  The SDK documentation is also available on-line
   `here <https://opendxl.github.io/opendxl-databus-client-java/docs/javadoc/index.html>`__.

Maven Repository
----------------

Visit the `OpenDXL Databus Java Client Maven Repository <https://search.maven.org/artifact/com.opendxl/dxldatabusclient>`__
for access to all released versions including the appropriate dependency
syntax for a large number of management systems (Maven, Gradle, SBT, Ivy, Grape, etc.).

Maven:

.. code:: xml

    <dependency>
      <groupId>com.opendxl</groupId>
      <artifactId>dxldatabusclient</artifactId>
      <version>2.4.6</version>
    </dependency>

or Gradle:

.. code:: groovy

    compile 'com.opendxl:dxldatabusclient:2.4.6'

API Documentation
-----------------

* `JavaDoc API Documentation <javadoc/index.html>`_

CLI (Command Line Interface)
----------------------------

.. toctree::
        :maxdepth: 1

        CLI-Example.rst


Samples
-------

Basic

.. toctree::
	:maxdepth: 1

        Basic-producer-consumer-example.rst
        Producer-metrics-sample.rst
        Consumer-metrics-sample.rst
        Transactions-producer-consumer-example.rst
        Basic-push-consumer-example.rst

Bugs and Feedback
-----------------

For bugs, questions and discussions please use the `GitHub
Issues <https://github.com/opendxl/opendxl-databus-client-java/issues>`__.

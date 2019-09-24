Welcome to OpenDXL Databus Java Client Library
----------------------------------------------

Overview
--------

The OpenDXL Databus Java client library is used to consume and produce
records from/to a `Data Exchange
Layer <http://www.mcafee.com/us/solutions/data-exchange-layer.aspx>`__
(DXL) Streaming Service.

Documentation
-------------

The OpenDXL Databus Java client library is used to consume and produce
records from/to a `Data Exchange
Layer <http://www.mcafee.com/us/solutions/data-exchange-layer.aspx>`__
(DXL) Streaming Service.

See the `OpenDXL Databus Java Client Library
Documentation <https://opendxl.github.io/opendxl-databus-client-java/docs/index.html>`__
for API documentation and examples.

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

| Visit the `OpenDXL Databus Java Client Maven
  Repository <https://search.maven.org/artifact/com.opendxl/dxldatabus>`__
  for
| access to all released versions including the appropriate dependency
  syntax for a large number of management
| systems (Maven, Gradle, SBT, Ivy, Grape, etc.).

Maven:

.. code:: xml

    <dependency>
      <groupId>com.opendxl.databus</groupId>
      <artifactId>opendxldatabusclient-java-sdk</artifactId>
      <version>0.1.1</version>
    </dependency>

or Gradle:

.. code:: groovy

    compile 'com.opendxl.streaming:opendxldatabusclient-java-sdk:0.1.1'

Java SDK Overview and Prerequisites
-----------------------------------

.. toctree::
	:maxdepth: 1

        Overview.rst
        architecture
        features
	Prerequisites.rst

API Documentation
-----------------

* `JavaDoc API Documentation <javadoc/index.html>`_

Samples
-------

Basic

.. toctree::
	:maxdepth: 1

	Basic-producer-consumer-example.rst

.. toctree::
	:maxdepth: 1

	Producer-metrics-sample.rst

.. toctree::
	:maxdepth: 1

	Consumer-metrics-sample.rst

.. toctree::
        :maxdepth: 1

        CLI-Example.rst

.. toctree::
        :maxdepth: 1

        Transactions-producer-consumer-example.rst

Bugs and Feedback
-----------------

| For bugs, questions and discussions please use the
| `GitHub
  Issues <https://github.com/opendxl/opendxl-databus-client-java/issues>`__.

LICENSE
-------

Copyright 2019 McAfee, LLC

Licensed under the Apache License, Version 2.0 (the "License"); you may
not use this file except in compliance with the License. You may obtain
a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

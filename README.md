# IcarusEdgeSim – An edge computing simulator based on Icarus 

IcarusEdgeSim is a Python-based discrete-event simulator for evaluating the 
performance of both networks with both cache and compute resources based on 
the Icarus simulator (https://github.com/icarus-sim/icarus) for ICN caching networks. 

Icarus is not bound to any specific ICN or edge-computing architecture. Its design allows users
to implement and evalute new caching and computing policies or routing strategies
with few lines of code.

The code is in general self-explanatory. Below are some notes on how to start experimenting with IcarusEdgeSim. 

## Configuring Experiment Scenarios:

You can find sample configurations in the /examples folder. 

A typical configuration contains information on the scenario to be executed. A general scenario includes  
a network topology, workload, placement of caches, placement of data producers, and most importantly
a strategy. A strategy determines how requests are processed (i.e., routed from source to resources and possibly back), 
a workload determines the arrival rate and placement of initial requests. 

### Strategies:

Example strategies are located under models/strategy/. The service.py in the examples contains strategies for edge-computing implemented 
for the following papers:

 * In [Uncoordinated placement for edge-clouds](http://discovery.ucl.ac.uk/10027134/1/Pavlou_Ascig-17-cloudcom.pdf)
 * In [Fogspot: Spot pricing for application provisioning in edge/fog computing](https://www.researchgate.net/publication/330609355_FogSpot_Spot_Pricing_for_Application_Provisioning_in_EdgeFog_Computing)

###Cache resource configuration:

These configurations include placement of caches and content (see ./scenarios/cacheplacement.py and ./scenarios/contentplacement.py), total storage budget (in number of content), total number of contents. A cache model comprises policies that manage how content is replaces under ./models/cache/ containing replacement policies. The placement and retrieval of content to/from caches at run-time is determined by a strategy.

### Compute resources configuration:

These configurations resemble caching configurations and include number of functions, total computation budget (in number of CPU cores) and computation placement. The placement setting locates a set of  **computation spots**, i.e., Cloudlets, (see ./models/service/compSpoy.py) which comprises a number of CPU cores and VMs or containeras, in the topology. Each function is associated with a service rate, i.e., processing time per input data chunk. 

A number of policies dictate how computation spots are managed. An admission policy determines how a node decides whether or not to accept a request for execution. A scheduling policy determines the order of execution for admitted requests. A strategy determines the placement of functions (mapping of VMs to function) for each computation spot. 

### Topologies:

The topology determines the connectivity of the network. Available topologies include measurement-based
ones such as Rocketfuel (please see /resources/topologies). 

This document explains how to configure and run the simulator.

## Download and installation

### Prerequisites
Before using the simulator, you need to install all required dependencies.

#### Ubuntu 13.10+
If you use Ubuntu (version 13.10+) you can run the script `ubuntusetup.sh`
located in the `scripts` folder which will take of installing all the
dependencies. To run it, executes the following commands

    $ cd <YOUR ICARUSEDGESIM FOLDER>
    $ sh scripts/ubuntusetup.sh

The script, after being launched, will ask you for superuser password.

Finally, it is advisable to add IcarusEdgeSim path to the PYTHONPATH environment variable. This makes it possible to launch IcarusEdgeSim from outside the IcarusEdgeSim root directory or call IcarusEdgeSim APIs from other programs:

    $ cd <YOUR ICARUSEDGESIM FOLDER>
    $ export PYTHONPATH=`pwd`:$PYTHONPATH

Note however that setting the PYTHONPATH this way does not persist across reboots. To make it persist you should add the export instruction to a script that your machine executes at boot or login time, e.g. `.bashrc` (if you use Bash).

#### Other operating systems
If you have other operating systems, you can install all dependencies manually. 

IcarusEdgeSim dependencies are:

* **Python interpreter (2.7.x)**: you can either download it
  from the [Python website](http://www.python.org) or, possibly, from the package
  manager of your operating system.
* The following Python packages: 
   * **numpy** (version 1.6 onwards)
   * **scipy** (version 0.12 onwards)
   * **matplotlib** (version 1.2 onwards)
   * **networkx** (version 1.6 onwards)
   * **fnss** (version 0.5.1 onwards)

All these packages can be installed using either [`easy_install`](http://pythonhosted.org/setuptools/easy_install.html) or [`pip`](http://www.pip-installer.org/en/latest/) utilities.

If you use `pip` run:

    $ pip install numpy scipy matplotlib networkx fnss

If you use `easy_install` run:

    $ easy_install numpy scipy matplotlib networkx fnss

You may need to run `pip` or `easy_install` as superuser. The installation of these packages, especially `numpy` and `scipy` may also require to install additional libraries.

#### Virtual machine
You can also run IcarusEdgeSim within a virtual machine. [This repository](https://github.com/icarus-sim/icarus-vm) contains scripts and documentation to set up a virtual machine with IcarusEdgeSim and all its dependencies.


### Download
You can download a stable release in a zip or tar.gz format using the links below.

**Latest version:**

 * Version 0.6.0: \[[zip](https://github.com/icarus-sim/icarus/archive/v0.6.0.zip)\] \[[tar.gz](https://github.com/icarus-sim/icarus/archive/v0.6.0.tar.gz)\]

**Older versions:**

 * Version 0.5.0: \[[zip](https://github.com/icarus-sim/icarus/archive/v0.5.0.zip)\] \[[tar.gz](https://github.com/icarus-sim/icarus/archive/v0.5.0.tar.gz)\]
 * Version 0.4.0: \[[zip](https://github.com/icarus-sim/icarus/archive/v0.4.0.zip)\] \[[tar.gz](https://github.com/icarus-sim/icarus/archive/v0.4.0.tar.gz)\]
 * Version 0.3.0: \[[zip](https://github.com/icarus-sim/icarus/archive/v0.3.0.zip)\] \[[tar.gz](https://github.com/icarus-sim/icarus/archive/v0.3.0.tar.gz)\]
 * Version 0.2.1: [\[zip\]](https://github.com/icarus-sim/icarus/archive/v0.2.1.zip) [\[tar.gz\]](https://github.com/icarus-sim/icarus/archive/v0.2.1.tar.gz)
 * Version 0.2: [\[zip\]](https://github.com/icarus-sim/icarus/archive/v0.2.zip) [\[tar.gz\]](https://github.com/icarus-sim/icarus/archive/v0.2.tar.gz)
 * Version 0.1.1: [\[zip\]](https://github.com/icarus-sim/icarus/archive/v0.1.1.zip) [\[tar.gz\]](https://github.com/icarus-sim/icarus/archive/v0.1.1.tar.gz)
 * Version 0.1.0: [\[zip\]](https://github.com/icarus-sim/icarus/archive/v0.1.zip) [\[tar.gz\]](https://github.com/icarus-sim/icarus/archive/v0.1.tar.gz)

You can also get the development branch from the Github repository using Git. Just open a shell, `cd` to the directory where you want to download the simulator and type:

    $ git clone https://github.com/icarus-sim/icarus.git

## Usage

### Run simulations

To use IcarusEdgeSim with the currently implemented topologies and models of caching and computing policies and strategies you need to do the following.

First, create a configuration file with all the desired parameters of your
simulation. You can modify the file `config.py`, which is a well documented
example configuration. You can even use the configuration file as it is just
to get started. Alternatively, have a look at the `examples` folder which
contains examples of configuration files for various use cases.

Second, run Icarus by running the script `icarus.py` using the following syntax

    $ python icarus.py --results RESULTS_FILE CONF_FILE

where:

 * `RESULTS_FILE` is the [pickle](http://docs.python.org/3/library/pickle.html) file in which results will be saved,
 * `CONF_FILE` is the configuration file

Example usage could be:

    $ python icarus.py --results results.pickle config.py

After saveing the results in pickle format you can extract them in a human
readable format using the `printresults.py` script from the `scripts` folder. Example usage could be:

    $ python scripts/printresults.py results.pickle > results.txt

Icarus also provides a set of helper functions for plotting results. Have a look at the `examples`
folder for plot examples.

By executing the steps illustrated above it is possible to run simulations using the
topologies, cache policies, strategies and result collectors readily available on
Icarus. Icarus makes it easy to implement new models to use in simulations.

To implement new models, please refer to the description of the original Icarus simulator 
provided in this paper:

L.Saino, I. Psaras and G. Pavlou, Icarus: a Caching Simulator for Information Centric
Networking (ICN), in Proc. of SIMUTOOLS'14, Lisbon, Portugal, March 2014.
\[[PDF](http://www.ee.ucl.ac.uk/~lsaino/publications/icarus-simutools14.pdf)\],
\[[Slides](http://www.ee.ucl.ac.uk/~lsaino/publications/icarus-simutools14-slides.pdf)\],
\[[BibTex](http://www.ee.ucl.ac.uk/~lsaino/publications/icarus-simutools14.bib)\]

Otherwise, please browse the source code. It is very well documented and easy to
understand.


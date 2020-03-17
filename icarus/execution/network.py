# -*- coding: utf-8 -*-
"""Network Model-View-Controller (MVC)

TODO: Main Edge network data placement manager ("ERP") - to be implemented
    appropriately, with a few placement strategies, probably, or a new
    class for these specific data storage placement strategies, using the
    inputs from service feedback, network updates, system-wide status updates
    and service provision updates.

This module contains classes providing an abstraction of the network shown to
the strategy implementation. The network is modelled using an MVC design
pattern.

A strategy performs actions on the network by calling methods of the
`NetworkController`, that in turns updates  the `NetworkModel` instance that
updates the `NetworkView` instance. The strategy can get updated information
about the network status by calling methods of the `NetworkView` instance.

The `NetworkController` is also responsible to notify a `DataCollectorProxy`
of all relevant events.
"""
import random
import logging
import sys

import networkx as nx
import fnss

import heapq

from icarus.registry import CACHE_POLICY
from icarus.util import path_links, iround
from icarus.models.service.compSpot import ComputationSpot
from icarus.models.service.compSpot import Task
from collections import Counter

__all__ = [
    'Service',
    'Event',
    'NetworkModel',
    'NetworkView',
    'NetworkController'
          ]

logger = logging.getLogger('orchestration')

class Event(object):
    """Implementation of an Event object: arrival of a request to a node"""

    def __init__(self, time, receiver, service, node, flow_id, deadline, rtt_delay, status, task=None):
        """Constructor
        Parameters
        ----------
        time : Arrival time of the request
        node : Node that the request arrived
        deadline : deadline of the request
        flow_id : the id of the flow that the request is belong to
        """
        self.time = time
        self.receiver = receiver
        self.node = node
        self.service = service
        self.flow_id = flow_id
        self.deadline = deadline
        self.rtt_delay = rtt_delay
        self.status = status
        self.task = task

    def __lt__(self, other):
        return self.time < other.time
        #return cmp(self.time, other.time)

class Service(object):
    """Implementation of a service object"""

    def __init__(self, service_time=None, deadline=None):
        """Constructor
        Parameters
        ----------
        service_time : computation time to process a request and produce results
        deadline : the total amount of time (taking into account the computational
            and network delays) to process the request for this service once the request
            leaves the user, for an acceptable level of QoS.
        """

        self.service_time = service_time
        self.deadline = deadline

def symmetrify_paths(shortest_paths):
    """Make paths symmetric

    Given a dictionary of all-pair shortest paths, it edits shortest paths to
    ensure that all path are symmetric, e.g., path(u,v) = path(v,u)

    Parameters
    ----------
    shortest_paths : dict of dict
        All pairs shortest paths

    Returns
    -------
    shortest_paths : dict of dict
        All pairs shortest paths, with all paths symmetric

    Notes
    -----
    This function modifies the shortest paths dictionary provided
    """
    for u in shortest_paths:
        for v in shortest_paths[u]:
            shortest_paths[u][v] = list(reversed(shortest_paths[v][u]))
    return shortest_paths


class NetworkView(object):
    """Network view

    TODO: Very important part of the ERP system view, for updating storage (and
        service) allocation within the local EDR environment. To be used in
        conjunction with the adapted "optimal placement" strategy, for the data.

    This class provides an interface that strategies and data collectors can
    use to know updated information about the status of the network.
    For example the network view provides information about shortest paths,
    characteristics of links and currently cached objects in nodes.
    """

    def __init__(self, model):
        """Constructor

        Parameters
        ----------
        model : NetworkModel
            The network model instance
        """
        if not isinstance(model, NetworkModel):
            raise ValueError('The model argument must be an instance of '
                             'NetworkModel')
        self.model = model
        for node in model.compSpot.keys():
            model.compSpot[node].view = self
            model.compSpot[node].node = node

    def service_locations(self, service_labels=Counter()):
        """Return a set of all current locations of a specific content.

                This include both persistent content sources and temporary caches.

                Parameters
                ----------
                k : any hashable type
                    The function identifier

                service_labels: any function and/or content topics
                    Topics identifiers, as array

                Returns
                -------
                nodes : set
                    A set of all nodes currently storing the given content
        TODO implement this - INDEED!
        """

    def content_locations(self, k):
        """Return a set of all current locations of a specific content.

        TODO: Add MOST_POPULAR_STORAGE_ and _REQUEST_LABELS, which return
            the nodes which have the most hits of certain labels, through
            the "NetworkView" class.

        This include both persistent content sources and temporary caches.

        Parameters
        ----------
        k : any hashable type
            The content identifier

        Returns
        -------
        nodes : set
            A set of all nodes currently storing the given content
        """
        loc = set(v for v in self.model.cache if self.model.cache[v].has(k))
        # This ^ locates all the nodes which have that content (with that hash)
        # cached. TODO: Add a content type/topic_present method and a
        #               hashed_function_present method to the RepoStorage class,
        #               to check if that node has the requested item and keep note
        #               in the centralised mgmt system for that service hash/content
        #               topic. Add RepoStorage - node associations in NetworkModel!!!

        source = self.content_source(k)
        if source:
            loc.add(source)
        return loc

    def content_source(self, k):
        """Return the node identifier where the content is persistently stored.

        Parameters
        ----------
        k : any hashable type
            The content identifier

        Returns
        -------
        node : any hashable type
            The node persistently storing the given content or None if the
            source is unavailable
        """
        return self.model.content_source.get(k, None)

    def shortest_path(self, s, t):
        """Return the shortest path from *s* to *t*

        Parameters
        ----------
        s : any hashable type
            Origin node
        t : any hashable type
            Destination node

        Returns
        -------
        shortest_path : list
            List of nodes of the shortest path (origin and destination
            included)
        """
        return self.model.shortest_path[s][t]


    def num_services(self):
        """
        Returns
        ------- 
        the size of the service population
        """
        return self.model.n_services

    def all_pairs_shortest_paths(self):
        """Return all pairs shortest paths

        Return
        ------
        all_pairs_shortest_paths : dict of lists
            Shortest paths between all pairs
        """
        return self.model.shortest_path

    def cluster(self, v):
        """Return cluster to which a node belongs, if any

        Parameters
        ----------
        v : any hashable type
            Node

        Returns
        -------
        cluster : int
            Cluster to which the node belongs, None if the topology is not
            clustered or the node does not belong to any cluster
        """
        if 'cluster' in self.model.topology.node[v]:
            return self.model.topology.node[v]['cluster']
        else:
            return None

    def link_type(self, u, v):
        """Return the type of link *(u, v)*.

        Type can be either *internal* or *external*

        Parameters
        ----------
        u : any hashable type
            Origin node
        v : any hashable type
            Destination node

        Returns
        -------
        link_type : str
            The link type
        """
        return self.model.link_type[(u, v)]

    def link_delay(self, u, v):
        """Return the delay of link *(u, v)*.

        Parameters
        ----------
        u : any hashable type
            Origin node
        v : any hashable type
            Destination node

        Returns
        -------
        delay : float
            The link delay
        """
        return self.model.link_delay[(u, v)]

    def path_delay(self, s, t):
        """Return the delay from *s* to *t*

        Parameters
        ----------
        s : any hashable type
            Origin node
        t : any hashable type
            Destination node
        Returns
        -------
        delay : float
        """
        path = self.shortest_path(s, t)
        delay = 0.0
        for indx in range(0, len(path)-1):
            delay += self.link_delay(path[indx], path[indx+1])

        return delay

    def topology(self):
        """Return the network topology

        Returns
        -------
        topology : fnss.Topology
            The topology object

        Notes
        -----
        The topology object returned by this method must not be modified by the
        caller. This object can only be modified through the NetworkController.
        Changes to this object will lead to inconsistent network state.
        """
        return self.model.topology

    def eventQ(self):
        """Return the event queue
        """

        return self.model.eventQ

    def getRequestRate(self):
        """Return the request rate per second of the aggregate traffic 
        """
        return self.model.rate

    def services(self):
        """Return the services list (i.e., service population)
        """

        return self.model.services

    def compSpot(self, node):
        """Return the computation spot at a given node
        """

        return self.model.compSpot[node]

    def service_nodes(self):
        """Return
        a dictionary consisting of only the nodes with computational spots
        the dict. maps node to its comp. spot

        TODO: EXTEND TO RepoStorage AND labels!
            Also NOTE: This CompSpot return IS NOT SERVICE-SPECIFIC!!!!!!!
            NOTE ON NOTE: Check has_computationalSpot and has_service defs
            below!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
        """

        return self.model.compSpot

    def cache_nodes(self, size=False):
        """Returns a list of nodes with caching capability

        Parameters
        ----------
        size: bool, opt
            If *True* return dict mapping nodes with size

        Returns
        -------
        cache_nodes : list or dict
            If size parameter is False or not specified, it is a list of nodes
            with caches. Otherwise it is a dict mapping nodes with a cache
            and their size.
        """
        return {v: c.maxlen for v, c in self.model.cache.items()} if size \
                else list(self.model.cache.keys())

    def has_cache(self, node):
        """Check if a node has a content cache.

        Parameters
        ----------
        node : any hashable type
            The node identifier

        Returns
        -------
        has_cache : bool,
            *True* if the node has a cache, *False* otherwise
        """
        return node in self.model.cache

    def has_computationalSpot(self, node):
        """Check if a node is a computational spot.

        Parameters
        ----------
        node : any hashable type
            The node identifier

        Returns
        -------
        has_computationalSpot : bool,
            *True* if the node has a computational spot, *False* otherwise
        """
        return node in self.model.compSpot

    def has_service(self, node, service):
        """Check if a node is a computational spot and is running a service instance

        Parameters
        ----------
        node : any hashable type
            The node identifier

        Returns
        -------
        has_service : bool,
            *True* if the node is running the service, *False* otherwise
        """

        if self.has_computationalSpot(node):
            cs = self.model.compSpot[node]
            if cs.is_cloud:
                return True
            elif cs.numberOfVMInstances[service] > 0:
                return True

        return False

    def cache_lookup(self, node, content):
        """Check if the cache of a node has a content object, without changing
        the internal state of the cache.

        This method is meant to be used by data collectors to calculate
        metrics. It should not be used by strategies to look up for contents
        during the simulation. Instead they should use
        `NetworkController.get_content`

        TODO: Provide another means of showing topics/labels, for RepoStorage
            in nodes associated with RepoStorage. Provide: node with most of
            one label, node(s) with the most relevant SET of labels, THEN
            (more complex) most relevant SET of labels with highest freshness
            period/shelf-life; could MOST AND LEAST ACCESSED RepoStorage labels
            be checked???!!!

        Parameters
        ----------
        node : any hashable type
            The node identifier
        content : any hashable type
            The content identifier

        Returns
        -------
        has_content : bool
            *True* if the cache of the node has the content, *False* otherwise.
            If the node does not have a cache, return *None*
        """
        if node in self.model.cache:
            return self.model.cache[node].has(content)

    def local_cache_lookup(self, node, content):
        """Check if the local cache of a node has a content object, without
        changing the internal state of the cache.

        The local cache is an area of the cache of a node reserved for
        uncoordinated caching. This is currently used only by hybrid
        hash-routing strategies.

        This method is meant to be used by data collectors to calculate
        metrics. It should not be used by strategies to look up for contents
        during the simulation. Instead they should use
        `NetworkController.get_content_local_cache`.

        Parameters
        ----------
        node : any hashable type
            The node identifier
        content : any hashable type
            The content identifier

        Returns
        -------
        has_content : bool
            *True* if the cache of the node has the content, *False* otherwise.
            If the node does not have a cache, return *None*
        """
        if node in self.model.local_cache:
            return self.model.local_cache[node].has(content)
        else:
            return False

    def cache_dump(self, node):
        """Returns the dump of the content of a cache in a specific node

        Parameters
        ----------
        node : any hashable type
            The node identifier

        Returns
        -------
        dump : list
            List of contents currently in the cache
        """
        if node in self.model.cache:
            return self.model.cache[node].dump()

    def most_popular_request_node(self, request_labels=Counter()):
        self.sess_latency = 0.0
        self.session[request_labels] = request_labels

    def most_storage_labels_node(self, flow_id=0, storage_labels=Counter()):
        # TODO: Maybe storage labels should rather be dictionaries, with only one entry,
        #       and keep being updated?! (keeping only the request labels as counters)
        self.sess_latency = 0.0
        self.flow_cloud[flow_id] = False
        self.session[storage_labels] = storage_labels

    def deadline_sensitive_requests(self, flow_id, deadline_min=0, deadline_max=0):
        """
        Parameters
        ----------
        flow_id : dict, optional
            The ID of the request and associated flow
        deadline_min : any hashable type
            Origin node
        deadline_max : any hashable type
            Destination node
        """

        pass
class NetworkModel(object):
    """Models the internal state of the network.

    This object should never be edited by strategies directly, but only through
    calls to the network controller.

    TODO: This is where all the node characteristics are determined, through the "stack" property.
        Look into this, how it is initiated (discovered in cacheplacement, so maybe start with that)
        and develop on that, to associate RepoStorage objects and see how messages could be better
        "instantiated".
    """

    def __init__(self, topology, cache_policy, sched_policy, n_services, rate, seed=0, shortest_path=None):
        """
            TODO: Check line 589! That is where the caches are initialised,
                for each node!
        """

        """Constructor

        Parameters
        ----------
        topology : fnss.Topology
            The topology object
        cache_policy : dict or Tree
            cache policy descriptor. It has the name attribute which identify
            the cache policy name and keyworded arguments specific to the
            policy
        shortest_path : dict of dict, optional
            The all-pair shortest paths of the network
        """
        # Filter inputs
        if not isinstance(topology, fnss.Topology):
            raise ValueError('The topology argument must be an instance of '
                             'fnss.Topology or any of its subclasses.')

        # Shortest paths of the network
        self.shortest_path = shortest_path if shortest_path is not None \
                             else symmetrify_paths(nx.all_pairs_dijkstra_path(topology))

        # Network topology
        self.topology = topology
        self.topology_depth = 0

        # Dictionary mapping each content object to its source
        # dict of location of contents keyed by content ID
        self.content_source = {}
        # Dictionary mapping the reverse, i.e. nodes to set of contents stored
        self.source_node = {}

        # TODO: ^^^ Need to map labels as contents are mapped above, also the
        #  other way around. These dicts should be much more extensive than in
        #  the case of contents, especially since only one content hash normally
        #  only has a one-to-one mapping and more content hashes mapped to one node
        #  have many-to-one mappings, being easier to manage as dicts. In our case,
        #  labels may have one-to-many mappings both ways, making the choice of
        #  mapping much better, but the choice mechanism more complicated, however
        #  making it simpler, the more labels (restrictive) the mechanism has to
        #  choose/filter. Should we also include a label-to-content mapping? - nope,
        #  nevermind - they already are attached to "contents"

        # Dictionary mapping each node to sets of labels stored in each node
        # dict of stored labels, keyed by node location, with label counters,
        # counting number of labels with that name for the node
        self.node_labels = {}
        # Dictionary mapping the reverse, i.e. content labels to their storing node(s)
        # dict of locations of labels keyed by label name, with the number of labels of
        # that label/key stored in each of these nodes
        self.labels_node = {}

        # A heap with events (see Event class above)
        self.eventQ = []

        # Dictionary of link types (internal/external)
        self.link_type = nx.get_edge_attributes(topology, 'type')
        self.link_delay = fnss.get_delays(topology)
        # Instead of this manual assignment, I could have converted the
        # topology to directed before extracting type and link delay but that
        # requires a deep copy of the topology that can take long time if
        # many content source mappings are included in the topology
        if not topology.is_directed():
            for (u, v), link_type in list(self.link_type.items()):
                self.link_type[(v, u)] = link_type
            for (u, v), delay in list(self.link_delay.items()):
                self.link_delay[(v, u)] = delay

        cache_size = {}
        storageSize = {}
        comp_size = {}
        service_size = {}
        all_node_labels = Counter()
        self.rate = rate
        for node in topology.nodes():
            stack_name, stack_props = fnss.get_stack(topology, node)
            # get the depth of the tree
            if stack_name == 'router' and 'depth' in self.topology[node].keys():
                depth = self.topology.nodes[node]['depth']
                if depth > self.topology_depth:
                    self.topology_depth = depth
            # get computation size per depth
            if stack_name == 'router':
                if 'cache_size' in stack_props:
                    cache_size[node] = stack_props['cache_size']
                if 'storageSize' in stack_props:
                    storageSize[node] = stack_props['storageSize']
                if 'computation_size' in stack_props:
                    comp_size[node] = stack_props['computation_size']
                if 'service_size' in stack_props:
                    service_size[node] = stack_props['service_size']
            elif stack_name == 'source': # A Cloud with infinite resources
                comp_size[node] = float('inf')
                service_size[node] = float('inf')

                contents = stack_props['contents']
                for c in contents:
                    for labels in c['labels']:
                        all_node_labels.update([labels])

                self.source_node[node] = contents
                for content in contents:
                    self.content_source[content] = node

                self.labels_node[node].update(all_node_labels)
                for k, v in all_node_labels:
                    Counter(self.content_source[k]).update([node, v])

        if any(c < 1 for c in cache_size.values()):
            logger.warn('Some content caches have size equal to 0. '
                        'I am setting them to 1 and run the experiment anyway')
            for node in cache_size:
                if cache_size[node] < 1:
                    cache_size[node] = 1

        policy_name = cache_policy['name']
        policy_args = {k: v for k, v in cache_policy.items() if k != 'name'}

        # The actual cache objects storing the content
        self.cache = {node: CACHE_POLICY[policy_name](cache_size[node], **policy_args)
                          for node in cache_size}

        # Generate the actual services processing requests
        self.services = []
        self.n_services = n_services
        internal_link_delay = 0.001 # This is the delay from receiver to router

        service_time_min = 0.10 # used to be 0.001
        service_time_max = 0.10 # used to be 0.1
        #delay_min = 0.005
        delay_min = 2*topology.graph['receiver_access_delay'] + service_time_max
        delay_max = delay_min + 2*topology.graph['depth']*topology.graph['link_delay'] + 0.005


        service_indx = 0
        random.seed(seed)
        for service in range(0, n_services):
            service_time = random.uniform(service_time_min, service_time_max)
            #service_time = 2*random.uniform(service_time_min, service_time_max)
            deadline = random.uniform(delay_min, delay_max) + 2*internal_link_delay
            #deadline = service_time + 1.5*(random.uniform(delay_min, delay_max) + 2*internal_link_delay)
            s = Service(service_time, deadline)
            #print ("Service " + str(service) + " has a deadline of " + str(deadline))
            self.services.append(s)
        #""" #END OF Generating Services

        ### Prepare input for the optimizer
        if False:
            aFile = open('inputToOptimizer.txt', 'w')
            aFile.write("# 1. ServiceIDs\n")
            first = True
            tostr = ""
            for service in range(0, n_services):
                if first:
                    tostr += str(service)
                    first = False
                else:
                    tostr += "," + str(service)
            aFile.write(s)

            aFile.write("# 2. Set of APs:\n")
            first = True
            tostr = ""
            for ap in topology.graph['receivers']:
                if first:
                    tostr = str(ap)
                    first = False
                else:
                    tostr += "," + str(ap)
            tostr += '\n'
            aFile.write(tostr)

            aFile.write("# 3. Set of nodes:\n")
            first = True
            tostr = ""
            for node in topology.nodes_iter():
                if node in topology.graph['receivers']:
                    continue
                if first:
                    tostr = str(node)
                    first = False
                else:
                    tostr = "," + str(node)
            tostr += '\n'
            aFile.write(tostr)

            aFile.write("# 4. NodeID, serviceID, numCores\n")
            if topology.graph['type'] == 'TREE':
                ap_node_to_services = {}
                ap_node_to_delay = {}
                for ap in topology.graph['receivers']:
                    node_to_delay = {}
                    node_to_services = {}
                    node_to_delay[ap] = 0.0
                    ap_node_to_services[ap] = node_to_services
                    ap_node_to_delay[ap] = node_to_delay
                    for node in topology.nodes_iter():
                        for egress, ingress in topology.edges_iter():
                            #print str(ingress) + " " + str(egress)
                            if ingress in node_to_delay.keys() and egress not in node_to_delay.keys():
                                node_to_delay[egress] = node_to_delay[ingress] + topology.edge[ingress][egress]['delay']
                                node_to_services[egress] = []
                                service_indx = 0
                                for s in self.services:
                                    if s.deadline >= (s.service_time + 2*node_to_delay[egress]):
                                        node_to_services[egress].append(service_indx)
                                    service_indx += 1
                aFile.write("# 4. Ap,Node,service1,service2, ....]\n")
                for ap in topology.graph['receivers']:
                    node_to_services = ap_node_to_services[ap]
                    node_to_delay = ap_node_to_delay[ap]
                    for node, services in node_to_services.items():
                        s = str(ap) + "," + str(node) #+ "," + str(node_to_delay[node])
                        for serv in services:
                            s += "," + str(serv)
                        s += '\n'
                        aFile.write(s)
                aFile.write("# 5. AP, rate_service1, rate_service2, ... rate_serviceN\n")
                rate = 1.0/(len(topology.graph['receivers'])*len(self.services))
                for ap in topology.graph['receivers']:
                    s = str(ap) + ","
                    for serv in self.services:
                        s += str(rate)
                    s += '\n'
                    aFile.write(s)

            aFile.close()
        ComputationSpot.services = self.services
        self.compSpot = {node: ComputationSpot(self, comp_size[node], service_size[node], self.services, node, sched_policy, None)
                            for node in comp_size}
        #print ("Generated Computation Spot Objects")
        sys.stdout.flush()
        # This is for a local un-coordinated cache (currently used only by
        # Hashrouting with edge cache)
        self.local_cache = {}

        # Keep track of nodes and links removed to simulate failures
        self.removed_nodes = {}
        # This keeps track of neighbors of a removed node at the time of removal.
        # It is needed to ensure that when the node is restored only links that
        # were removed as part of the node removal are restored and to prevent
        # restoring nodes that were removed manually before removing the node.
        self.disconnected_neighbors = {}
        self.removed_links = {}
        self.removed_sources = {}
        self.removed_caches = {}
        self.removed_local_caches = {}

class NetworkController(object):
    """Network controller

    This class is in charge of executing operations on the network model on
    behalf of a strategy implementation. It is also in charge of notifying
    data collectors of relevant events.

    TODO: Important! One of the main (if not THE MAIN) transmitters of data
        (and functions) across the network - the main network controller/
        data plane, it seems.
    """

    def __init__(self, model):
        """Constructor

        Parameters
        ----------
        model : NetworkModel
            Instance of the network model
        """
        self.session = {}
        self.model = model
        self.collector = None

    def attach_collector(self, collector):
        """Attach a data collector to which all events will be reported.

        Parameters
        ----------
        collector : DataCollector
            The data collector
        """
        self.collector = collector

    def detach_collector(self):
        """Detach the data collector."""
        self.collector = None

    def start_session(self, timestamp, receiver, content, labels, log, feedback, flow_id=0, deadline=0):
        """Instruct the controller to start a new session (i.e. the retrieval
        of a content).

        Parameters
        ----------
        timestamp : int
            The timestamp of the event
        receiver : any hashable type
            The receiver node requesting a content
        content : any hashable type
            The content identifier requested by the receiver
        labels :

        log : bool
            *True* if this session needs to be reported to the collector,
            *False* otherwise
        feedback: bool
            *True* if this session uses feedback for content placement and performance optimisation
            *False* otherwise
        """
        self.session[flow_id] = dict(timestamp=timestamp,
                            receiver=receiver,
                            content=content,
                            labels=labels,
                            log=log,
                            feedback=feedback,
                            deadline=deadline)

        self.sess_content = content

        #if self.collector is not None and self.session[flow_id]['log']:
        self.collector.start_session(timestamp, receiver, content, labels, feedback, flow_id, deadline)

    def forward_request_path(self, s, t, path=None, main_path=True):
        """Forward a request from node *s* to node *t* over the provided path.

        Parameters
        ----------
        s : any hashable type
            Origin node
        t : any hashable type
            Destination node
        path : list, optional
            The path to use. If not provided, shortest path is used
        main_path : bool, optional
            If *True*, indicates that link path is on the main path that will
            lead to hit a content. It is normally used to calculate latency
            correctly in multicast cases. Default value is *True*
        """
        if path is None:
            path = self.model.shortest_path[s][t]
        for u, v in path_links(path):
            self.forward_request_hop(u, v, main_path)

    def forward_content_path(self, u, v, path=None, main_path=True):
        """Forward a content from node *s* to node *t* over the provided path.

        Parameters
        ----------
        s : any hashable type
            Origin node
        t : any hashable type
            Destination node
        path : list, optional
            The path to use. If not provided, shortest path is used
        main_path : bool, optional
            If *True*, indicates that this path is being traversed by content
            that will be delivered to the receiver. This is needed to
            calculate latency correctly in multicast cases. Default value is
            *True*
        """
        if path is None:
            path = self.model.shortest_path[u][v]
        for u, v in path_links(path):
            self.forward_content_hop(u, v, main_path)


    def forward_repo_request_path(self, s, t, path=None):
        """Forward a request from node *s* to node *t* over the provided path.

        TODO: This (and all called methods, defined within this class) should be
            redefined, to account for the forwarding and redirection of the requests,
            towards the appropriate collectors, for optimal storage placement decisions,
            depending on service request type and data request source, popularity and distance.

        Parameters
        ----------
        s : any hashable type
            Origin node
        t : any hashable type
            Destination node
        path : list, optional
            The path to use. If not provided, shortest path is used
        flow_id : dict, optional
            The ID of the request and associated flow
        """
        if path is None:
            path = self.model.shortest_path[s][t]
        for u, v in path_links(path):
            self.forward_request_hop(u, v)
        self.add_request_labels_to_node(s, t, self.sess_content.get_request_labels())

    def forward_repo_content_path(self, u, v, path=None, main_path=True):
        """Forward a content from node *s* to node *t* over the provided path.

        TODO: This (and all called methods, defined within this class) should be
            redefined, to account for the feedback and redirection of the content,
            towards the optimal storage locations. BUT (!!!) once the content gets
            redirected, it should also be stored by the appropriate Repo.

        Parameters
        ----------
        u : any hashable type
            Origin node
        v : any hashable type
            Destination node
        path : list, optional
            The path to use. If not provided, shortest path is used
        main_path : bool, optional
            If *True*, indicates that this path is being traversed by content
            that will be delivered to the receiver. This is needed to
            calculate latency correctly in multicast cases. Default value is
            *True*
        """
        if path is None:
            path = self.model.shortest_path[u][v]
        for u, v in path_links(path):
            self.forward_content_hop(u, v, main_path)
        # TODO: Add the relevant content labels of the *EDR-admitted* content
        #       to the network view/model, corresponding to the EDR dictionary/counter
        #       periodically. (if possible, check here and add, otherwise check
        #       EDR dictionaries/counters periodically)



    def forward_request_hop(self, u, v, main_path=True):
        """Forward a request over link  u -> v.

        TODO: Account for feedback collector, as well, on top of the logger.
            This will be included below, but the session property of 'feedback'
            also needs to be added in session! THESE SHOULD BE DONE HERE, BUT
            THEY SHOULD BE PROPERLY DEFINED IN COLLECTORS.PY!!!!!!!!!!!!!!!!!!

        Parameters
        ----------
        u : any hashable type
            Origin node
        v : any hashable type
            Destination node
        main_path : bool, optional
            If *True*, indicates that link link is on the main path that will
            lead to hit a content. It is normally used to calculate latency
            correctly in multicast cases. Default value is *True*
        """
        if self.collector is not None and self.session['log']:
            self.collector.request_hop(u, v, main_path)
        if self.collector is not None and self.session['feedback']:
            self.collector.content_request_labels(v, self.collector.request_labels)

    def forward_content_hop(self, u, v, main_path=True):
        """Forward a content over link  u -> v.

        Parameters
        ----------
        u : any hashable type
            Origin node
        v : any hashable type
            Destination node
        main_path : bool, optional
            If *True*, indicates that this link is being traversed by content
            that will be delivered to the receiver. This is needed to
            calculate latency correctly in multicast cases. Default value is
            *True*
        """
        if self.collector is not None and self.session['log']:
            self.collector.content_hop(u, v, main_path)
        if self.collector is not None and self.session['feedback']:
            self.collector.content_storage_labels(u, self.session.storage_labels)

    def add_request_labels_to_node(self, s, t, request_labels):
        """Forward a request from node *s* to node *t* over the provided path.

        TODO: This (and all called methods, defined within this class) should be
            redefined, to account for the forwarding and redirection of the requests,
            towards the appropriate collectors, for optimal storage placement decisions,
            depending on service request type and data request source, popularity and
            distance.
            !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
            *AND YES! - flow_id's are basically the identifiers for requests - currently*
            !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

        Parameters
        ----------
        s : any hashable type
            Origin node
        t : any hashable type
            Destination node
        path : list, optional
            The path to use. If not provided, shortest path is used

        """

    def add_storage_labels_to_node(self, s, content):
        """Forward a content from node *s* to node *t* over the provided path.

        TODO: This (and all called methods, defined within this class) should be
            redefined, to account for the feedback and redirection of the content,
            towards the optimal storage locations. BUT (!!!) once the content gets
            redirected, it should also be stored by the appropriate Repo.
            The _content methods defined from here on need to be adapted for storage,
            as well!

        Parameters
        ----------
        s : any hashable type
            Origin node
        t : any hashable type
            Destination node
        path : list, optional
            The path to use. If not provided, shortest path is used
        main_path : bool, optional
            If *True*, indicates that this path is being traversed by content
            that will be delivered to the receiver. This is needed to
            calculate latency correctly in multicast cases. Default value is
            *True*
        """

    def put_content(self, node, content=0):
        """Store content in the specified node.

        The node must have a cache stack and the actual insertion of the
        content is executed according to the caching policy. If the caching
        policy has a selective insertion policy, then content may not be
        inserted.

        Parameters
        ----------
        node : any hashable type
            The node where the content is inserted

        Returns
        -------
        evicted : any hashable type
            The evicted object or *None* if no contents were evicted.
        """
        if node in self.model.cache:
            return self.model.cache[node].put(content)

    def get_content(self, node, content=0):
        """Get a content from a server or a cache.

        Parameters
        ----------
        node : any hashable type
            The node where the content is retrieved

        Returns
        -------
        content : bool
            True if the content is available, False otherwise
        """
        if node in self.model.cache:
            cache_hit = self.model.cache[node].get(content)
            if cache_hit:
                #if self.session['log']:
                self.collector.cache_hit(node)
            else:
                #if self.session['log']:
                self.collector.cache_miss(node)
            return cache_hit
        name, props = fnss.get_stack(self.model.topology, node)
        if name == 'source':
            if self.collector is not None and self.session['log']:
                self.collector.server_hit(node)
            return True
        else:
            return False

    def remove_content(self, node):
        """Remove the content being handled from the cache

        Parameters
        ----------
        node : any hashable type
            The node where the cached content is removed

        Returns
        -------
        removed : bool
            *True* if the entry was in the cache, *False* if it was not.
        """
        if node in self.model.cache:
            return self.model.cache[node].remove(self.session['content'])

    def add_event(self, time, receiver, service, node, flow_id, deadline, rtt_delay, status, task=None):
        """Add an arrival event to the eventQ
        """
        if time == float('inf'):
            raise ValueError("Invalid argument in add_event(): time parameter is infinite")
        e = Event(time, receiver, service, node, flow_id, deadline, rtt_delay, status, task)
        heapq.heappush(self.model.eventQ, e)

    def replacement_interval_over(self, flow_id, replacement_interval, timestamp):
        """ Perform replacement of services at each computation spot
        """
        #if self.collector is not None and self.session[flow_id]['log']:
        self.collector.replacement_interval_over(replacement_interval, timestamp)

    def execute_service(self, flow_id, service, node, timestamp, is_cloud):
        """ Perform execution of the service at node with starting time
        """

        self.collector.execute_service(flow_id, service, node, timestamp, is_cloud)

    def complete_task(self, task, timestamp):
        """ Perform execution of the task at node with starting time
        """
        cs = self.model.compSpot[task.node]
        if cs.is_cloud:
            self.execute_service(task.flow_id, task.service, task.node, timestamp, True)
            return
        else:
            cs.complete_task(self, task, timestamp)
            if task.taskType == Task.TASK_TYPE_SERVICE:
                self.execute_service(task.flow_id, task.service, task.node, timestamp, False)

    def reassign_vm(self, time, compSpot, serviceToReplace, serviceToAdd, debugFlag=False):
        """ Instantiate a VM with a given service
        NOTE: this method should ideally call reassign_vm of ComputationSpot as well. 
        However, some strategies rebuild VMs from scratch every time and they do not 
        use that method always. 
        """
        if serviceToAdd == serviceToReplace:
            print ("Error in reassign_vm(): serviceToAdd equals serviceToReplace")
            raise ValueError("Error in reassign_vm(): service replaced and added are same")


        compSpot.reassign_vm(self, time, serviceToReplace, serviceToAdd, debugFlag)
        self.collector.reassign_vm(compSpot.node, serviceToReplace, serviceToAdd)

    def end_session(self, success=True, timestamp=0, flow_id=0):
        """Close a session

        Parameters
        ----------
        success : bool, optional
            *True* if the session was completed successfully, *False* otherwise
        """
        #if self.collector is not None and self.session[flow_id]['log']:
        self.collector.end_session(success, timestamp, flow_id)
        self.session.pop(flow_id, None)

    def rewire_link(self, u, v, up, vp, recompute_paths=True):
        """Rewire an existing link to new endpoints

        This method can be used to model mobility patters, e.g., changing
        attachment points of sources and/or receivers.

        Note well. With great power comes great responsibility. Be careful when
        using this method. In fact as a result of link rewiring, network
        partitions and other corner cases might occur. Ensure that the
        implementation of strategies using this method deal with all potential
        corner cases appropriately.

        Parameters
        ----------
        u, v : any hashable type
            Endpoints of link before rewiring
        up, vp : any hashable type
            Endpoints of link after rewiring
        """
        link = self.model.topology.edge[u][v]
        self.model.topology.remove_edge(u, v)
        self.model.topology.add_edge(up, vp, **link)
        if recompute_paths:
            shortest_path = nx.all_pairs_dijkstra_path(self.model.topology)
            self.model.shortest_path = symmetrify_paths(shortest_path)

    def remove_link(self, u, v, recompute_paths=True):
        """Remove a link from the topology and update the network model.

        Note well. With great power comes great responsibility. Be careful when
        using this method. In fact as a result of link removal, network
        partitions and other corner cases might occur. Ensure that the
        implementation of strategies using this method deal with all potential
        corner cases appropriately.

        Also, note that, for these changes to be effective, the strategy must
        use fresh data provided by the network view and not storing local copies
        of network state because they won't be updated by this method.

        Parameters
        ----------
        u : any hashable type
            Origin node
        v : any hashable type
            Destination node
        recompute_paths: bool, optional
            If True, recompute all shortest paths
        """
        self.model.removed_links[(u, v)] = self.model.topology.edge[u][v]
        self.model.topology.remove_edge(u, v)
        if recompute_paths:
            shortest_path = nx.all_pairs_dijkstra_path(self.model.topology)
            self.model.shortest_path = symmetrify_paths(shortest_path)

    def restore_link(self, u, v, recompute_paths=True):
        """Restore a previously-removed link and update the network model

        Parameters
        ----------
        u : any hashable type
            Origin node
        v : any hashable type
            Destination node
        recompute_paths: bool, optional
            If True, recompute all shortest paths
        """
        self.model.topology.add_edge(u, v, **self.model.removed_links.pop((u, v)))
        if recompute_paths:
            shortest_path = nx.all_pairs_dijkstra_path(self.model.topology)
            self.model.shortest_path = symmetrify_paths(shortest_path)

    def remove_node(self, v, recompute_paths=True):
        """Remove a node from the topology and update the network model.

        Note well. With great power comes great responsibility. Be careful when
        using this method. In fact, as a result of node removal, network
        partitions and other corner cases might occur. Ensure that the
        implementation of strategies using this method deal with all potential
        corner cases appropriately.

        It should be noted that when this method is called, all links connected
        to the node to be removed are removed as well. These links are however
        restored when the node is restored. However, if a link attached to this
        node was previously removed using the remove_link method, restoring the
        node won't restore that link as well. It will need to be restored with a
        call to restore_link.

        This method is normally quite safe when applied to remove cache nodes or
        routers if this does not cause partitions. If used to remove content
        sources or receiver, special attention is required. In particular, if
        a source is removed, the content items stored by that source will no
        longer be available if not cached elsewhere.

        Also, note that, for these changes to be effective, the strategy must
        use fresh data provided by the network view and not storing local copies
        of network state because they won't be updated by this method.

        Parameters
        ----------
        v : any hashable type
            Node to remove
        recompute_paths: bool, optional
            If True, recompute all shortest paths
        """
        self.model.removed_nodes[v] = self.model.topology.node[v]
        # First need to remove all links the removed node as endpoint
        neighbors = self.model.topology.edge[v]
        self.model.disconnected_neighbors[v] = set(neighbors.keys())
        for u in self.model.disconnected_neighbors[v]:
            self.remove_link(v, u, recompute_paths=False)
        self.model.topology.remove_node(v)
        if v in self.model.cache:
            self.model.removed_caches[v] = self.model.cache.pop(v)
        if v in self.model.local_cache:
            self.model.removed_local_caches[v] = self.model.local_cache.pop(v)
        if v in self.model.source_node:
            self.model.removed_sources[v] = self.model.source_node.pop(v)
            for content in self.model.removed_sources[v]:
                self.model.countent_source.pop(content)
        if recompute_paths:
            shortest_path = nx.all_pairs_dijkstra_path(self.model.topology)
            self.model.shortest_path = symmetrify_paths(shortest_path)

    def restore_node(self, v, recompute_paths=True):
        """Restore a previously-removed node and update the network model.

        Parameters
        ----------
        v : any hashable type
            Node to restore
        recompute_paths: bool, optional
            If True, recompute all shortest paths
        """
        self.model.topology.add_node(v, **self.model.removed_nodes.pop(v))
        for u in self.model.disconnected_neighbors[v]:
            if (v, u) in self.model.removed_links:
                self.restore_link(v, u, recompute_paths=False)
        self.model.disconnected_neighbors.pop(v)
        if v in self.model.removed_caches:
            self.model.cache[v] = self.model.removed_caches.pop(v)
        if v in self.model.removed_local_caches:
            self.model.local_cache[v] = self.model.removed_local_caches.pop(v)
        if v in self.model.removed_sources:
            self.model.source_node[v] = self.model.removed_sources.pop(v)
            for content in self.model.source_node[v]:
                self.model.countent_source[content] = v
        if recompute_paths:
            shortest_path = nx.all_pairs_dijkstra_path(self.model.topology)
            self.model.shortest_path = symmetrify_paths(shortest_path)

    def reserve_local_cache(self, ratio=0.1):
        """Reserve a fraction of cache as local.

        This method reserves a fixed fraction of the cache of each caching node
        to act as local uncoodinated cache. Methods `get_content` and
        `put_content` will only operated to the coordinated cache. The reserved
        local cache can be accessed with methods `get_content_local_cache` and
        `put_content_local_cache`.

        This function is currently used only by hybrid hash-routing strategies.

        Parameters
        ----------
        ratio : float
            The ratio of cache space to be reserved as local cache.
        """
        if ratio < 0 or ratio > 1:
            raise ValueError("ratio must be between 0 and 1")
        for v, c in list(self.model.cache.items()):
            maxlen = iround(c.maxlen * (1 - ratio))
            if maxlen > 0:
                self.model.cache[v] = type(c)(maxlen)
            else:
                # If the coordinated cache size is zero, then remove cache
                # from that location
                if v in self.model.cache:
                    self.model.cache.pop(v)
            local_maxlen = iround(c.maxlen * (ratio))
            if local_maxlen > 0:
                self.model.local_cache[v] = type(c)(local_maxlen)

    def get_content_local_cache(self, node):
        """Get content from local cache of node (if any)

        Get content from a local cache of a node. Local cache must be
        initialized with the `reserve_local_cache` method.

        Parameters
        ----------
        node : any hashable type
            The node to query
        """
        if node not in self.model.local_cache:
            return False
        cache_hit = self.model.local_cache[node].get(self.session['content'])
        if cache_hit:
            if self.session['log']:
                self.collector.cache_hit(node)
        else:
            if self.session['log']:
                self.collector.cache_miss(node)
        return cache_hit

    def put_content_local_cache(self, node):
        """Put content into local cache of node (if any)

        Put content into a local cache of a node. Local cache must be
        initialized with the `reserve_local_cache` method.

        Parameters
        ----------
        node : any hashable type
            The node to query
        """
        if node in self.model.local_cache:
            return self.model.local_cache[node].put(self.session['content'])

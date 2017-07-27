# -*- coding: utf-8 -*-
"""Network Model-View-Controller (MVC)

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

import networkx as nx
import fnss

import heapq 

from icarus.registry import CACHE_POLICY
from icarus.util import path_links, iround
from icarus.models.service.compSpot import ComputationalSpot

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

    def __init__(self, time, receiver, service, node, flow_id, deadline, rtt_delay, status):
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

    def __cmp__(self, other):
        return cmp(self.time, other.time)

class Service(object):
    """Implementation of a service object"""

    def __init__(self, service_time=None, deadline=None):
        """Constructor
        Parameters
        ----------
        service_time : computation time to process a request and produce results
        deadline : the total amount of time (taking in to account the computational and network delays) to process the request for this service once the request leaves the user, for an acceptable level of QoS.
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

    def service_locations(self, k):
        """ TODO implement this
        """

    def content_locations(self, k):
        """Return a set of all current locations of a specific content.

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
            elif cs.numberOfInstances[service] > 0:
                return True
 
        return False

    def cache_lookup(self, node, content):
        """Check if the cache of a node has a content object, without changing
        the internal state of the cache.

        This method is meant to be used by data collectors to calculate
        metrics. It should not be used by strategies to look up for contents
        during the simulation. Instead they should use
        `NetworkController.get_content`

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


class NetworkModel(object):
    """Models the internal state of the network.

    This object should never be edited by strategies directly, but only through
    calls to the network controller.
    """

    def __init__(self, topology, cache_policy, sched_policy, n_services, rate, seed=0, shortest_path=None):
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
        comp_size = {}
        service_size = {}
        for node in topology.nodes_iter():
            stack_name, stack_props = fnss.get_stack(topology, node)
            # get the depth of the tree
            if stack_name == 'router' and 'depth' in self.topology[node].keys():
                depth = self.topology.node[node]['depth']
                if depth > self.topology_depth:
                    self.topology_depth = depth
            # get computation size per depth
            if stack_name == 'router':
                if 'cache_size' in stack_props:
                    cache_size[node] = stack_props['cache_size']
                if 'computation_size' in stack_props:
                    comp_size[node] = stack_props['computation_size']
                if 'service_size' in stack_props:
                    service_size[node] = stack_props['service_size']
            elif stack_name == 'source':
                contents = stack_props['contents']
                self.source_node[node] = contents
                for content in contents:
                    self.content_source[content] = node
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
        
        service_time_min = 0.004 # used to be 0.001
        service_time_max = 0.004 # used to be 0.1 
        #delay_min = 0.005
        delay_min = 0.001*2 + 0.020 # Remove*10
        delay_max = 0.202  #NOTE: make sure this is not too large; otherwise all requests go to cloud and are satisfied! 

        aFile = open('services.txt', 'w')
        aFile.write("# ServiceID\tserviceTime\tserviceDeadline\tDifference\n")

        service_indx = 0
        deadlines = []
        service_times = []
        random.seed(seed)

        for service in range(0, n_services):
            service_time = random.uniform(service_time_min, service_time_max)
            #service_time = 2*random.uniform(service_time_min, service_time_max)
            deadline = service_time + random.uniform(delay_min, delay_max) + 2*internal_link_delay
            #deadline = service_time + 1.5*(random.uniform(delay_min, delay_max) + 2*internal_link_delay)
            deadlines.append(deadline)
            service_times.append(service_time)

        #deadlines = sorted(deadlines) #Correlate deadline and popularity XXX
        #deadlines.reverse()
        for service in range(0, n_services):
            service_time = service_times[service_indx]
            deadline = deadlines[service_indx]
            diff = deadline - service_time

            s = str(service_indx) + "\t" + str(service_time) + "\t" + str(deadline) + "\t" + str(diff) + "\n"
            aFile.write(s)
            service_indx += 1
            s = Service(service_time, deadline)
            self.services.append(s)
        aFile.close()
        #""" #END OF Generating Services

        self.compSpot = {node: ComputationalSpot(self, comp_size[node], service_size[node], self.services, node, sched_policy, None) 
                            for node in comp_size}

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

    def start_session(self, timestamp, receiver, content, log, flow_id=0, deadline=0):
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
        log : bool
            *True* if this session needs to be reported to the collector,
            *False* otherwise
        """
        self.session[flow_id] = dict(timestamp=timestamp,
                            receiver=receiver,
                            content=content,
                            log=log,
                            deadline=deadline)

        #if self.collector is not None and self.session[flow_id]['log']:
        self.collector.start_session(timestamp, receiver, content, flow_id, deadline)

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

    def forward_request_hop(self, u, v, main_path=True):
        """Forward a request over link  u -> v.

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

    def add_event(self, time, receiver, service, node, flow_id, deadline, rtt_delay, status):
        """Add an arrival event to the eventQ
        """
        e = Event(time, receiver, service, node, flow_id, deadline, rtt_delay, status)
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

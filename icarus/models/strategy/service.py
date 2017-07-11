# -*- coding: utf-8 -*-
"""Implementations of all service-based strategies"""
from __future__ import division
from __future__ import print_function

import networkx as nx

from icarus.registry import register_strategy
from icarus.util import inheritdoc, path_links
from .base import Strategy

__all__ = [
       'StrictestDeadlineFirst',
       'MostFrequentlyUsed',
       'Hybrid'
           ]

# Status codes
REQUEST = 0
RESPONSE = 1
TASK_COMPLETE = 2
# Admission results:
DEADLINE_MISSED = 0
CONGESTION = 1
SUCCESS = 2
CLOUD = 3
ERROR = 4

# New one
@register_strategy('HYBRID')
class Hybrid(Strategy):
    """A distributed approach for service-centric routing
    """
    
    def __init__(self, view, controller, replacement_interval=10, debug=True, n_replacements=1, **kwargs):
        super(Hybrid, self).__init__(view,controller)
        self.replacement_interval = replacement_interval
        self.n_replacements = n_replacements
        self.last_replacement = 0
        self.receivers = view.topology().receivers()
        self.compSpots = self.view.service_nodes()
        self.num_nodes = len(self.compSpots.keys())
        self.num_services = self.view.num_services()
        self.debug = debug
        # metric to rank each VM of Comp. Spot
        self.deadline_metric = {x : {} for x in range(0, self.num_nodes)}
        self.cand_deadline_metric = {x : {} for x in range(0, self.num_nodes)}
        for node in self.compSpots.keys():
            cs = self.compSpots[node]
            for vm_indx in range(0, self.num_services):
                self.deadline_metric[node][vm_indx] = 0
            for service_indx in range(0, self.num_services):
                self.cand_deadline_metric[node][service_indx] = 0
    
    # New
    def initialise_metrics(self):
        """
        Initialise metrics/counters to 0
        """
        for node in self.compSpots.keys():
            cs = self.compSpots[node]
            cs.running_requests = [0 for x in range(0, self.num_services)]
            cs.missed_requests = [0 for x in range(0, self.num_services)]
            for vm_indx in range(0, cs.n_services):
                self.deadline_metric[node][vm_indx] = 0
            for service_indx in range(0, self.num_services):
                self.cand_deadline_metric[node][service_indx] = 0
    #HYBRID New
    def replace_services(self, k, time):
        """
        This method does the following:
        1. Evaluate instantiated and stored services at each computational spot for the past time interval, ie, [t-interval, t]. 
        2. Decide which services to instantiate in the next time interval [t, t+interval].
        Parameters:
        k : max number of instances to replace at each computational spot
        interval: the length of interval
        """

        for node, cs in self.compSpots.items():
            if cs.is_cloud:
                continue
            n_replacements = k
            vms = []
            cand_services = []
            vm_metrics = self.deadline_metric[node]
            cand_metric = self.cand_deadline_metric[node]
            if self.debug:
                print ("Replacement at node " + repr(node))
            for service in range(0, self.num_services):
                if cs.numberOfInstances[service] == 0:
                    continue
                metric = 0.0
                if cs.running_requests[service] == 0:
                    metric = 1.0 
                    if self.debug:
                        print ("No requests for service: " + repr(service)) 
                else:
                    usage_metric = cs.getIdleTime(time)/self.replacement_interval
                    usage_metric = (cs.running_requests[service] * cs.services[service].service_time)/(cs.numberOfInstances[service]*self.replacement_interval)
                    usage_metric = min(1.0, usage_metric)
                    sat_metric = vm_metrics[service]/cs.running_requests[service]
                    metric = max(usage_metric, sat_metric) # Take the worse one
                    if True: #self.debug:
                        print ("Usage metric for node: " + repr(node) + " is " + repr(usage_metric))
                        print ("Deadline metric for node: " + repr(node) + " is " + repr(sat_metric))
                        print ("Aggregate metric for" + " node: " + repr(node) + " is " + repr(metric))
                        if sat_metric > usage_metric:
                            print ("Sat metric dominates!")

                vms.append([metric, service])
            
            for service in range(0, self.num_services):
                if cs.numberOfInstances[service] > 0:
                    continue
                if self.debug:
                    print ("\tNumber of Requests for upstream service " + repr(service) + " is "  + repr(cs.missed_requests[service]))
                if cs.missed_requests[service] == 0:
                    metric = float('inf')
                else:
                    usage_metric = (cs.missed_requests[service] * cs.services[service].service_time)/(self.replacement_interval)
                    usage_metric = min(1.0, usage_metric)
                    sat_metric = cand_metric[service]/cs.missed_requests[service]
                    metric = max(usage_metric, sat_metric) # Take the worse one
                    if sat_metric > usage_metric:
                        print ("Upstream Sat. metric dominates for service: " + repr(service))

                    if self.debug:
                        print ("Usage metric for Upstream Service: " + repr(service) + " is " + repr(usage_metric))
                        print ("Deadline metric for Upstream Service: " + repr(service) + " is " + repr(sat_metric))
                        print ("Aggregate metric for Upstream Service: " + repr(service) + " is " + repr(metric))
                cand_services.append([metric, service])
            # sort vms and virtual_vms arrays according to metric
            vms = sorted(vms, key=lambda x: x[0], reverse=True) #larger to smaller
            cand_services = sorted(cand_services, key=lambda x: x[0]) #smaller to larger
            if self.debug:
                print ("VMs: " + repr(vms))
                print ("Cand. Services: " + repr(cand_services))
            # Small metric is better
            indx = 0
            for vm in vms:
                # vm's metric is worse than the cand. and they are not the same service
                if vm[0] > cand_services[indx][0] and vm[1] != cand_services[indx][1]:
                    cs.reassign_vm(vm[1], cand_services[indx][1], self.debug)
                    n_replacements -= 1
                if n_replacements == 0 or indx >= self.num_services:
                    break
                indx += 1

    #HYBRID New
    @inheritdoc(Strategy)
    def process_event(self, time, receiver, content, log, node, flow_id, deadline, rtt_delay, status):
        """
        response : True, if this is a response from the cloudlet/cloud
        deadline : deadline for the request 
        flow_id : Id of the flow that the request/response is part of
        node : the current node at which the request/response arrived
        """
        service = content
        if receiver == node and status == REQUEST:
            self.controller.start_session(time, receiver, service, log, flow_id, deadline)
        if time - self.last_replacement > self.replacement_interval:
            #self.print_stats()
            self.controller.replacement_interval_over(flow_id, self.replacement_interval, time)
            self.replace_services(self.n_replacements, time)
            self.last_replacement = time
            self.initialise_metrics()

        if self.debug:
            print ("\nEvent\n time: " + repr(time) + " receiver  " + repr(receiver) + " service " + repr(service) + " node " + repr(node) + " flow_id " + repr(flow_id) + " deadline " + repr(deadline) + " status " + repr(status)) 

        compSpot = None
        if self.view.has_computationalSpot(node):
            compSpot = self.view.compSpot(node)
        else: # the node has no computational spots (0 services)
            if status is not RESPONSE:
                source = self.view.content_source(service)
                if node == source:
                    print ("Error: reached the source node: " + repr(node) + " this should not happen!")
                    return
                path = self.view.shortest_path(node, source)
                next_node = path[1]
                delay = self.view.link_delay(node, next_node)
                if self.debug:
                    print ("Pass upstream (no compSpot) to node: " + repr(next_node) + " " + repr(time+delay))
                self.controller.add_event(time+delay, receiver, service, next_node, flow_id, deadline, rtt_delay+2*delay, REQUEST)
                return
            
        if status == RESPONSE: 
            # response is on its way back to the receiver
            if node == receiver:
                self.controller.end_session(True, time, flow_id) #TODO add flow_time
                return
            else:
                path = self.view.shortest_path(node, receiver)
                next_node = path[1]
                delay = self.view.link_delay(node, next_node)
                self.controller.add_event(time+delay, receiver, service, next_node, flow_id, deadline, rtt_delay, RESPONSE)

        elif status == TASK_COMPLETE:
            task = compSpot.schedule(time)
            #schedule the next queued task at this node
            if task is not None:
                self.controller.add_event(task.finishTime, task.receiver, task.service, node, task.flow_id, task.expiry, task.rtt_delay, TASK_COMPLETE)

            # forward the completed task
            path = self.view.shortest_path(node, receiver)
            next_node = path[1]
            delay = self.view.link_delay(node, next_node)
            self.controller.add_event(time+delay, receiver, service, next_node, flow_id, deadline, rtt_delay, RESPONSE)
            
        elif status == REQUEST:
            # Processing a request
            source = self.view.content_source(service)
            path = self.view.shortest_path(node, source)
            next_node = path[1]
            delay = self.view.path_delay(node, next_node)
            rtt_delay += delay*2
            deadline_metric = (deadline - rtt_delay - compSpot.services[service].service_time)/deadline
            if self.view.has_service(node, service):
                if self.debug:
                    print ("Calling admit_task_EDF")
                ret, reason = compSpot.admit_task_EDF(service, time, flow_id, deadline, receiver, rtt_delay, self.controller)
                if self.debug:
                    print ("Done Calling admit_task_EDF")
                if ret is False:    
                    # Pass the Request upstream
                    self.controller.add_event(time+delay, receiver, service, next_node, flow_id, deadline, rtt_delay, REQUEST)
                    if deadline_metric > 0:
                        self.cand_deadline_metric[node][service] += deadline_metric
                    if self.debug:
                        print ("Pass upstream to node: " + repr(next_node))
                else:
                    self.deadline_metric[compSpot.node][service] += (1.0*(deadline - time - compSpot.services[service].service_time))/deadline
            else: #Not running the service
                if self.debug:
                    print ("Not running the service: Pass upstream to node: " + repr(next_node))
                self.controller.add_event(time+delay, receiver, service, next_node, flow_id, deadline, rtt_delay, REQUEST)
                self.cand_deadline_metric[node][service] += deadline_metric
        else:
            print ("Error: unrecognised status value : " + repr(status))


# Highest Utilisation First Strategy 
@register_strategy('MFU')
class MostFrequentlyUsed(Strategy):
    """A distributed approach for service-centric routing
    """

    def __init__(self, view, controller, replacement_interval=10, debug=False, n_replacements=1, **kwargs):
        super(MostFrequentlyUsed, self).__init__(view, controller)
        self.replacement_interval = replacement_interval
        self.n_replacements = n_replacements
        self.last_replacement = 0
        self.receivers = view.topology().receivers()
        self.compSpots = self.view.service_nodes()
        self.num_nodes = len(self.compSpots.keys())
        self.num_services = self.view.num_services()
        self.debug = debug
        # metric to rank each VM of Comp. Spot
        self.cs_metric = {x : {} for x in range(0, self.num_nodes)}
        self.cs_cand_metric = {x : {} for x in range(0, self.num_nodes)}
        for node in self.compSpots.keys():
            cs = self.compSpots[node]
            for vm_indx in range(0, cs.numOfVMs):
                self.cs_metric[node][vm_indx] = 0
            for service_indx in range(0, self.num_services):
                self.cs_cand_metric[node][service_indx] = 0

    def print_stats(self):
        if self.debug:
            for node, cs in self.compSpots.items():
                cs.print_stats()
            
    def initialise_metrics(self):
        """
        Initialise metrics/counters to 0
        """
        for node in self.compSpots.keys():
            cs = self.compSpots[node]
            cs.vm_requests = [0 for x in range(0, cs.numOfVMs)]
            cs.virtual_requests = [0 for x in range(0, self.num_services)]
            cs.idleTime = [0 for x in range(0, cs.numOfVMs)]
            cs.virtual_idleTime = [0 for x in range(0, self.num_services)]
            for vm_indx in range(0, cs.numOfVMs):
                self.cs_metric[node][vm_indx] = 0
            for service_indx in range(0, self.num_services):
                self.cs_cand_metric[node][service_indx] = 0
    #MFU
    def replace_services(self, k, time):
        """
        This method does the following:
        1. Evaluate instantiated and stored services at each computational spot for the past time interval, ie, [t-interval, t]. 
        2. Decide which services to instantiate in the next time interval [t, t+interval].
        Parameters:
        k : max number of instances to replace at each computational spot
        interval: the length of interval
        """

        for node, cs in self.compSpots.items():
            if cs.is_cloud:
                continue
            n_replacements = k
            cs.update_counters(time)
            vms = []
            cand_services = []
            vm_metrics = self.cs_metric[node]
            cand_metric = self.cs_cand_metric[node]
            if self.debug:
                print ("Number of VMs at node " + repr(node) + " is " + repr(cs.numOfVMs))
            for indx in range(0, cs.numOfVMs):
                # TODO: incorportate idle time to the metric
                metric = 0.0
                if self.debug:
                    print ("\tNumber of Requests for VM (service: " + repr(cs.vmAssignment[indx]) + ") " + repr(indx) + " is "  + repr(cs.vm_requests[indx]))
                if cs.vm_requests[indx] == 0:
                    metric = float('inf')
                else:
                    metric = cs.getIdleTime(indx, time) # vm_indx vm_metrics[indx]/(cs.vm_requests[indx])
                if self.debug:
                    print ("\tMetric for VM " + repr(indx) + " is " + repr(metric))
                vms.append([metric, cs.vmAssignment[indx], indx])
            
            for indx in range(0, self.num_services):
                if self.debug:
                    print ("\tNumber of Requests for stored service " + repr(indx) + " is "  + repr(cs.virtual_requests[indx]))
                if cs.virtual_requests[indx] == 0:
                    metric = float('inf')
                else:
                    metric = cs.getVirtualIdleTime(indx, time)
                if self.debug:
                    print ("\tMetric for service " + repr(indx) + " is " + repr(metric))
                cand_services.append([metric, indx])
            # sort vms and virtual_vms arrays according to metric
            vms = sorted(vms, key=lambda x: x[0], reverse=True) #larger to smaller
            cand_services = sorted(cand_services, key=lambda x: x[0]) # smaller to larger
            if self.debug:
                print ("VMs: " + repr(vms))
                print ("Cand. Services: " + repr(cand_services))
            # Small deadline is better
            indx = 0
            for vm in vms:
                # vm's metric is worse than the cand. and they are not the same service
                if vm[0] > cand_services[indx][0] and vm[1] != cand_services[indx][1]:
                    cs.reassign_vm(vm[2], cand_services[indx][1], self.debug)
                    n_replacements -= 1
                if n_replacements == 0 or indx >= self.num_services:
                    break
                indx += 1

    #MFU
    @inheritdoc(Strategy)
    def process_event(self, time, receiver, content, log, node, flow_id, deadline, response):
        """
        response : True, if this is a response from the cloudlet/cloud
        deadline : deadline for the request 
        flow_id : Id of the flow that the request/response is part of
        node : the current node at which the request/response arrived
        """
        service = content
        if receiver == node and response is False:
            self.controller.start_session(time, receiver, service, log, flow_id, deadline)
        if time - self.last_replacement > self.replacement_interval:
            #self.print_stats()
            self.controller.replacement_interval_over(flow_id, self.replacement_interval, time)
            self.replace_services(self.n_replacements, time)
            self.last_replacement = time
            self.initialise_metrics()

        if self.debug:
            print ("\nEvent\n time: " + repr(time) + " receiver  " + repr(receiver) + " service " + repr(service) + " node " + repr(node) + " flow_id " + repr(flow_id) + " deadline " + repr(deadline) + " response " + repr(response)) 

        compSpot = None
        if self.view.has_computationalSpot(node):
            compSpot = self.view.compSpot(node)
        else: # the node has no computational spots (0 services)
            if response is False:
                source = self.view.content_source(service)
                if node == source:
                    print ("Error: reached the source node: " + repr(node) + " this should not happen!")
                    return
                path = self.view.shortest_path(node, source)
                next_node = path[1]
                delay = self.view.link_delay(node, next_node)
                if self.debug:
                    print ("Pass upstream (no compSpot) to node: " + repr(next_node) + " " + repr(time+delay))
                self.controller.add_event(time+delay, receiver, service, next_node, flow_id, deadline, False)
                return
            
        if response is True:
            # response is on its way back to the receiver
            if node == receiver:
                self.controller.end_session(True, time, flow_id) #TODO add flow_time
                return
            else:
                path = self.view.shortest_path(node, receiver)
                next_node = path[1]
                delay = self.view.link_delay(node, next_node)
                self.controller.add_event(time+delay, receiver, service, next_node, flow_id, deadline, True)
        else:
            # Processing a request
            source = self.view.content_source(service)
            path = self.view.shortest_path(node, source)
            return_delay = self.view.path_delay(node, receiver)
            if self.view.has_service(node, service):
                compTime, vm_indx = compSpot.getFinishTime(service, time)
                if (compTime + return_delay > deadline) and (vm_indx is not None):
                    # Pass the request upstream due to congestion
                    success, vCompTime = compSpot.runVirtualService(service, time, deadline, return_delay)
                    if success:
                        self.cs_cand_metric[compSpot.node][service] += deadline - vCompTime
                    path = self.view.shortest_path(node, source)
                    next_node = path[1]
                    delay = self.view.link_delay(node, next_node)
                    if self.debug:
                        print ("Pass upstream to node: " + repr(next_node))
                    self.controller.add_event(time+delay, receiver, service, next_node, flow_id, deadline, False)
                else:
                    # Success in running the service
                    if deadline > time and vm_indx is not None:
                        #self.cs_metric[compSpot.node][vm_indx] += deadline - compTime
                        self.cs_metric[compSpot.node][vm_indx] += (1.0*(deadline - compTime - return_delay))/deadline
                    path = self.view.shortest_path(node, receiver)
                    next_node = path[1]
                    delay = self.view.link_delay(node, next_node)
                    compSpot.schedule_service(service, vm_indx, time)
                    if self.debug:
                        print ("Return Response (success) to node: " + repr(next_node))
                    self.controller.add_event(compTime+delay, receiver, service, next_node, flow_id, deadline, True)
            else:
                # Pass the request upstream (lack of instantiated service)
                success, vCompTime = compSpot.runVirtualService(service, time, deadline, return_delay)
                #success, vCompTime = compSpot.runVirtualService(service, time, deadline)
                if success:
                    self.cs_cand_metric[compSpot.node][service] += deadline - vCompTime
                source = self.view.content_source(service)
                path = self.view.shortest_path(node, source)
                next_node = path[1]
                delay = self.view.link_delay(node, next_node)
                if self.debug:
                    print ("Pass upstream (not running the service) to node " + repr(next_node) + " " + repr(time+delay))
                self.controller.add_event(time+delay, receiver, service, next_node, flow_id, deadline, False)


# Strictest Deadline First Strategy
@register_strategy('SDF')
class StrictestDeadlineFirst(Strategy):
    """ A distributed approach for service-centric routing
    """
   
    def __init__(self, view, controller, replacement_interval=10, debug=False, n_replacements=1, **kwargs):
        super(StrictestDeadlineFirst, self).__init__(view, controller)
        self.replacement_interval = replacement_interval
        self.n_replacements = n_replacements
        self.last_replacement = 0
        self.receivers = view.topology().receivers()
        self.compSpots = self.view.service_nodes()
        self.num_nodes = len(self.compSpots.keys())
        self.num_services = self.view.num_services()
        self.debug = debug
        # metric to rank each VM of Comp. Spot
        self.cs_metric = {x : {} for x in range(0, self.num_nodes)}
        self.cs_cand_metric = {x : {} for x in range(0, self.num_nodes)}
        for node in self.compSpots.keys():
            cs = self.compSpots[node]
            for vm_indx in range(0, cs.numOfVMs):
                self.cs_metric[node][vm_indx] = 0
            for service_indx in range(0, self.num_services):
                self.cs_cand_metric[node][service_indx] = 0

    def print_stats(self):
        if self.debug:
            for node, cs in self.compSpots.items():
                cs.print_stats()
            
    def initialise_metrics(self):
        """
        Initialise metrics/counters to 0
        """
        for node in self.compSpots.keys():
            cs = self.compSpots[node]
            cs.vm_requests = [0 for x in range(0, cs.numOfVMs)]
            cs.virtual_requests = [0 for x in range(0, self.num_services)]
            cs.idleTime = [0 for x in range(0, cs.numOfVMs)]
            cs.virtual_idleTime = [0 for x in range(0, self.num_services)]
            for vm_indx in range(0, cs.numOfVMs):
                self.cs_metric[node][vm_indx] = 0
            for service_indx in range(0, self.num_services):
                self.cs_cand_metric[node][service_indx] = 0
    #SDF
    def replace_services(self, k, time):
        """
        This method does the following:
        1. Evaluate instantiated and stored services at each computational spot for the past time interval, ie, [t-interval, t]. 
        2. Decide which services to instantiate in the next time interval [t, t+interval].
        Parameters:
        k : max number of instances to replace
        interval: the length of interval
        """

        for node, cs in self.compSpots.items():
            if cs.is_cloud:
                continue
            n_replacements = k
            cs.update_counters(time)
            vms = []
            cand_services = []
            vm_metrics = self.cs_metric[node]
            cand_metric = self.cs_cand_metric[node]
            if self.debug:
                print ("Number of VMs at node " + repr(node) + " is " + repr(cs.numOfVMs))
            for indx in range(0, cs.numOfVMs):
                if self.debug:
                    print ("\tNumber of Requests for VM (service: " + repr(cs.vmAssignment[indx]) + ") " + repr(indx) + " is "  + repr(cs.vm_requests[indx]))
                if cs.vm_requests[indx] == 0:
                    metric = float('inf')
                else:
                    metric = vm_metrics[indx]/(cs.vm_requests[indx])
                if self.debug:
                    print ("\tMetric for VM " + repr(indx) + " is " + repr(metric))
                vms.append([metric, cs.vmAssignment[indx], indx])
            for indx in range(0, self.num_services):
                if self.debug:
                    print ("\tNumber of Requests for stored service " + repr(indx) + " is "  + repr(cs.virtual_requests[indx]))
                if cs.virtual_requests[indx] == 0:
                    metric = float('inf')
                else:
                    metric = cand_metric[indx]/cs.virtual_requests[indx]
                if self.debug:
                    print ("\tMetric for service " + repr(indx) + " is " + repr(metric))
                cand_services.append([metric, indx])
            # sort vms and virtual_vms arrays according to metric
            vms = sorted(vms, key=lambda x: x[0], reverse=True) #larger to smaller
            cand_services = sorted(cand_services, key=lambda x: x[0]) # smaller to larger
            if self.debug:
                print ("VMs: " + repr(vms))
                print ("Cand. Services: " + repr(cand_services))
            # Small deadline is better
            indx = 0
            for vm in vms:
                # vm's metric is worse than the cand. and they are not the same service
                if vm[0] > cand_services[indx][0] and vm[1] != cand_services[indx][1]:
                    cs.reassign_vm(vm[2], cand_services[indx][1], self.debug)
                    n_replacements -= 1
                if n_replacements == 0 or indx >= self.num_services:
                    break
                indx += 1

    @inheritdoc(Strategy)
    def process_event(self, time, receiver, content, log, node, flow_id, deadline, response):
        """
        response : True, if this is a response from the cloudlet/cloud
        deadline : deadline for the request 
        flow_id : Id of the flow that the request/response is part of
        node : the current node at which the request/response arrived
        """

        service = content
        if receiver == node and response is False:
            self.controller.start_session(time, receiver, service, log, flow_id, deadline)
        if time - self.last_replacement > self.replacement_interval:
            #self.print_stats()
            self.controller.replacement_interval_over(flow_id, self.replacement_interval, time)
            self.replace_services(self.n_replacements, time)
            self.last_replacement = time
            self.initialise_metrics()

        if self.debug:
            print ("\nEvent\n time: " + repr(time) + " receiver  " + repr(receiver) + " service " + repr(service) + " node " + repr(node) + " flow_id " + repr(flow_id) + " deadline " + repr(deadline) + " response " + repr(response)) 

        compSpot = None
        if self.view.has_computationalSpot(node):
            compSpot = self.view.compSpot(node)
        else: # the node has no computational spots (0 services)
            if response is False:
                source = self.view.content_source(service)
                if node == source:
                    print ("Error: reached the source node: " + repr(node) + " this should not happen!")
                    return
                path = self.view.shortest_path(node, source)
                next_node = path[1]
                delay = self.view.link_delay(node, next_node)
                if self.debug:
                    print ("Pass upstream (no compSpot) to node: " + repr(next_node) + " " + repr(time+delay))
                self.controller.add_event(time+delay, receiver, service, next_node, flow_id, deadline, False)
                return
            
        if response is True:
            # response is on its way back to the receiver
            if node == receiver:
                self.controller.end_session(True, time, flow_id) #TODO add flow_time
                return
            else:
                path = self.view.shortest_path(node, receiver)
                next_node = path[1]
                delay = self.view.link_delay(node, next_node)
                self.controller.add_event(time+delay, receiver, service, next_node, flow_id, deadline, True)
        else:
            # Processing a request
            source = self.view.content_source(service)
            path = self.view.shortest_path(node, source)
            return_delay = self.view.path_delay(node, receiver)
            if self.view.has_service(node, service):
                compTime, vm_indx = compSpot.getFinishTime(service, time)
                if (compTime + return_delay > deadline) and (vm_indx is not None):
                    # Pass the request upstream due to congestion
                    success, vCompTime = compSpot.runVirtualService(service, time, deadline, return_delay)
                    if success:
                        self.cs_cand_metric[compSpot.node][service] += deadline - vCompTime
                    path = self.view.shortest_path(node, source)
                    next_node = path[1]
                    delay = self.view.link_delay(node, next_node)
                    if self.debug:
                        print ("Pass upstream to node: " + repr(next_node))
                    self.controller.add_event(time+delay, receiver, service, next_node, flow_id, deadline, False)
                else:
                    # Success in running the service
                    if deadline > time and vm_indx is not None:
                        self.cs_metric[compSpot.node][vm_indx] += (1.0*(deadline - compTime - return_delay))/deadline
                    path = self.view.shortest_path(node, receiver)
                    next_node = path[1]
                    delay = self.view.link_delay(node, next_node)
                    compSpot.schedule_service(service, vm_indx, time)
                    if self.debug:
                        print ("Return Response (success) to node: " + repr(next_node))
                    self.controller.add_event(compTime+delay, receiver, service, next_node, flow_id, deadline, True)
            else:
                # Pass the request upstream (lack of instantiated service)
                success, vCompTime = compSpot.runVirtualService(service, time, deadline, return_delay)
                if success:
                    self.cs_cand_metric[compSpot.node][service] += deadline - vCompTime
                source = self.view.content_source(service)
                path = self.view.shortest_path(node, source)
                next_node = path[1]
                delay = self.view.link_delay(node, next_node)
                if self.debug:
                    print ("Pass upstream (not running the service) to node " + repr(next_node) + " " + repr(time+delay))
                self.controller.add_event(time+delay, receiver, service, next_node, flow_id, deadline, False)


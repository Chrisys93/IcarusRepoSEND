# -*- coding: utf-8 -*-
"""Content placement strategies.

This module contains function to decide the allocation of content objects to
source nodes.
"""
import random
import collections

from fnss.util import random_from_pdf
from icarus.registry import register_content_placement


__all__ = ['uniform_content_placement', 'weighted_content_placement', 'weighted_repo_content_placement']


def apply_content_placement(placement, topology):
    """Apply a placement to a topology

    Parameters
    ----------
    placement : dict of sets
        Set of contents to be assigned to nodes keyed by node identifier
    topology : Topology
        The topology
    """
    for v, contents in placement.items():
        topology.node[v]['stack'][1]['contents'] = contents

def apply_labels_association(association, data, content, topics, types):
    """
    Apply association of labels to contents

    Parameters
    ----------
    association:
    topics:
    types:
    :return:
    """
    for label, content in association.items():
        if label not in data[content]['labels']:
            data[content]['labels'].update(association)

def get_sources(topology):
    return [v for v in topology if topology.node[v]['stack'][0] == 'source']

@register_content_placement('UNIFORM')
def uniform_content_placement(topology, contents, seed=None):
    """Places content objects to source nodes randomly following a uniform
    distribution.

    Parameters
    ----------
    topology : Topology
        The topology object
   contents : iterable
        Iterable of content objects
    source_nodes : list
        List of nodes of the topology which are content sources

    Returns
    -------
    cache_placement : dict
        Dictionary mapping content objects to source nodes

    Notes
    -----
    A deterministic placement of objects (e.g., for reproducing results) can be
    achieved by using a fix seed value
    """
    random.seed(seed)
    source_nodes = get_sources(topology)
    content_placement = collections.defaultdict(set)
    for c in contents:
        content_placement[random.choice(source_nodes)].add(c)
    apply_content_placement(content_placement, topology)


@register_content_placement('WEIGHTED')
def weighted_content_placement(topology, contents, source_weights, seed=None):
    """Places content objects to source nodes randomly according to the weight
    of the source node.

    Parameters
    ----------
    topology : Topology
        The topology object
   contents : iterable
        Iterable of content objects

   source_weights : dict
        Dict mapping nodes nodes of the topology which are content sources and
        the weight according to which content placement decision is made.

    Returns
    -------
    cache_placement : dict
        Dictionary mapping content objects to source nodes

    Notes
    -----
    A deterministic placement of objects (e.g., for reproducing results) can be
    achieved by using a fix seed value
    """
    random.seed(seed)
    norm_factor = float(sum(source_weights.values()))
    source_pdf = dict((k, v / norm_factor) for k, v in source_weights.items())
    content_placement = collections.defaultdict(set)
    for c in contents:
        content_placement[random_from_pdf(source_pdf)].add(c)
    apply_content_placement(content_placement, topology)


@register_content_placement('WEIGHTED_REPO')
def weighted_repo_content_placement(topology, contents, topics, types, freshness_per,
                                    shelf_life, msg_size, source_weights, types_weights,
                                    topics_weights, max_label_nos, seed=None):
    """Places content objects to source nodes randomly according to the weight
    of the source node.

    TODO: This should be modified, or another one created, to include content
        placement parameters, like the freshness periods, shelf-lives, topics/types
        of labels and placement possibilities, maybe depending on hashes, placement
        of nodes and possibly other scenario-specific/service-specific parameters.

    Parameters
    ----------
    topology : Topology
        The topology object
    contents : iterable
        Iterable of content objects
    topics :

    types :

    freshness_per :

    shelf_life :

    msg_size :

    source_weights : dict
        Dict mapping nodes nodes of the topology which are content sources and
        the weight according to which content placement decision is made.

    Returns
    -------
    cache_placement : dict
       Dictionary mapping content objects to source nodes

    Notes
    -----
    A deterministic placement of objects (e.g., for reproducing results) can be
    achieved by using a fix seed value
    """

    # TODO: This is the format that each datum (message) shuold have
    #       placed_data = {content, msg_topics, msg_type, freshness_per,
    #                       shelf_life, msg_size}

    placed_data = {}
    random.seed(seed)
    for c in contents:
        placed_data[c] = {'content':     c}
    norm_factor = float(sum(source_weights.values()))
    # TODO: These ^\/^\/^ might need redefining, to make label-specific
    #  source weights, and then the labels distributed according to these.
    #  OR the other way around, distributing sources according to label weights
    types_labels_norm_factor = float(sum(types_weights.values()))
    topics_labels_norm_factor = float(sum(topics_weights.values()))
    # TODO: Think about a way to randomise, but still maintain a certain
    #  distribution among the users that receive data with certain labels.
    #  Maybe associate the pdf with labels, rather than contents, SOMEHOW!
    source_pdf = dict((k, v / norm_factor) for k, v in source_weights.items())
    types_labels_pdf = dict((k, v / types_labels_norm_factor) for k, v in types_weights.items())
    topics_labels_pdf = dict((k, v / topics_labels_norm_factor) for k, v in topics_weights.items())
    labels_association = collections.defaultdict(set)
    content_placement = collections.defaultdict(set)
    # Further TODO: Add all the other data characteristics and maybe place
    #           content depending on those at a later point (create other
    #           placement strategies)
    # NOTE: All label names will come as a list of strings
    for c in contents:
        for i in range(1, max_label_nos):
            if types is not None :
                labels_association[random_from_pdf(types_labels_pdf)].add(c)
            labels_association[random_from_pdf(topics_labels_pdf)].add(c)
        apply_labels_association(labels_association, placed_data, c, topics, types)
    for d in placed_data:
        content_placement[random_from_pdf(source_pdf)].add(d)
    apply_content_placement(content_placement, topology)

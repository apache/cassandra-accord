package accord.primitives;

/**
 * Routable: a RoutingKey or Range (of RoutingKey). Something that can address a replica in the cluster.
 * Seekable: a Key or Range (of either RoutingKey or Key). Something that can address some physical data on a node.
 * Routables: a collection of Routable
 * Seekables: a collection of Seekable
 * Route: a collection of Routable including a homeKey. Represents a consistent slice (or slices) of token ranges.
 *        Either a PartialRoute or a FullRoute.
 */
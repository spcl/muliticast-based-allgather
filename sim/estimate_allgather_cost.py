import math


class NetworkOp:

    def __init__(self, src_id, dst_id=math.nan):
        self.src_id = src_id
        self.dst_id = dst_id

    def is_mcast(self):
        return math.isnan(self.dst_id)

    def print(self):
        print(self.src_id, self.dst_id)


class TopoFatTree:

    def __init__(self, nodes_count, radix):
        self.nodes_count = nodes_count
        self.radix       = radix
        self.hradix      = radix / 2

    def calculate_p2p_cost(self, op):
        distance = op.dst_id - op.src_id
        op.src_id % self.hradix 
        if distance < (self.hradix - op.src_id % self.hradix):             # ToR
            return 2
        if distance < (2 * self.hradix - (op.src_id % (2 * self.hradix))): # POD
            return 4
        else:                                                              # Core
            return 6

    def calculate_mcast_cost(self, nodes_count):
        # each fat tree layer is described using tuple [n_upstream_sends, n_downstream_sends]
        core_cost = [0, 0]
        pod_cost  = [0, 0]
        tor_cost  = [0, 0]
        if nodes_count <= self.hradix:       # traffic located within 1 ToR
            tor_cost = [0, nodes_count - 1]
        elif nodes_count <= 2 * self.hradix: # traffic located within 1 POD
            pod_cost = [0, 1]
            tor_cost = [1, nodes_count - 1]
        else:                                # traffic crosses Core
            tors_count = int(nodes_count / self.hradix)
            if tors_count % 2:
                pods_count = int(tors_count / 2) + 1
            else:
                pods_count = int(tors_count / 2)
            assert pods_count <= self.radix
            core_cost = [0, pods_count]
            pod_cost  = [1, tors_count - 1]
            tor_cost  = [1, nodes_count - 1]
        return 1 + sum(core_cost) + sum(pod_cost) + sum(tor_cost)

    def get_op_cost(self, op, nodes_count):
        assert op.src_id < nodes_count
        assert op.src_id != op.dst_id
        if op.is_mcast():
            return self.calculate_mcast_cost(nodes_count)
        assert op.dst_id < nodes_count
        return self.calculate_p2p_cost(op)

    def get_schedule_cost(self, coll_algorithm, nodes_count):
        assert nodes_count <= self.nodes_count
        cost = 0
        node_time = [0] * nodes_count
        for op in coll_algorithm.schedule:
            #print(self.get_op_cost(op, nodes_count))
            cost = cost + self.get_op_cost(op, nodes_count)
            node_time[op.src_id] += 1
        return cost #, max(node_time)


class ScheduleAllgatherLinear:

    def __init__(self, nodes_count):
        self.schedule = []
        for src_node_id in range(nodes_count):
            for dst_node_id in range(nodes_count):
                if src_node_id == dst_node_id:
                    continue
                self.schedule.append(NetworkOp(src_id=src_node_id, 
                                               dst_id=dst_node_id))


class ScheduleAllgatherRing:

    def __init__(self, nodes_count):
        self.schedule = []
        for src_node_id in range(nodes_count):
                for _ in range(nodes_count - 1):
                    self.schedule.append(NetworkOp(src_id=src_node_id,
                                                   dst_id=(src_node_id+1)%nodes_count))


#  inspired by:
#  https://github.com/open-mpi/ompi/blob/d231daf236110057d2bd3fa35b27e43b3ecccbc9/ompi/mca/coll/base/coll_base_allgather.c#L312C5-L314C1
class ScheduleAllgatherRecursiveDoubling:

    def __init__(self, nodes_count):
        self.schedule = []
        for src_node_id in range(nodes_count):
            distance = 0x1
            while (distance < nodes_count):
                dst_node_id = src_node_id ^ distance
                if src_node_id == dst_node_id:
                    continue
                for _ in range(distance):
                    self.schedule.append(NetworkOp(src_id=src_node_id,
                                                   dst_id=dst_node_id))
                distance = distance << 1


class ScheduleAllgatherMcast:

    def __init__(self, nodes_count):
        self.schedule = []
        for src_node_id in range(nodes_count):
            self.schedule.append(NetworkOp(src_id=src_node_id))


ft_1k = TopoFatTree(1024, 32)

print("nodes,linear,ring,recdoubling,multicast")
for nodes_count in (2**p for p in range(1, 11)):
    ag_linear_cost = ft_1k.get_schedule_cost(ScheduleAllgatherLinear            (nodes_count), nodes_count)
    ag_ring_cost   = ft_1k.get_schedule_cost(ScheduleAllgatherRing              (nodes_count), nodes_count)
    ag_rd_cost     = ft_1k.get_schedule_cost(ScheduleAllgatherRecursiveDoubling (nodes_count), nodes_count)
    ag_mcast_cost  = ft_1k.get_schedule_cost(ScheduleAllgatherMcast             (nodes_count), nodes_count)
    print(f"{nodes_count},{ag_linear_cost},{ag_ring_cost},{ag_rd_cost},{ag_mcast_cost}")
    #print("##########################")
    #print("Nodes count",       nodes_count)
    #print("Linear",            ag_linear_cost)
    #print("Ring",              ag_ring_cost)
    #print("RecursiveDoubling", ag_rd_cost)
    #print("Mulitcast",         ag_mcast_cost)
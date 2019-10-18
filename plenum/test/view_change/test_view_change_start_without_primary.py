import pytest

from plenum.test.helper import stopNodes, view_change_timeout
from plenum.test.test_node import checkProtocolInstanceSetup, getRequiredInstances, \
    checkNodesConnected
from plenum.test import waits

VIEW_CHANGE_TIMEOUT = 10


@pytest.fixture(scope="module")
def tconf(tconf):
    with view_change_timeout(tconf, nv_timeout=VIEW_CHANGE_TIMEOUT):
        yield tconf


def test_view_change_without_primary(txnPoolNodeSet, looper):
    first, others = stop_nodes_and_remove_first(looper, txnPoolNodeSet)

    start_and_connect_nodes(looper, others)

    timeout = waits.expectedPoolElectionTimeout(len(txnPoolNodeSet)) + VIEW_CHANGE_TIMEOUT

    checkProtocolInstanceSetup(looper=looper, nodes=txnPoolNodeSet, retryWait=1,
                               customTimeout=timeout,
                               instances=range(getRequiredInstances(len(txnPoolNodeSet))))


def stop_nodes_and_remove_first(looper, nodes):
    first_node = nodes[0]
    stopNodes(nodes, looper)
    looper.removeProdable(first_node)
    looper.runFor(3)  # let the nodes stop
    return first_node, \
           list(filter(lambda x: x.name != first_node.name, nodes))


def start_and_connect_nodes(looper, nodes):
    for n in nodes:
        n.start(looper.loop)
    looper.run(checkNodesConnected(nodes))

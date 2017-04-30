-- explain
select dg_utils.transducer($PHIPS$PhiExec python2
import vitessedata.phi
import tensorflow.python.platform
import time
import numpy as np
import tensorflow as tf

vitessedata.phi.DeclareTypes('''
//
// BEGIN INPUT TYPES
// dummy int32
// END INPUT TYPES
//
// BEGIN OUTPUT TYPES
// accuracy float32
// END OUTPUT TYPES
//
''')

tf.app.flags.DEFINE_string("ps_hosts", "localhost:16180", "Comma-separated list of hostname:port pairs")
tf.app.flags.DEFINE_string("worker_hosts", "localhost:16181,localhost:16182", "Comma-separated list of hostname:port pairs")
tf.app.flags.DEFINE_integer('workers', 2, 'Number of max workers')
FLAGS = tf.app.flags.FLAGS

def create_done_queue(i):
    """ Queue used to signal death for i'th ps shard. Intended to have 
    all workers enqueue an item onto it to signal doneness."""
    with tf.device("/job:ps/task:%d" % (i)):
        return tf.FIFOQueue(FLAGS.workers, tf.int32, shared_name="done_queue"+
                        str(i))
  
def create_done_queues():
    """ Assume one 1 ps host. """
    return [create_done_queue(i) for i in range(1)] 

def main(_): 
    # cluster and server stuff 
    ps_hosts = FLAGS.ps_hosts.split(",")
    worker_hosts = FLAGS.worker_hosts.split(",")
    cluster = tf.train.ClusterSpec({"ps":ps_hosts, "worker":worker_hosts})
    server = tf.train.Server(cluster, job_name="ps", task_index=0) 

    sess = tf.Session(server.target)
    queue = create_done_queue(0) 
         
    # wait until all workers are done
    for i in range(FLAGS.workers):
        sess.run(queue.dequeue())

    vitessedata.phi.WriteOutput(None)

if __name__ == '__main__':
    tf.app.run()
$PHIPS$,
tps.*),
dg_utils.transducer_column_float4(1) as accuracy
from (select 1::int) tps;

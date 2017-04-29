import tensorflow.python.platform

import numpy as np
import tensorflow as tf

# Define the flags useable from the command line.
tf.app.flags.DEFINE_string("ps_hosts", "localhost:16180",
                           "Comma-separated list of hostname:port pairs")
tf.app.flags.DEFINE_string("worker_hosts", "localhost:16181,localhost:16182",
                           "Comma-separated list of hostname:port pairs")
tf.app.flags.DEFINE_string('train', 'simdata/linear_data_train.csv',
                           'File containing the training data (labels & features).')
tf.app.flags.DEFINE_string('test', 'simdata/linear_data_eval.csv', 
                           'File containing the test data (labels & features).')
tf.app.flags.DEFINE_string('job_name', None, 'Job, ps or worker')
tf.app.flags.DEFINE_integer('task_index', 0, 'Task idx')
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


# Global variables.
NUM_LABELS = 2    # The number of labels.
BATCH_SIZE = 100  # The number of training examples to use per training step.

# Extract numpy representations of the labels and features given rows consisting of:
#   label, feat_0, feat_1, ..., feat_n
def extract_data(filename):

    # Arrays to hold the labels and feature vectors.
    labels = []
    fvecs = []

    # Iterate over the rows, splitting the label from the features. Convert labels
    # to integers and features to floats.
    for line in file(filename):
        row = line.split(",")
        labels.append(int(row[0]))
        fvecs.append([float(x) for x in row[1:]])

    # Convert the array of float arrays into a numpy float matrix.
    fvecs_np = np.matrix(fvecs).astype(np.float32)

    # Convert the array of int labels into a numpy array.
    labels_np = np.array(labels).astype(dtype=np.uint8)

    # Convert the int numpy array into a one-hot matrix.
    labels_onehot = (np.arange(NUM_LABELS) == labels_np[:, None]).astype(np.float32)

    # Return a pair of the feature matrix and the one-hot label matrix.
    return fvecs_np,labels_onehot

def main(_): 
    # cluster and server stuff 
    ps_hosts = FLAGS.ps_hosts.split(",")
    worker_hosts = FLAGS.worker_hosts.split(",")
    cluster = tf.train.ClusterSpec({"ps":ps_hosts, "worker":worker_hosts})
    server = tf.train.Server(cluster, job_name=FLAGS.job_name, task_index=FLAGS.task_index)

    if FLAGS.job_name == "ps":
        sess = tf.Session(server.target)
        queue = create_done_queue(FLAGS.task_index)
         
        # wait until all workers are done
        for i in range(FLAGS.workers):
            sess.run(queue.dequeue())
            print("ps %d received done %d" % (FLAGS.task_index, i))
        print("ps %d: quitting"%(FLAGS.task_index))
    elif FLAGS.job_name == "worker":
        # chief worker reset graph...
        # if FLAGS.task_index == 0:
        #      tf.reset_default_graph()

        # Get the data.
        train_data_filename = FLAGS.train
        test_data_filename = FLAGS.test
        # Extract it into numpy matrices.
        train_data,train_labels = extract_data(train_data_filename)
        test_data, test_labels = extract_data(test_data_filename)

        # Get the shape of the training data.
        train_size,num_features = train_data.shape

        # For the test data, hold the entire dataset in one constant node.
        test_data_node = tf.constant(test_data)

        # Assigns ops to the local worker by default.
        with tf.device(tf.train.replica_device_setter(
            worker_device="/job:worker/task:%d" % FLAGS.task_index,
            cluster=cluster)):
            global_step = tf.Variable(0, trainable=False) 

            # This is where training samples and labels are fed to the graph.
            # These placeholder nodes will be fed a batch of training data at each
            # training step using the {feed_dict} argument to the Run() call below.
            x = tf.placeholder("float", shape=[None, num_features])
            y_ = tf.placeholder("float", shape=[None, NUM_LABELS])

            # These are the weights that inform how much each feature contributes to
            # the classification.
            W = tf.Variable(tf.zeros([num_features,NUM_LABELS]))
            b = tf.Variable(tf.zeros([NUM_LABELS]))
            y = tf.nn.softmax(tf.matmul(x,W) + b)

            # Optimization.
            cross_entropy = -tf.reduce_sum(y_*tf.log(y))
            train_step = tf.train.GradientDescentOptimizer(0.01).minimize(cross_entropy, global_step=global_step)

            # Evaluation.
            predicted_class = tf.argmax(y,1);
            correct_prediction = tf.equal(tf.argmax(y,1), tf.argmax(y_,1))
            accuracy = tf.reduce_mean(tf.cast(correct_prediction, "float"))

            init_op = tf.global_variables_initializer() 

            enq_ops = []
            for q in create_done_queues():
                qop = q.enqueue(1)
                enq_ops.append(qop)

        # Create a "supervisor", which oversees the training process.
        sv = tf.train.Supervisor(is_chief=(FLAGS.task_index == 0),
                             logdir="./logs_%d" % FLAGS.task_index,
                             init_op=init_op,
                             # summary_op=summary_op,
                             # saver=saver,
                             # save_model_secs=60,
                             global_step=global_step)

        # on a localhost with mulitple workers, there is a race condition that hangs non chief 
        # workers.   
        sess_config = tf.ConfigProto(allow_soft_placement=True, log_device_placement=True,
                                 device_filters=["/job:ps", "/job:worker/task:%d" % FLAGS.task_index])
        with sv.prepare_or_wait_for_session(server.target, config=sess_config) as sess:
            # Iterate and train.
            for step in xrange(train_size // BATCH_SIZE):
                offset = (step * BATCH_SIZE) % train_size

                # get a batch of data
                batch_data = train_data[offset:(offset + BATCH_SIZE), :]
                batch_labels = train_labels[offset:(offset + BATCH_SIZE)]

                # feed data into the model
                print "Worker main ", FLAGS.job_name, " task idx ", FLAGS.task_index, " step ", step
                _, gstep = sess.run([train_step, global_step], feed_dict={x: batch_data, y_: batch_labels})
                print "Worker main ", FLAGS.job_name, " task idx ", FLAGS.task_index, " global step ", gstep

            a, gstep = sess.run([accuracy, global_step], feed_dict={x: test_data, y_: test_labels})
            print "Global step: ", gstep, " Accuracy: ", a 

            for op in enq_ops:
                sess.run(op)
    
        sv.stop()
        print("Done!")

if __name__ == '__main__':
    tf.app.run()

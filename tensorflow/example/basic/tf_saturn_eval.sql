-- explain
select * from (
select 
dg_utils.transducer_column_int4(1) as prediction,
dg_utils.transducer_column_int4(2) as tag, 
dg_utils.transducer_column_float4(3) as x, 
dg_utils.transducer_column_float4(4) as y,
dg_utils.transducer($PHIWORKER$PhiExec python2
import vitessedata.phi
import tensorflow.python.platform
import time
import numpy as np
import tensorflow as tf
import logging

vitessedata.phi.DeclareTypes('''
//
// BEGIN INPUT TYPES
// tag int32
// x float32
// y float32
// END INPUT TYPES
//
// BEGIN OUTPUT TYPES
// predication int32
// tag int32
// x float32
// y float32
// END OUTPUT TYPES
//
''')

# Global variables.
NUM_LABELS = 2    # The number of labels.
NUM_FEATURES = 2  # The number of features
BATCH_SIZE = 100  # The number of training examples to use per training step.

def nextbatch(): 
    labels = []
    fvecs = []
    recs = []
    cnt = 0
    while True:
        if cnt == BATCH_SIZE:
            break
        rec = vitessedata.phi.NextInput()
        if not rec:
            break
        cnt += 1
        labels.append(rec[0])
        fvecs.append([rec[1], rec[2]]) 
        recs.append(rec)

    if cnt == 0:
        logging.info("nextbach return a empty batch.") 
        return cnt, None, None, None
    else:
        # Convert the array of float arrays into a numpy float matrix.
        fvecs_np = np.matrix(fvecs).astype(np.float32)

        # Convert the array of int labels into a numpy array.
        labels_np = np.array(labels).astype(dtype=np.uint8)

        # Convert the int numpy array into a one-hot matrix.
        labels_onehot = (np.arange(NUM_LABELS) == labels_np[:, None]).astype(np.float32)
        logging.info("nextbach return a batch of size %d", cnt)
        return cnt, recs, fvecs_np, labels_onehot

def main(_): 
    global_step = tf.Variable(0, trainable=False) 
    # This is where training samples and labels are fed to the graph.
    # These placeholder nodes will be fed a batch of training data at each
    # training step using the {feed_dict} argument to the Run() call below.
    x = tf.placeholder("float", shape=[None, NUM_FEATURES]) 
    y_ = tf.placeholder("float", shape=[None, NUM_LABELS])

    # These are the weights that inform how much each feature contributes to
    # the classification.
    W = tf.Variable(tf.zeros([NUM_FEATURES,NUM_LABELS]))
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

    sess_config = tf.ConfigProto(allow_soft_placement=True, log_device_placement=False)
    saver = tf.train.Saver()
    sess = tf.Session(config=sess_config)
    ckpt = tf.train.get_checkpoint_state("/home/ftian/oss/dgtools/tensorflow/example/basic/logsat_0/")
    if ckpt:
        saver.restore(sess, ckpt.model_checkpoint_path) 
    else:
        return

    while True:
        cnt, inrecs, test_data, test_labels = nextbatch()
        if cnt == 0:
            logging.info("empty batch, done.")
            break
        else:
            p, gstep = sess.run([predicted_class, global_step], feed_dict={x: test_data, y_: test_labels})
            for i in range(cnt):
                rec = inrecs[i]
                outrec = [p[i], rec[0], rec[1], rec[2]]
                vitessedata.phi.WriteOutput(outrec)

    logging.info("Flushing ...")
    vitessedata.phi.WriteOutput(None)

if __name__ == '__main__':
    tf.app.run()

$PHIWORKER$), tworker.*
from ( select tag::int4, x::float4, (x*x + y*y)::float4 from saturn_eval) tworker
) tf 
where prediction <> tag
;


-- explain
select * from (
select dg_utils.transducer($PHIWORKER$PhiExec python2
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
// cat float32
// x float32
// y float32
// END INPUT TYPES
//
// BEGIN OUTPUT TYPES
// predication int32
// tag int32
// cat float32
// x float32
// y float32
// END OUTPUT TYPES
//
''')

# Global variables.
NUM_LABELS = 2    # The number of labels.
NUM_FEATURES = 3  # The number of features
BATCH_SIZE = 100  # The number of training examples to use per training step.
NUM_HIDDEN = 20

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
        fvecs.append([rec[1], rec[2], rec[3]]) 
        recs.append(rec)

    if cnt == 0:
        return cnt, None, None, None
    else:
        # Convert the array of float arrays into a numpy float matrix.
        fvecs_np = np.matrix(fvecs).astype(np.float32)

        # Convert the array of int labels into a numpy array.
        labels_np = np.array(labels).astype(dtype=np.uint8)

        # Convert the int numpy array into a one-hot matrix.
        labels_onehot = (np.arange(NUM_LABELS) == labels_np[:, None]).astype(np.float32)
        return cnt, recs, fvecs_np, labels_onehot

# Init weights method. (Lifted from Delip Rao: http://deliprao.com/archives/100)
def init_weights(shape, init_method='xavier', xavier_params = (None, None)):
    if init_method == 'zeros':
        return tf.Variable(tf.zeros(shape, dtype=tf.float32))
    elif init_method == 'uniform':
        return tf.Variable(tf.random_normal(shape, stddev=0.01, dtype=tf.float32))
    else: #xavier
        (fan_in, fan_out) = xavier_params
        low = -4*np.sqrt(6.0/(fan_in + fan_out)) # {sigmoid:4, tanh:1} 
        high = 4*np.sqrt(6.0/(fan_in + fan_out))
        return tf.Variable(tf.random_uniform(shape, minval=low, maxval=high, dtype=tf.float32))

def main(_): 
    global_step = tf.Variable(0, trainable=False) 

    # This is where training samples and labels are fed to the graph.
    # These placeholder nodes will be fed a batch of training data at each
    # training step using the {feed_dict} argument to the Run() call below.
    x = tf.placeholder("float", shape=[None, NUM_FEATURES])
    y_ = tf.placeholder("float", shape=[None, NUM_LABELS])
    
    # Define and initialize the network.
    # Initialize the hidden weights and biases.
    w_hidden = init_weights(
        [NUM_FEATURES, NUM_HIDDEN], 
        'xavier', xavier_params=(NUM_FEATURES, NUM_HIDDEN)) 

    b_hidden = init_weights([1,NUM_HIDDEN],'zeros') 

    # The hidden layer.
    hidden = tf.nn.tanh(tf.matmul(x,w_hidden) + b_hidden)

    # Initialize the output weights and biases.
    w_out = init_weights(
            [NUM_HIDDEN, NUM_LABELS],
            'xavier', xavier_params=(NUM_HIDDEN, NUM_LABELS))

    b_out = init_weights([1,NUM_LABELS],'zeros')

    # The output layer.
    y = tf.nn.softmax(tf.matmul(hidden, w_out) + b_out)
    
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
    ckpt = tf.train.get_checkpoint_state("/home/ftian/oss/dgtools/tensorflow/example/basic/logh3_0/")
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
                outrec = [p[i], rec[0], rec[1], rec[2], rec[3]]
                vitessedata.phi.WriteOutput(outrec)

    logging.info("Flushing ...")
    vitessedata.phi.WriteOutput(None)

if __name__ == '__main__':
    tf.app.run()

$PHIWORKER$,
tworker.*),
dg_utils.transducer_column_int4(1) as prediction,
dg_utils.transducer_column_int4(2) as tag, 
dg_utils.transducer_column_float4(3) as cat, 
dg_utils.transducer_column_float4(4) as x, 
dg_utils.transducer_column_float4(5) as y 
from ( select tag, 
       case when cat = 'linear' then 1.0::float4
            when cat = 'moon' then 2.0::float4
            else 3.0::float4 end,
       x::float4, y::float4 from tf_eval ) tworker
) tf 
where prediction <> tag
;


import numpy as np
import tensorflow as tf

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
    test_data_filename = "simdata/linear_data_eval.csv"
    test_data, test_labels = extract_data(test_data_filename)
    test_size,num_features = test_data.shape

    print 'test_size: ', test_size, 'num_feature', num_features
    # For the test data, hold the entire dataset in one constant node.
    test_data_node = tf.constant(test_data)

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


    sess_config = tf.ConfigProto(allow_soft_placement=True, log_device_placement=False)
    saver = tf.train.Saver()
    session = tf.Session(config=sess_config)
    ckpt = tf.train.get_checkpoint_state("./logs_0/")
    if ckpt:
        print "Restore chk point from ", ckpt.model_checkpoint_path
        saver.restore(session, ckpt.model_checkpoint_path) 
    else:
        print "Error: cannot restore ckpt" 
        return

    p, a, gstep = session.run([predicted_class, accuracy, global_step], feed_dict={x: test_data, y_: test_labels})
    p, a, gstep = session.run([predicted_class, accuracy, global_step], feed_dict={x: test_data, y_: test_labels})
    print "Global step: ", gstep, " Accuracy: ", a 
    print "Predicted class: ", p

if __name__ == '__main__':
    tf.app.run()
 

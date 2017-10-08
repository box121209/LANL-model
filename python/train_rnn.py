# -*- coding: utf-8 -*-

"""
Example usage:

python ./python/train_rnn.py flows_for_rnn_model_1000.csv.bz2 -arch 16,16

"""

import sys, time, os, shutil, time
import numpy as np
import csv, bz2, json
import re
import math
import matplotlib.pyplot as plt

#####################################################################
# read input parameters

# defaults:
outfile = ""
niters = 10
nhidden = [128]
unroll = 20
step = 3
dropout = 0.1
batch_size = 128
verbose = 1
datapath = './sdata/'
modelpath = './models/'
imgpath = './img/'
boxfile = datapath + 'boxport_data.json'
coordfile = datapath + 'box_coords.csv'

# command line:
try:
    infile = sys.argv[1];           del sys.argv[1]
except:
    print "Usage: python ", sys.argv[0], "infile [options]"
    print "Options are:"
    print "        -o outfile "
    print "        -datapath [./sdata]"
    print "        -modelpath [./models]"
    print "        -unroll [20]"
    print "        -step [3]"
    print "        -dropout [0.1]"
    print "        -niters [10]"
    print "        -arch (hidden layer sizes) [128]"
    sys.exit(1)

while len(sys.argv) > 1:
    option = sys.argv[1];               del sys.argv[1]
    if option == '-o':
        outfile = sys.argv[1];          del sys.argv[1]
    elif option == '-datapath':
        datapath = sys.argv[1];          del sys.argv[1]
    elif option == '-modelpath':
        modelpath = sys.argv[1];          del sys.argv[1]
    elif   option == '-unroll':
        unroll = int(sys.argv[1]);      del sys.argv[1]
    elif option == '-step':
        step = int(sys.argv[1]);        del sys.argv[1]
    elif option == '-dropout':
        dropout = float(sys.argv[1]);   del sys.argv[1]
    elif option == '-niters':
        niters = int(sys.argv[1]);      del sys.argv[1]
    elif option == '-arch':
        nhidden = [int(x) for x in sys.argv[1].split(',')]
        del sys.argv[1]
    else:
        print sys.argv[0],': invalid option', option
        sys.exit(1)

#####################################################################
# set output file

if outfile == "":
    outfile = "model_from_%s_arch" % infile
for i in nhidden: 
    outfile += "_%d" % i
outfile += "_unroll_%d_step_%d_dropout_%g" % (unroll, step, dropout)


#####################################################################
# load box/port info

boxdata = json.load(open(boxfile, 'r'))

def jsonIntKeys(x):
    if isinstance(x, dict):
            return {int(k):str(v) for k,v in x.items()}
    return x

def jsonIntVals(x):
    if isinstance(x, dict):
            return {str(k):int(v) for k,v in x.items()}
    return x

box_index = jsonIntVals( boxdata['box_indices'] )
index_box = jsonIntKeys( boxdata['indices_box'] )
port_index = jsonIntVals( boxdata['port_indices'] )
index_port = jsonIntKeys( boxdata['indices_port'] )

bbox = set([re.sub('[\"\n]+', "", s) for s in box_index.keys()])

#####################################################################
# set RNN input vector sizes

lg_duration = 24
lg_packets = 32
lg_bytes = 32
nr_proto = 4
nr_box = len(index_box) 
nr_port = len(index_port)

#####################################################################
# load box embedding

fp = open(coordfile, 'r')
boxcoords = {line[0]: [float(line[i]) for i in range(1,5)] for line in csv.reader(fp)}
fp.close()

comp_embed = np.zeros([nr_box, 4])
for b in boxcoords.keys():
    comp_embed[box_index[b],:] = boxcoords[b]


#####################################################################
# check hardware

from tensorflow.python.client import device_lib

print("Checking hardware ...")
print(device_lib.list_local_devices())

#####################################################################
# build the model: stacked LSTM

sys.setrecursionlimit(10000) # needed to prevent query-time bug...

from keras.layers import Input, Embedding, LSTM, Dense, Dropout
from keras.models import Model
from keras.layers.merge import Concatenate

nlayers = len(nhidden)

# hyperparameters:
embed_dim = 4
dropout_W = 0.1 # input gates
dropout_U = 0.1 # recurrent connections

# netflow inputs:
src_input = Input(shape=(unroll,), dtype='int32', name='src_input')
dst_input = Input(shape=(unroll,), dtype='int32', name='dst_input')
src_pt_input = Input(shape=(unroll,), dtype='int32', name='src_pt')
dst_pt_input = Input(shape=(unroll,), dtype='int32', name='dst_pt')
proto_input = Input(shape=(unroll,), dtype='int32', name='proto')
duration_input = Input(shape=(unroll,), dtype='int32', name='duration')
packets_input = Input(shape=(unroll,), dtype='int32', name='packets')
bytes_input = Input(shape=(unroll,), dtype='int32', name='bytes')

# shared embedding for computer feeds:
comp_encoding = Embedding(output_dim=embed_dim, input_dim=nr_box, input_length=unroll, weights=[comp_embed], trainable=False)
src = comp_encoding(src_input)
dst = comp_encoding(dst_input)

# other embeddings:
src_pt = Embedding(output_dim=128, input_dim=nr_port, input_length=unroll)(src_pt_input)
dst_pt = Embedding(output_dim=128, input_dim=nr_port, input_length=unroll)(dst_pt_input)
proto = Embedding(output_dim=2, input_dim=nr_proto, input_length=unroll)(proto_input)
duration = Embedding(output_dim=4, input_dim=lg_duration, input_length=unroll)(duration_input)
packets = Embedding(output_dim=8, input_dim=lg_packets, input_length=unroll)(packets_input)
bytes = Embedding(output_dim=16, input_dim=lg_bytes, input_length=unroll)(bytes_input)

# merge:
data_merged = Concatenate()([src_pt, dst_pt, proto, duration, packets, bytes])

# add src computer for next time-step, as a query stream to train on:
next_src = Input(shape=(unroll,), dtype='int32', name='next_src')
query = comp_encoding(next_src)

# pass data and query to RNN layers:
inner = Concatenate()([data_merged, src, dst, query])
for i in range(len(nhidden)-1):
    inner = LSTM(nhidden[i], return_sequences=True, dropout=dropout_U, recurrent_dropout=dropout_W)(inner)
inner = LSTM(nhidden[-1], return_sequences=False, dropout=dropout_U, recurrent_dropout=dropout_W)(inner)
inner = Dropout(dropout_W)(inner)

# add softmax outputs:
proto_output = Dense(4, activation='softmax', name='proto_output')(inner)
duration_output = Dense(lg_duration, activation='softmax', name='duration_output')(inner)
packets_output = Dense(lg_packets, activation='softmax', name='packets_output')(inner)
bytes_output = Dense(lg_bytes, activation='softmax', name='bytes_output')(inner)
src_port_output = Dense(nr_port, activation='softmax', name='src_port_output')(inner)
dst_port_output = Dense(nr_port, activation='softmax', name='dst_port_output')(inner)

# add dst computer output:
next_dst = Dense(embed_dim, activation='relu', name='next_dst')(inner)

# put it all together:
model = Model(inputs=[src_input,
                     dst_input,
                     src_pt_input,
                     dst_pt_input,
                     proto_input,
                     duration_input,
                     packets_input, 
                     bytes_input,
                     next_src], 
              outputs=[proto_output,
                      duration_output,
                      packets_output,
                      bytes_output,
                      src_port_output,
                      dst_port_output,
                      next_dst])

loss_weights = [0/0.5, # proto
                0/1.5,   # duration
                0/1.5,   # packets
                0/2.0,   # bytes
                0/1.0,   # src_port
                0/1.0,   # dst_prt
                1/500.0  # next_dst
               ]
model.compile(optimizer='rmsprop', 
              loss=['categorical_crossentropy' for i in range(6)] + ['mse'],
              loss_weights=loss_weights )

print("Model architecture:")
print(model.summary())

#####################################################################
# load current weights

try:
    model.set_weights(np.load("%s" % modelpath + outfile))
    print("Initialising with weights found at %s.npy" % modelpath + outfile)
except:
    print("Can't find existing model, initialising random weights")


#####################################################################
# read training data

print("Reading data file %s ..." % datapath + infile)
t0 = time.clock()
bz_file = bz2.BZ2File(datapath + infile)
lines = bz_file.readlines()
nlines = len(lines)
flowdata = [[int(x) for x in line[:-1].split(',')] for line in lines[:nlines]]
t1 = time.clock()
print("Read %d records in %g seconds" % (nlines, (t1 - t0)))


#####################################################################
# prepare and run training

def make_input_vectors(z, mx, unroll=unroll):
    n = len(z)
    X = np.zeros((n - unroll, unroll), dtype='int32')
    for i in range(n - unroll):
        X[i,:] = z[i: i + unroll]
    return X

def make_onehot(x, n):
    N = len(x)-unroll
    out = np.zeros([N,n])
    for i in range(1,N):
        out[i, x[i]] = 1
    return out

def make_nextdst(x, cmp_code):
    N = len(x)-unroll
    out = np.zeros([N, embed_dim])
    for i in range(N):
        out[i,:] = cmp_code[x[i],:]
    return out

def make_train_seq(start, end):
    out = {}
    raw = np.array(flowdata[start:(end+1)])
    n = end - start
    out['input'] = [make_input_vectors(raw[range(n), 2], nr_box),
                    make_input_vectors(raw[range(n), 4], nr_box),
                    make_input_vectors(raw[range(n), 3], nr_port), 
                    make_input_vectors(raw[range(n), 5], nr_port),
                    make_input_vectors(raw[range(n), 6], nr_proto),
                    make_input_vectors(raw[range(n), 1], lg_duration),
                    make_input_vectors(raw[range(n), 7], lg_packets),
                    make_input_vectors(raw[range(n),8], lg_bytes),
                    make_input_vectors(raw[range(1,n+1),2], nr_box)]
    out['target'] = [make_onehot(raw[range(1,n+1), 6], nr_proto),
                     make_onehot(raw[range(1,n+1), 1], lg_duration),
                     make_onehot(raw[range(1,n+1), 7], lg_packets),
                     make_onehot(raw[range(1,n+1),8], lg_bytes),
                     make_onehot(raw[range(1,n+1), 3], nr_port),
                     make_onehot(raw[range(1,n+1), 5], nr_port),
                     make_nextdst(raw[range(1,n+1), 4], comp_embed)]
    out['raw'] = raw
    return out

train = make_train_seq(0, nlines-1)

from keras.callbacks import Callback

class LossHistory(Callback):
    def on_train_begin(self, logs={}):
        self.losses = {'total':[], 
                       'proto':[], 
                       'duration':[], 
                       'packets':[],
                       'bytes':[], 
                       'src_port':[], 
                       'dst_port':[], 
                       'next_dst':[]}
    def on_batch_end(self, batch, logs={}):
        self.losses['total'].append(logs.get('loss'))
        self.losses['proto'].append(logs.get('proto_output_loss'))
        self.losses['duration'].append(logs.get('duration_output_loss'))
        self.losses['packets'].append(logs.get('packets_output_loss'))
        self.losses['bytes'].append(logs.get('bytes_output_loss'))
        self.losses['src_port'].append(logs.get('src_port_output_loss'))
        self.losses['dst_port'].append(logs.get('dst_port_output_loss'))
        self.losses['next_dst'].append(logs.get('next_dst_loss'))

history = LossHistory()

print("Fitting model ...")
qiters = niters / 5
riters = niters % 5
t0 = time.clock()
for i in range(qiters):
    print("Epoch block %d:" % i)
    model.fit(train['input'], train['target'],
              epochs=5,
              verbose=1,
              callbacks=[history])
    np.save("%s%s_iter_%d.npy" % (modelpath, outfile, 5*(i+1)),
            model.get_weights())
if riters > 0:
    print("Epoch block %d:" % qiters)
    model.fit(train['input'], train['target'],
              epochs=riters,
              verbose=1,
              callbacks=[history])
    np.save("%s/%s_iter_%d.npy" % (modelpath, outfile, niters), model.get_weights())
t1 = time.clock()
print("Done in %g seconds" % (t1 - t0))

#####################################################################
# report and plot history

print("Model written to %s%s" % (modelpath, outfile))

def movingaverage(x, window_size):
    window = np.ones(int(window_size))/float(window_size)
    return np.convolve(x, window, 'same')

total_loss = history.losses['total']/np.max(history.losses['total'])
proto_loss = history.losses['proto']/np.max(history.losses['proto'])
duration_loss = history.losses['duration']/np.max(history.losses['duration'])
packets_loss = history.losses['packets']/np.max(history.losses['packets'])
bytes_loss = history.losses['bytes']/np.max(history.losses['bytes'])
src_port_loss = history.losses['src_port']/np.max(history.losses['src_port'])
dst_port_loss = history.losses['dst_port']/np.max(history.losses['dst_port'])
next_dst_loss = history.losses['next_dst']/np.max(history.losses['next_dst'])

fig = plt.figure(figsize=(20,10))
wsize = 20

plt.plot(movingaverage(total_loss, wsize), label='total')
plt.plot(movingaverage(proto_loss, wsize), label='protocol')
plt.plot(movingaverage(duration_loss, wsize), label='duration')
plt.plot(movingaverage(packets_loss, wsize), label='packets')
plt.plot(movingaverage(bytes_loss, wsize), label='bytes')
plt.plot(movingaverage(src_port_loss, wsize), label='src port')
plt.plot(movingaverage(dst_port_loss, wsize), label='dst port')
plt.plot(movingaverage(next_dst_loss, wsize), label='next dst')

plt.legend(loc='lower center')
fig.savefig('%s%s.png' % (imgpath, outfile))
print("History plot written to %s%s" % (imgpath, outfile))

#####################################################################

# EC2 GPU system prep for deep learning

# See:
# https://www.tensorflow.org/install/install_linux
#
# Choose Ubuntu 16.04 GPU p2.xlarge (Tesla K80)
# CUDA Toolkit 8.0
# cuDNN 6.0
# Tensorflow, Keras, Jupyter
# Install with Virtualenv

# check GPU

lspci -nnk | grep -i nvidia

# install basics

sudo apt-get update
#sudo apt-get install libglu1-mesa libxi-dev libxmu-dev -y
#sudo apt-get — yes install build-essential
sudo apt-get install python-pip python-dev -y
sudo apt-get install python-numpy python-scipy -y
sudo apt-get install -y linux-headers-$(uname -r)
sudo apt-get install -y gcc
sudo apt-get install -y make
sudo apt-get install -y g++
sudo apt-get install git

# clone git repos

git clone https://github.com/box121209/dnn-tmp
git clone https://github.com/box121209/cc-domain-graph

# CUDA drivers and toolkit
# see
# http://docs.nvidia.com/cuda/cuda-installation-guide-linux/index.html

mkdir packages
cd packages

wget https://developer.nvidia.com/compute/cuda/8.0/Prod2/local_installers/cuda-repo-ubuntu1604-8-0-local-ga2_8.0.61-1_amd64-deb
sudo dpkg -i cuda-repo-ubuntu1604-8-0-local-ga2_8.0.61-1_amd64-deb
sudo apt-get update
sudo apt-get install -y cuda
export PATH=$PATH:/usr/local/cuda-8.0/bin
export LD_LIBRARY_PATH=/usr/local/cuda-8.0/lib64\
                         ${LD_LIBRARY_PATH:+:${LD_LIBRARY_PATH}}
export CUDA_HOME=/usr/local/cuda-8.0

# verify installation

nvidia-smi
nvcc --version
sudo modprobe nvidia
cd /usr/local/cuda/samples/1_Utilities/deviceQuery
sudo make
./deviceQuery
cd ~/packages

# install cuDNN
# NOTE: TF needs cuDNN v6; v7 doesn't work.
# See
# http://docs.nvidia.com/deeplearning/sdk/cudnn-install/index.html

cp ~/dnn-tmp/lib/* .

sudo mv /usr/lib/nvidia-375/libEGL.so.1 /usr/lib/nvidia-375/libEGL.so.1.org
sudo mv /usr/lib32/nvidia-375/libEGL.so.1 /usr/lib32/nvidia-375/libEGL.so.1.org
sudo ln -s /usr/lib/nvidia-375/libEGL.so.375.39 /usr/lib/nvidia-375/libEGL.so.1
sudo ln -s /usr/lib32/nvidia-375/libEGL.so.375.39 /usr/lib32/nvidia-375/libEGL.so.1

sudo dpkg -i libcudnn6_6.0.21-1+cuda8.0_amd64.deb
sudo dpkg -i libcudnn6-dev_6.0.21-1+cuda8.0_amd64.deb
sudo dpkg -i libcudnn6-doc_6.0.21-1+cuda8.0_amd64.deb

# testing:
cp -r /usr/src/cudnn_samples_v6/ $HOME
cd $HOME/cudnn_samples_v6/mnistCUDNN
make clean; make
./mnistCUDNN


# install tensorflow using Virtualenv
# see
# https://www.tensorflow.org/install/install_linux

cd $HOME/packages
sudo apt-get install libcupti-dev
sudo apt-get install python-pip python-dev python-virtualenv

mkdir ~/tensorflow
sudo virtualenv --system-site-packages ~/tensorflow 
source ~/tensorflow/bin/activate

sudo pip install --upgrade tensorflow 
sudo pip install --upgrade tensorflow-gpu 
sudo pip install --upgrade https://storage.googleapis.com/tensorflow/linux/gpu/tensorflow_gpu-1.3.0-cp27-none-linux_x86_64.whl


# install jupyter and keras etc

sudo apt-get update
sudo apt-get -y install ipython ipython-notebook
sudo pip install --upgrade pip
sudo pip install jupyter
sudo pip install keras
sudo pip install boto

# configure Jupyter password

jupyter notebook --generate-config
jupyter notebook password

cd $HOME/cc-domain-graph

# jupyter notebook &
# nohup python ./python/train_rnn.py big_domain_string_1.gz -arch 16,16 -niters 64 > log.out &

# Post-compute:

sudo apt install awscli

mkdir $HOME/models
cp $HOME/cc-domain-graph/sdata/model* $HOME/models
aws s3 sync $HOME/models s3://billsdata.net/CommonCrawl

# and update git repo:

cd $HOME/cc-domain-graph
git init
git add . --all
git commit -m "EC2 experiments"
git remote add cc-domain-graph https://github.com/box121209/cc-domain-graph
git push -u cc-domain-graph master




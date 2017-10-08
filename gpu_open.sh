KEY="/Users/49269/.ssh/Billsdata-20170604.pem"

[ $# -eq 0 ] && { echo "Usage: $0 AWS_Hostname"; exit 1; }

# Copy setup script to EC2 instance:
scp -i $KEY ./gpu_setup.sh ubuntu@$1:~

# Login to master, with tunnels for Jupyter and TensorBoard:
ssh -i $KEY -L 8888:localhost:8888 -L 6006:localhost:6006 ubuntu@$1

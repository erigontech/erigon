#!/usr/bin/env sh
# This is an init script that sould be used for GCP VM initialization.
# Do not perform any changes at VM without modyfying this script
set -e

ERIGON_UID=942
BUCKET_PATH=is-env-confg/erigon

# Installing packages
gcsFuseRepo=gcsfuse-`lsb_release -c -s`;
echo "deb http://packages.cloud.google.com/apt $gcsFuseRepo main" | tee /etc/apt/sources.list.d/gcsfuse.list;
curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key add -;
apt-get update
apt-get -y upgrade
apt-get -y install bash-completion vim tmux vim mc docker.io docker-compose gcsfuse
apt-get clean


# Docker 
systemctl enable docker
systemctl start docker

# Configure user
adduser --disabled-password --uid $ERIGON_UID --gecos $ERIGON_UID --home /erigon erigon
sudo usermod -aG docker erigon

# Authorize docker/GCP
su erigon -c "printf 'yes' | gcloud auth configure-docker us-central1-docker.pkg.dev"

su erigon -c "mkdir -p ~/scripts"
su erigon -c "gsutil cp gs://$BUCKET_PATH/* ~/scripts/"
su erigon -c "chmod +x ~/scripts/update.sh && ~/scripts/update.sh"

su erigon -c "~/scripts/compose-reinit.sh"

echo "Initialization completed \\(^_^)/ !"
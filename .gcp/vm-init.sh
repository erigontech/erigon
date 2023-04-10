#!/usr/bin/env sh
# This is an init script that sould be used for GCP VM initialization.
# https://cloud.google.com/compute/docs/instances/startup-scripts
# Do not perform any direct changes at VM without modyfying this script
#
# To filter these messages in logs add to query
# jsonPayload.message=~"^startup-script-url"
set -e

ERIGON_UID=942
BUCKET_PATH=is-env-confg/erigon

# Installing packages
# require ca-certificates curl gnupg
gcsFuseRepo=gcsfuse-`lsb_release -c -s`;
echo "deb [signed-by=/etc/apt/keyrings/google-cloud.gpg] http://packages.cloud.google.com/apt $gcsFuseRepo main" | \
  tee /etc/apt/sources.list.d/gcsfuse.list;

echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/debian \
  $(. /etc/os-release && echo "$VERSION_CODENAME") stable" | \
  tee /etc/apt/sources.list.d/docker.list > /dev/null

mkdir -m 0755 -p /etc/apt/keyrings
curl -fsSL https://packages.cloud.google.com/apt/doc/apt-key.gpg | gpg --dearmor -o /etc/apt/keyrings/google-cloud.gpg
chmod a+r /etc/apt/keyrings/google-cloud.gpg

curl -fsSL https://download.docker.com/linux/debian/gpg | gpg --dearmor -o /etc/apt/keyrings/docker.gpg
chmod a+r /etc/apt/keyrings/docker.gpg

echo "Update & upgrade packages"
apt-get update
apt-get -y upgrade

echo "Installing basic packages"
#Could also install here:  gcsfuse 
apt-get -y install bash-completion vim tmux vim mc

echo "Installing docker"
apt-get -y install docker-ce docker-ce-cli containerd.io docker-compose-plugin

echo "Installation complete - cleaning"
apt-get clean


echo "Configuring docker"
systemctl enable docker
systemctl start docker

echo "Configuring user"
set +e
userdel -fr erigon
set -e
adduser --disabled-password --uid $ERIGON_UID --gecos $ERIGON_UID --home /erigon erigon
sudo usermod -aG docker erigon

mkdir -p /erigon/.ssh
echo -e "ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQDWJXs3KUVwNmiUcHtRmYw6XWyS58aK4u68DIVsVEBz3RSpdlyLbPbPY1V19cQeUoS+IxKm9q6agm/4XrhOY4ReALDZ1Wmyq9Oy4ZA8tEogU0cUly+WIRgO7mjD+783J0+UIp9mmsxE0qQt0RdmWeh/74v+5YN3qanh09bxSrQ5vTFShBdl5n9fv8OMaw7xM4Etnv+RsichQWjXHDwnrAuGmt9yGRbpmGweX+RBusCjvG05jdnYv8AqlyNnTyp3f3zAOcHe69CNG/noHv22LCzAkYbl8xK74Kckopqlg2z3KDRUuzIglSt16Q5hr6GDLPP2iiJhfiUYEX8kcJz2txbz erigon\n" > /erigon/.ssh/authorized_keys
chmod -R erigon:erigon /erigon/.ssh

echo "Authorize docker/GCP"
su erigon -c "printf 'yes' | gcloud auth configure-docker us-central1-docker.pkg.dev"

echo "Prepare scripts"
su erigon -c "mkdir -p ~/scripts"
su erigon -c "gsutil cp gs://$BUCKET_PATH/* ~/scripts/"
su erigon -c "chmod +x ~/scripts/update.sh && ~/scripts/update.sh"

echo "Creating docker compose env"
su erigon -c "~/scripts/env-clean-start.sh"

echo "Initialization completed \\(^_^)/ !"

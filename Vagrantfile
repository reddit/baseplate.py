Vagrant.configure(2) do |config|
  config.vm.box = "trusty-cloud-image"
  config.vm.box_url = "https://cloud-images.ubuntu.com/vagrant/trusty/current/trusty-server-cloudimg-amd64-vagrant-disk1.box"
  config.vm.hostname = "baseplate.vm"

  config.vm.provision :puppet do |puppet|
    puppet.manifests_path = "./manifests"
    puppet.manifest_file = "init.pp"
  end

  # project synced folder
  config.vm.synced_folder  ".", "/home/vagrant/baseplate"
end

Vagrant.configure(2) do |config|

  config.vm.box = "trusty-cloud-image"
  config.vm.hostname = "baseplate.vm"

  config.vm.provider :virtualbox do |vb|
    vb.customize ["modifyvm", :id, "--memory", "8096"]
  end

  config.vm.provision :puppet do |puppet|
    puppet.manifests_path = "./manifests"
    puppet.manifest_file = "init.pp"
  end

  # project synced folder
  config.vm.synced_folder  ".", "/home/vagrant/baseplate"
end

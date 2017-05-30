Vagrant.configure(2) do |config|
  config.vm.box = "trusty-cloud-image"
  config.vm.box_url = "https://cloud-images.ubuntu.com/vagrant/trusty/current/trusty-server-cloudimg-amd64-vagrant-disk1.box"
  config.vm.network "private_network", type: "dhcp"
  config.vm.hostname = "baseplate.local"

  config.vm.provider :virtualbox do |vb|
    vb.customize ["modifyvm", :id, "--memory", "4096"]
  end

  config.vm.provision :puppet do |puppet|
    puppet.manifests_path = "./puppet"
    puppet.manifest_file = "init.pp"
    puppet.module_path = "./puppet/modules"
    puppet.facter = {
      "user" => "vagrant",
      "project_path" => "/home/vagrant/baseplate",
    }
  end

  config.vm.synced_folder  ".", "/home/vagrant/baseplate"
end

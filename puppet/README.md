This directory contains puppet manifests that configure a development
environment for Baseplate.

These manifests are applied automatically if you use Vagrant. To apply them
manually, run the following from the root of the project:

    sudo FACTER_user=$USER FACTER_project_path=$PWD puppet apply --modulepath puppet/modules puppet/init.pp

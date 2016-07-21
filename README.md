This file includes sample code to configure a Virtual SAN cluster as a Stretched Cluster or 2 Node configuration.

The code has been tested with the following cases

Testbed: One cluster with four hosts. Each host has one 50G SSD and two 100G SSD

Preconditions:

Please make sure your VSAN's license contain streched cluster
The cluster has VSAN turned on with or without disks claimed
There are no fault domains configured
There is no witness VM deployed
There is no witness host added
The witness host contain available SSD/HDDs
Case1: Create streched cluster with two fault domains f1 and f2 with virtual witness host

python stretchedCluster.py -s <VCENTERSERVER> -u administrator@vsphere.local -p ****  --preferdomain f1 --seconddomain f2 --ovfurl 'http://server/VMware-VirtualSAN-Witness-6.0.0.update02-3620759.ovf' --witness-vmhost <ESXIHOST> --witness-datastore vsanDatastore --witness-vmname witness --witness-vmpassword **** --witnessdc VSAN-DC --witness-vmdatacenter VSAN-DC

Case2: Create streched cluster with two fault domains f1 and f2 with physical witness host

python stretchedCluster.py -s <VCENTERSERVER> -u administrator@vsphere.local -p ****  --preferdomain f1 --seconddomain f2 --witnesshost <WITNESSHOST> --witnesshost-user root --witnesshost-pwd **** --witnessdc VSAN-DC

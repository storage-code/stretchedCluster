#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Copyright 2016 VMware, Inc.  All rights reserved.

This file includes sample codes about convert VSAN cluster to stretched cluster
Before running this script, please make sure you have set up a VSAN cluster

"""

__author__ = 'VMware, Inc'

from pyVim.connect import SmartConnect, Disconnect
from pyVmomi import vim
import sys
import ssl
import atexit
import argparse
import getpass
import urllib, urllib2
import time
import urlparse
import re
import httplib
import pyVim
# import the VSAN API python bindings
import vsanmgmtObjects
import vsanapiutils

'''
The code has been tested with the following cases
Testbed: One cluster with four hosts,Each host has one 50G SSD and two 100G SSD
Preconditions:
1>Please make sure your VSAN's license contain streched cluster
2>The cluster has VSAN turned on with or without disks claimed
3>There are no fault domains configured
4>There is no witness VM deployed
5>There is no witness host added
6>The witness host contain available SSD/HDDs

Case1: Create streched cluster with two fault domains f1 and f2 with virtual witness host
python stretchedCluster.py -s 10.162.3.79 -u administrator@vsphere.local -p ****  --preferdomain f1 --seconddomain f2
--ovfurl 'http://server/VMware-VirtualSAN-Witness-6.0.0.update02-3620759.ovf'
--witness-vmhost 10.162.5.67 --witness-datastore vsanDatastore --witness-vmname witness
--witness-vmpassword **** --witnessdc VSAN-DC --witness-vmdatacenter VSAN-DC

Case2: Create streched cluster with two fault domains f1 and f2 with physical witness host
python stretchedCluster.py -s 10.162.3.79 -u administrator@vsphere.local -p ****  --preferdomain f1 --seconddomain f2
--witnesshost 10.162.9.140 --witnesshost-user root --witnesshost-pwd **** --witnessdc VSAN-DC
'''

def GetArgs():
   """
   Supports the command-line arguments listed below.
   """
   parser = argparse.ArgumentParser(
           description='Process args for VSAN SDK sample application')
   parser.add_argument('-s', '--host', required=True, action='store',
                       help='Remote host to connect to')
   parser.add_argument('-o', '--port', type=int, default=443, action='store',
                       help='Port to connect on')
   parser.add_argument('-u', '--user', required=True, action='store',
                       help='User name to use when connecting to host')
   parser.add_argument('-p', '--password', required=False, action='store',
                       help='Password to use when connecting to host')
   parser.add_argument('--cluster', dest='clusterName', metavar="CLUSTER",
                       default='VSAN-Cluster')
   parser.add_argument('--preferdomain', action='store',
                       help='Prefered fault domains name')
   parser.add_argument('--seconddomain', action='store',
                       help='Secondary fault domains name')
   parser.add_argument('--ovfurl', action='store',
                       help='witness ovf url')
   parser.add_argument('--witness-vmdatacenter', dest = 'datacenter', action='store',
                       help='Data center to deploy witness vm')
   parser.add_argument('--witness-vmhost', dest = 'vmhost', action='store',
                       help='witness vm container host')
   parser.add_argument('--witness-datastore', dest ='datastore', action='store',
                       help='witness vm located datastore')
   parser.add_argument('--witness-network', dest='network', action='store',
                       help='witness vm network name')
   parser.add_argument('--witness-vmname', dest= 'name', action='store',
                       help='witness vm name')
   parser.add_argument('--witness-vmpassword', dest = 'vmpassword', action='store',
                       help='witness vm password')
   parser.add_argument('--witnessdc', action='store',
                       help='Data center to add witness host')
   parser.add_argument('--witnesshost', action='store',
                       help='witness host name')
   parser.add_argument('--witnesshost-user', dest='witnesshostuser', action='store',
                       help='witness host admin user')
   parser.add_argument('--witnesshost-pwd', dest='witnesshostpwd', action='store',
                       help='witness host password')

   args = parser.parse_args()
   return args


def getClusterInstance(clusterName, serviceInstance):
   content = serviceInstance.RetrieveContent()
   searchIndex = content.searchIndex
   datacenters = content.rootFolder.childEntity
   for datacenter in datacenters:
      cluster = searchIndex.FindChild(entity = datacenter.hostFolder, name = clusterName)
      if cluster is not None:
         return cluster
   return None

def yes(ques) :
   "Force the user to answer 'yes' or 'no' or something similar. Yes returns true"
   while 1 :
      ans = raw_input(ques)
      ans = str.lower(ans[0:1])
      return True if ans == 'y' else False

def getHostSystem(hostname, dcRef, si):
   searchIndex = si.content.searchIndex
   hostSystem = searchIndex.FindByIp(datacenter = dcRef, ip = hostname, vmSearch = False) or \
                searchIndex.FindByDnsName(datacenter = dcRef, dnsName = hostname, vmSearch = False)
   return hostSystem


def CollectMultiple(content, objects, parameters, handleNotFound=True):
   if len(objects) == 0:
      return {}
   result = None
   pc = content.propertyCollector
   propSet = [vim.PropertySpec(
           type=objects[0].__class__,
           pathSet=parameters
   )]

   while result == None and len(objects) > 0:
      try:
         objectSet = []
         for obj in objects:
            objectSet.append(vim.ObjectSpec(obj=obj))
         specSet = [vim.PropertyFilterSpec(objectSet=objectSet, propSet=propSet)]
         result = pc.RetrieveProperties(specSet=specSet)
      except vim.ManagedObjectNotFound as ex:
         objects.remove(ex.obj)
         result = None

   out = {}
   for x in result:
      out[x.obj] = {}
      for y in x.propSet:
         out[x.obj][y.name] = y.val
   return out


def createHttpConn(protocol, hostPort):
   '''
   Construct a http/s connection
   @param protocol:
   @param hostPort:
   @return:http connect
   '''
   if protocol == 'https':
      return httplib.HTTPSConnection(hostPort)
   return httplib.HTTPConnection(hostPort)


def splitURL(url):
   '''
   Get protocol, hostport, request from url
   @param url: request url
   @return: connect protocol, hostport and request string
   '''
   urlMatch = re.search("^(https?|ftps?|file?)://(.+?)(/.*)$", url)
   protocol = urlMatch.group(1)
   hostPort = urlMatch.group(2)
   reqStr = urlMatch.group(3)
   return (protocol, hostPort, reqStr)


def uploadFile(srcURL, dstURL, create, lease, minProgress, progressIncrement,
               vmName=None, log=None):
   '''
   This function will upload vmdk file to vc by using http protocol
   @param srcURL: source url
   @param dstURL: destnate url
   @param create: http request method
   @param lease: HttpNfcLease object
   @param minProgress: file upload progress initial value
   @param progressIncrement: file upload progress update value
   @param vmName: imported virtual machine name
   @param log: log object
   @return:
   '''
   srcData = urllib2.urlopen(srcURL)
   length = int(srcData.headers['content-length'])
   ssl._create_default_https_context = ssl._create_unverified_context
   protocol, hostPort, reqStr = splitURL(dstURL)
   dstHttpConn = createHttpConn(protocol, hostPort)
   reqType = create and 'PUT' or 'POST'
   dstHttpConn.putrequest(reqType, reqStr)
   dstHttpConn.putheader('Content-Length', length)
   dstHttpConn.endheaders()

   bufSize = 1048768  # 1 MB
   total = 0
   progress = minProgress
   if log:
      # If args.log is available, then log to it
      log = log.info
   else:
      log = sys.stdout.write
   log("%s: %s: Start: srcURL=%s dstURL=%s\n" % (time.asctime(time.localtime()), vmName, srcURL, dstURL))
   log(
      "%s: %s:    progress=%d total=%d length=%d\n" % (time.asctime(time.localtime()), vmName, progress, total, length))
   while True:
      data = srcData.read(bufSize)
      if lease.state != vim.HttpNfcLease.State.ready:
         break
      dstHttpConn.send(data)
      total = total + len(data)
      progress = (int)(total * (progressIncrement) / length)
      progress += minProgress
      lease.Progress(progress)
      if len(data) == 0:
         break

   log("%s: %s: Finished: srcURL=%s dstURL=%s\n" % (time.asctime(time.localtime()),
                                                    vmName, srcURL, dstURL))
   log("%s: %s:    progress=%d total=%d length=%d\n" % \
       (time.asctime(time.localtime()), vmName, progress, total, length))
   log("%s: %s:    Lease State: %s\n" % \
       (time.asctime(time.localtime()), vmName, lease.state))

   if lease.state == vim.HttpNfcLease.State.error:
      raise lease.error

   dstHttpConn.getresponse()
   return progress


def uploadFiles(fileItems, lease, ovfURL, vmName=None, log=None):
   '''
   Upload witness vm's vmdk files to vCenter by using http protocol
   @param fileItems: the source vmdks read from ovf file
   @param lease: Represents a lease on a VirtualMachine or a VirtualApp, which can be used to import or export
   disks for the entity
   @param ovfURL: witness vApp ovf url
   @param vmName: The name of witness vm
   @param log:
   @return:
   '''
   uploadUrlMap = {}
   for kv in lease.info.deviceUrl:
      uploadUrlMap[kv.importKey] = (kv.key, kv.url)

   progress = 5
   increment = (int)(90 / len(fileItems))
   for file in fileItems:
      ovfDevId = file.deviceId
      srcDiskURL = urlparse.urljoin(ovfURL, file.path)
      (viDevId, url) = uploadUrlMap[ovfDevId]
      if lease.state == vim.HttpNfcLease.State.error:
         raise lease.error
      elif lease.state != vim.HttpNfcLease.State.ready:
         raise Exception("%s: file upload aborted, lease state=%s" % \
                         (vmName, lease.state))
      progress = uploadFile(srcDiskURL, url, file.create, lease, progress,
                            increment, vmName, log)


def DeployWitnessOVF(ovfURL, si, host, vmName, dsRef, vmFolder, vmPassword=None, network=None, log=None):
   '''
   Deploy witness VM to vCenter,  the import process consists of two steps:
   1>Create the VMs and/or vApps that make up the entity.
   2>Upload virtual disk contents.
   @param ovfURL: ovf source url
   @param si: Managed Object ServiceInstance
   @param host: HostSystem which the VM located
   @param vmName: VM name
   @param dsRef: Datastore which the VM located
   @param vmFolder: Folder which the VM belong to
   @param vmPassword: Password for the VM
   @param network: Managed Object Network of the VM
   @param log:
   @return:
   '''
   rp = host.parent.resourcePool
   params = vim.OvfManager.CreateImportSpecParams()
   params.entityName = vmName
   params.hostSystem = host
   params.diskProvisioning = 'thin'

   f = urllib.urlopen(ovfURL)
   ovfData = f.read()

   import xml.etree.ElementTree as ET

   params.networkMapping = []
   if vmPassword:
      params.propertyMapping = [vim.KeyValue(key='vsan.witness.root.passwd', value=vmPassword)]
   ovf_tree = ET.fromstring(ovfData)


   for nwt in ovf_tree.findall('NetworkSection/Network'):
      nm = vim.OvfManager.NetworkMapping()
      nm.name = nwt.attrib['name']
      if network != None:
         nm.network = network
      else:
         nm.network = host.parent.network[0]
      params.networkMapping.append(nm)

   res = si.content.ovfManager.CreateImportSpec(ovfDescriptor = ovfData,
                                                resourcePool = rp, datastore = dsRef, cisp = params)
   if isinstance(res, vim.MethodFault):
      raise res
   if res.error and len(res.error) > 0:
      raise res.error[0]
   if not res.importSpec:
      raise Exception("CreateImportSpec raised no errors, but importSpec is not set")

   lease = rp.ImportVApp(spec = res.importSpec, folder = vmFolder, host = host)
   while lease.state == vim.HttpNfcLease.State.initializing:
      time.sleep(1)

   if lease.state == vim.HttpNfcLease.State.error:
      raise lease.error

   # Upload files
   uploadFiles(res.fileItem, lease, ovfURL, vmName, log)
   lease.Complete()

   return lease.info.entity


def AddHost(host, user='root', pwd=None, dcRef=None, si=None, sslThumbprint=None, port=443):
   '''
   Add a host to a data center
   Returns a host system
   '''

   cnxSpec = vim.HostConnectSpec(
           force=True,
           hostName=host,
           port=port,
           userName=user,
           password=pwd,
           vmFolder=dcRef.vmFolder
   )

   if sslThumbprint:
      cnxSpec.sslThumbprint = sslThumbprint

   hostParent = dcRef.hostFolder

   try:
      task = hostParent.AddStandaloneHost(addConnected = True, spec = cnxSpec)
      vsanapiutils.WaitForTasks([task], si)
      return getHostSystem(host, dcRef, si)
   except vim.SSLVerifyFault as e:
      #By catching this exception, user doesn't need input the host's thumbprint of the SSL certificate, the logic below
      #will do this automaticlly
      cnxSpec.sslThumbprint = e.thumbprint
      task = hostParent.AddStandaloneHost(addConnected = True, spec = cnxSpec)
      vsanapiutils.WaitForTasks([task], si)
      return getHostSystem(host, dcRef, si)
   except vim.DuplicateName as e:
      raise Exception("AddHost: ESX host %s has already been added to VC." % host)


# Start program
def main():
   args = GetArgs()
   if args.password:
      password = args.password
   else:
      password = getpass.getpass(prompt='Enter password for host %s and '
                                        'user %s: ' % (args.host, args.user))

   # For python 2.7.9 and later, the defaul SSL conext has more strict
   # connection handshaking rule. We may need turn of the hostname checking
   # and client side cert verification
   context = None
   if sys.version_info[:3] > (2, 7, 8):
      context = ssl.create_default_context()
      context.check_hostname = False
      context.verify_mode = ssl.CERT_NONE

   si = SmartConnect(host=args.host,
                     user=args.user,
                     pwd=password,
                     port=int(args.port),
                     sslContext=context)

   atexit.register(Disconnect, si)

   cluster = getClusterInstance(args.clusterName, si)
   hostProps = CollectMultiple(si.content, cluster.host, ['name'])
   hosts = hostProps.keys()

   vcMos = vsanapiutils.GetVsanVcMos(si._stub, context = context)
   vsanScSystem = vcMos['vsan-stretched-cluster-system']
   searchIndex = si.content.searchIndex
   if args.ovfurl:
      print 'Start to add virtual witness host'
      '''
      Steps about add virtual witness,rather than a dedicate physical ESXi host to be a witness host,
      VMware has developed the VSAN witness appliance to take care of the witness requirements
      1) Deploy witness VM
         1.1)Specify the host for the witness VM
         1.2)Specify the storage for the witness VM
         1.3)Specify the network for the witness VM
      2) Get the witness VM and add it to the data center as a witness host
      '''
      dc = searchIndex.FindChild(entity = si.content.rootFolder, name = args.datacenter)
      #specify the host for the witness VM
      hostSystem = getHostSystem(args.vmhost, dc, si)
      #specify the storage for the witness VM
      ds = searchIndex.FindChild(entity = dc.datastoreFolder, name = args.datastore)
      #specify the network for the witness VM
      if args.network:
         network = [net for net in dc.networkFolder.childEntity
                        if net.name == args.network][0]
      else:
         network = dc.networkFolder.childEntity[0]

      witnessVm = DeployWitnessOVF(args.ovfurl, si, hostSystem, args.name, ds, dc.vmFolder, vmPassword=args.vmpassword,
                                   network=network)
      task = witnessVm.PowerOn()
      vsanapiutils.WaitForTasks([task], si)

      # Wait for vm to go up
      beginTime = time.time()
      while True:
         try:
            SmartConnect(host=witnessVm.guest.ipAddress,
                           user='root',
                           pwd=args.vmpassword,
                           port=443,
                           sslContext=context)
         except:
            time.sleep(10)
            timeWaiting = time.time() - beginTime
            if timeWaiting > (15 * 60):
               raise Exception("Timed out waiting (>15min) for VM to up!")
         else:
            break
      print 'Add witness host {} to datacenter {}'.format(witnessVm.name, args.witnessdc)
      dcRef = searchIndex.FindChild(entity = si.content.rootFolder, name = args.witnessdc)
      witnessHost = AddHost(witnessVm.guest.ipAddress, pwd=args.vmpassword, dcRef=dcRef, si=si)

   else:
      print 'Start to add physical witness host'
      dcRef = searchIndex.FindChild(entity = si.content.rootFolder, name = args.witnessdc)
      witnessHost = AddHost(args.witnesshost, user=args.witnesshostuser ,pwd=args.witnesshostpwd, dcRef=dcRef, si=si)
      disks = []

      for result in witnessHost.configManager.vsanSystem.QueryDisksForVsan():
         if result.state == 'ineligible' and type(result.error) == vim.DiskHasPartitions:
            disks.append(result.disk)

      for disk in disks:
         print 'Find ineligible disk {} in host {}'.format(disk.displayName, args.witnesshost)
         if yes('Do you want to wipe disk {}?\nPlease Always check the partition table and the data stored'
                 ' on those disks before doing any wipe! (yes/no)?'.format(disk.displayName)):
            witnessHost.configManager.storageSystem.UpdateDiskPartitions(disk.deviceName,
                                                                                vim.HostDiskPartitionSpec())


   disks = [result.disk for result in witnessHost.configManager.vsanSystem.QueryDisksForVsan() if
            result.state == 'eligible']
   diskMapping = None
   if disks:
      #witness host need at lease one disk group, For non-witness-OVF, please make sure there are enough
      #eligible disks. For witness host which created from witness ovf, there is no such worry.
      ssds = [disk for disk in disks if disk.ssd]
      nonSsds = [disk for disk in disks if not disk.ssd]
      #host with hybrid disks
      if len(ssds) > 0 and len(nonSsds) > 0:
         diskMapping = vim.VsanHostDiskMapping(
            ssd = ssds[0],
            nonSsd = nonSsds
         )
      #host with all-flash disks,choose the ssd with smaller capacity for cache layer.
      if len(ssds) > 0 and len(nonSsds) == 0:
         smallerSize = min([disk.capacity.block * disk.capacity.blockSize for disk in ssds])
         smallSsds = []
         biggerSsds = []
         for ssd in ssds:
            size = ssd.capacity.block * ssd.capacity.blockSize
            if size == smallerSize:
               smallSsds.append(ssd)
            else:
               biggerSsds.append(ssd)
         diskMapping = vim.VsanHostDiskMapping(
            ssd = smallSsds[0],
            nonSsd = biggerSsds
         )

   preferedFd = args.preferdomain
   secondaryFd = args.seconddomain

   firstFdHosts = []
   secondFdHosts = []
   for host in hosts:
      if yes('Add host {} to preferred fault domain ? (yes/no)'.format(hostProps[host]['name'])):
         firstFdHosts.append(host)
   for host in set(hosts) - set(firstFdHosts):
      if yes('Add host {} to second fault domain ? (yes/no)'.format(hostProps[host]['name'])):
         secondFdHosts.append(host)

   faultDomainConfig = vim.VimClusterVSANStretchedClusterFaultDomainConfig(
      firstFdHosts = firstFdHosts,
      firstFdName = preferedFd,
      secondFdHosts = secondFdHosts,
      secondFdName = secondaryFd
   )

   print 'start to create stretched cluster'
   task = vsanScSystem.VSANVcConvertToStretchedCluster(cluster=cluster, faultDomainConfig=faultDomainConfig,
                                                          witnessHost=witnessHost, preferredFd=preferedFd,
                                                          diskMapping=diskMapping)
   vsanapiutils.WaitForTasks([task], si)


# Start program
if __name__ == "__main__":
   main()

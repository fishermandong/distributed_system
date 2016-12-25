从Onenote笔记摘录的

HAproxy，DNS动态绑定，在很多服务上使用，这是因为它们重后台IO和处理，真正需要传输的量，往往不大。而且支持redirection。

但是存储不同，存储就是在做传输！ 如果不支持redirection，那么就只有动态的DNS 了。但是什么时候客户端会重新连接呢？  如果能够支持redirection，那么就好很多. (referral能行么？)

或者我们有个交换机，搞定这个事情！ 一台proxy机器可能无法胜任带宽，但是交换机可以。
   ==>这个有变成硬件方案了，不适合做云化。
       ==>网络虚拟化层呢？ 不是做vxlan么？能否改改？

另外，既然都是虚拟化的，能否让qemu所在物理机运行nfs server?  反正经过本机。 云化思维！ 或者运行在虚拟机的一个容器上？ 全是虚拟化网络，这样还能减少网络传输！ 反正都是容器，好迁移。


在bing上搜索： nfsv4 +redirection，有挺多结果。

这里讲了比较有用的东西：

Upon receiving the replica locations, the client selects a nearby replication server, mounts it at the place of reference, and continues its access. When a directory is migrated to another server, the old server returns NFS4ERR MOVED error for subsequent directory requests

来自 <http://www.nexoncn.com/read/07cac7ce2ab500aac42a31e8.html> 

这里也提到了replication，还有
SERVER REDIRECT


The FS_LOCATIONS attribute allows clients to find
migrated/replicated data locations dynamically at the
time of reference

来自 <http://www.citi.umich.edu/projects/nfsv4/reports/replication.pdf> 


Nache: Design and Implementation of a Caching Proxy for NFSv4

来自 <https://www.researchgate.net/publication/221353739_Nache_Design_and_Implementation_of_a_Caching_Proxy_for_NFSv4> 


Implementing Oracle11g Database over NFSv4 from a Shared Backend Storage

来自 <http://www.snia.org/sites/default/orig/sdc_archives/2008_presentations/wednesday/Bikash_R_Choudhury_oracle_nfsv4.pdf> 

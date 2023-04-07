sudo tc qdisc del dev enp1s0d1 root; 
sudo tc qdisc add dev enp1s0d1 root handle 1: htb; 
sudo tc class add dev enp1s0d1 parent 1: classid 1:1 htb rate 1gibps; 
sudo tc class add dev enp1s0d1 parent 1:1 classid 1:2 htb rate 1gibps; 
sudo tc qdisc add dev enp1s0d1 handle 2: parent 1:2 netem delay $1ms; 
sudo tc filter add dev enp1s0d1 pref 2 protocol ip u32 match ip dst 0.0.0.0/0 flowid 1:2;

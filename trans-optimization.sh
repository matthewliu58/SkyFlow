#!/bin/bash


if sudo dmidecode -s system-manufacturer 2>/dev/null | grep -qi "vultr"; then
    echo "==> Vultr machine detected, disabling UFW firewall..."
    sudo ufw disable
    sudo systemctl stop ufw
    sudo systemctl disable ufw
else
    echo "==> Not Vultr machine, skip UFW disable"
fi

#==============================
# Debian 12 cross-ocean large-file BBR optimization
# For GCP / AWS JP <-> US high-bandwidth transfer
#==============================

# 1. Write sysctl kernel tuning (BBR + TCP + buffers + queue)
cat > /etc/sysctl.d/rigel-bbr-tuning.conf <<EOF
# BBR congestion control
net.core.default_qdisc=fq
net.ipv4.tcp_congestion_control=bbr

# File handles
fs.file-max=1048576
fs.nr_open=1048576

# TCP buffer max
net.core.rmem_max=67108864
net.core.wmem_max=67108864
net.core.rmem_default=67108864
net.core.wmem_default=67108864

# Auto-tune buffers
net.ipv4.tcp_rmem=4096 87380 67108864
net.ipv4.tcp_wmem=4096 87380 67108864

# Port range
net.ipv4.ip_local_port_range=1024 65535

# TIME-WAIT reuse
net.ipv4.tcp_tw_reuse=1

# SYN defense (must enable)
net.ipv4.tcp_syncookies=1

# Connection backlog increase
net.core.somaxconn=4096
net.core.netdev_max_backlog=16384
net.ipv4.tcp_max_syn_backlog=8192

# Long connection: no slow start after idle
net.ipv4.tcp_slow_start_after_idle=0

# Required for long path
net.ipv4.tcp_window_scaling=1
net.ipv4.tcp_timestamps=1
net.ipv4.tcp_sack=1
EOF

# 2. Apply sysctl immediately
sysctl --system

# 3. Write file handle limits
cat >> /etc/security/limits.conf <<EOF
* soft nofile 1048576
* hard nofile 1048576
root soft nofile 1048576
root hard nofile 1048576
EOF

# 4. Apply ulimit to current session
ulimit -n 1048576

# 5. Print results
echo "=================================="
echo " BBR + TCP optimization applied!"
echo "=================================="
echo "Congestion control: $(sysctl -n net.ipv4.tcp_congestion_control)"
echo "Queue discipline: $(sysctl -n net.core.default_qdisc)"
echo "File handles: $(ulimit -n)"
echo "=================================="
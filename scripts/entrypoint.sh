#!/bin/sh
set -e

WG_DIR=/etc/wireguard
WG_IFACE=wg0

# Configurable via env — set defaults
: "${WG_ADDRESS:=10.200.0.1/16}"
: "${WG_EGRESS_IFACE:=eth0}"
: "${WG_LISTEN_PORT:=51820}"
: "${WG_NETWORK:=10.200.0.0/16}"
: "${POD_CIDR:=10.42.0.0/16}"
: "${SVC_CIDR:=10.43.0.0/16}"


mkdir -p $WG_DIR

if [ ! -f $WG_DIR/wg0.key ]; then
    wg genkey | tee $WG_DIR/wg0.key | wg pubkey > $WG_DIR/wg0.pub
fi


# Create wg0.conf dynamically
cat > $WG_DIR/$WG_IFACE.conf <<EOF
[Interface]
Address = ${WG_ADDRESS}
ListenPort = ${WG_LISTEN_PORT}
PrivateKey = $(cat $WG_DIR/wg0.key)

PostUp = sysctl -w net.ipv4.ip_forward=1

# Enable forwarding and permit transit between wg0 and the primary uplink
PostUp = iptables -A FORWARD -i %i -j ACCEPT
PostUp = iptables -A FORWARD -o %i -j ACCEPT
PostUp = iptables -t nat -A POSTROUTING -s ${WG_NETWORK} -o ${WG_EGRESS_IFACE} -j MASQUERADE
PostUp = iptables -t nat -A POSTROUTING -s ${POD_CIDR} -o ${WG_EGRESS_IFACE} -j MASQUERADE
PostUp = iptables -t nat -A POSTROUTING -s ${SVC_CIDR} -o ${WG_EGRESS_IFACE} -j MASQUERADE

PreDown = iptables -D FORWARD -i %i -j ACCEPT
PreDown = iptables -D FORWARD -o %i -j ACCEPT
PreDown = iptables -t nat -D POSTROUTING -s ${WG_NETWORK} -o ${WG_EGRESS_IFACE} -j MASQUERADE
PreDown = iptables -t nat -D POSTROUTING -s ${POD_CIDR} -o ${WG_EGRESS_IFACE} -j MASQUERADE
PreDown = iptables -t nat -D POSTROUTING -s ${SVC_CIDR} -o ${WG_EGRESS_IFACE} -j MASQUERADE
EOF

sysctl -w net.ipv4.ip_forward=1

if ip link show $WG_IFACE > /dev/null 2>&1; then
    wg-quick down $WG_DIR/$WG_IFACE.conf || ip link delete $WG_IFACE
fi

wg-quick up $WG_DIR/$WG_IFACE.conf

exec verda-cloud-provider "$@"

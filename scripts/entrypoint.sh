#!/bin/sh
set -e

WG_DIR=/etc/wireguard
WG_IFACE=wg0

mkdir -p $WG_DIR

# TODO: could be done in provider wg service
# Generate keys if missing
if [ ! -f $WG_DIR/wg0.key ]; then
    wg genkey | tee $WG_DIR/wg0.key | wg pubkey > $WG_DIR/wg0.pub
fi


# Create wg0.conf dynamically
cat > $WG_DIR/$WG_IFACE.conf <<EOF
[Interface]
Address = 10.200.0.1/16
ListenPort = 51820
PrivateKey = $(cat $WG_DIR/wg0.key)

PostUp = sysctl -w net.ipv4.ip_forward=1

# Enable forwarding and permit transit between wg0 and the primary uplink
PostUp = iptables -A FORWARD -i %i -j ACCEPT
PostUp = iptables -A FORWARD -o %i -j ACCEPT

# NAT only for public internet egress from cloud-side traffic
PostUp = iptables -t nat -A POSTROUTING -s 10.244.0.0/16 -o eth0 -j MASQUERADE

PreDown = iptables -D FORWARD -i %i -j ACCEPT
PreDown = iptables -D FORWARD -o %i -j ACCEPT
PreDown = iptables -t nat -D POSTROUTING -s 10.244.0.0/16 -o eth0 -j MASQUERADE

EOF

sysctl -w net.ipv4.ip_forward=1

# Bring down existing interface if present
if ip link show $WG_IFACE > /dev/null 2>&1; then
    wg-quick down $WG_DIR/$WG_IFACE.conf || ip link delete $WG_IFACE
fi

# Bring up interface
wg-quick up $WG_DIR/$WG_IFACE.conf

# Start app
exec verda-cloud-provider "$@"

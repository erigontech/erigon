#!/bin/bash

echo "🔧 Setting up Nginx Reverse Proxy for Olym3 Testnet Season 3"
echo "============================================================="

# Install Nginx
echo "📦 Installing Nginx..."
apt update
apt install -y nginx

# Create Nginx configuration
echo "⚙️  Creating Nginx configuration..."
cat > /etc/nginx/sites-available/olym3-rpc << 'EOF'
server {
    listen 80;
    server_name rpc3.olym3.xyz;

    # Redirect HTTP to HTTPS
    return 301 https://$server_name$request_uri;
}

server {
    listen 443 ssl http2;
    server_name rpc3.olym3.xyz;

    # SSL Configuration (you'll need to add SSL certificates)
    # ssl_certificate /path/to/your/certificate.crt;
    # ssl_certificate_key /path/to/your/private.key;

    # For now, we'll use HTTP (remove this when SSL is configured)
    listen 80;
    
    location / {
        proxy_pass http://127.0.0.1:8545;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        
        # CORS headers for web applications
        add_header 'Access-Control-Allow-Origin' '*' always;
        add_header 'Access-Control-Allow-Methods' 'GET, POST, OPTIONS' always;
        add_header 'Access-Control-Allow-Headers' 'DNT,User-Agent,X-Requested-With,If-Modified-Since,Cache-Control,Content-Type,Range' always;
        
        # Handle preflight requests
        if ($request_method = 'OPTIONS') {
            add_header 'Access-Control-Allow-Origin' '*';
            add_header 'Access-Control-Allow-Methods' 'GET, POST, OPTIONS';
            add_header 'Access-Control-Allow-Headers' 'DNT,User-Agent,X-Requested-With,If-Modified-Since,Cache-Control,Content-Type,Range';
            add_header 'Access-Control-Max-Age' 1728000;
            add_header 'Content-Type' 'text/plain; charset=utf-8';
            add_header 'Content-Length' 0;
            return 204;
        }
    }
}
EOF

# Enable the site
echo "🔗 Enabling Nginx site..."
ln -sf /etc/nginx/sites-available/olym3-rpc /etc/nginx/sites-enabled/
rm -f /etc/nginx/sites-enabled/default

# Test Nginx configuration
echo "🧪 Testing Nginx configuration..."
nginx -t

if [ $? -eq 0 ]; then
    echo "✅ Nginx configuration is valid"
    
    # Restart Nginx
    echo "🔄 Restarting Nginx..."
    systemctl restart nginx
    systemctl enable nginx
    
    echo "✅ Nginx is now running and configured"
    echo ""
    echo "📋 Next steps:"
    echo "1. Configure DNS: rpc3.olym3.xyz -> 34.123.99.88"
    echo "2. Setup SSL certificate (Let's Encrypt recommended)"
    echo "3. Test RPC endpoint: https://rpc3.olym3.xyz"
    echo ""
    echo "🔧 For SSL certificate:"
    echo "   apt install certbot python3-certbot-nginx"
    echo "   certbot --nginx -d rpc3.olym3.xyz"
else
    echo "❌ Nginx configuration has errors"
    echo "Please check the configuration file"
fi

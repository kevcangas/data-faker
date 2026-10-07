export const PRESETS = [
  {
    id: 'ecommerce',
    name: '🛒 E-Commerce Orders',
    topic: 'ecommerce-orders',
    keyStrategy: { type: 'extract_field', value: 'order_id' },
    rate: 100,
    headers: [
      { key: 'event-type', value: 'order.placed' },
      { key: 'schema-version', value: 'v1.2' }
    ],
    template: `{
  "order_id": "{{uuid}}",
  "customer_id": "CUST-{{random_int(1000, 9999)}}",
  "customer_name": "{{name}}",
  "email": "{{email}}",
  "currency": "{{choose(['USD', 'EUR', 'GBP', 'CAD'])}}",
  "total_amount": {{random_float(12.50, 899.99, 2)}},
  "items_count": {{random_int(1, 6)}},
  "status": "{{choose(['PENDING', 'PAID', 'PROCESSING', 'SHIPPED'])}}",
  "payment_method": "{{choose(['CREDIT_CARD', 'PAYPAL', 'APPLE_PAY', 'CRYPTO'])}}",
  "shipping_country": "{{country_code}}",
  "ip_address": "{{ipv4}}",
  "timestamp": {{timestamp}},
  "created_at": "{{timestamp_iso}}"
}`
  },
  {
    id: 'iot',
    name: '📡 IoT Sensor Telemetry',
    topic: 'iot-sensor-telemetry',
    keyStrategy: { type: 'extract_field', value: 'device_id' },
    rate: 250,
    headers: [
      { key: 'sensor-protocol', value: 'mqtt-bridge' },
      { key: 'firmware', value: '3.4.1-rc' }
    ],
    template: `{
  "device_id": "SNSR-{{country_code}}-{{random_int(100, 999)}}",
  "facility": "HUB-{{city}}",
  "temperature_celsius": {{random_float(18.0, 36.5, 2)}},
  "humidity_percent": {{random_float(30.0, 85.0, 1)}},
  "pressure_hpa": {{random_float(980.0, 1025.0, 1)}},
  "battery_voltage": {{random_float(3.2, 4.2, 2)}},
  "signal_rssi_dbm": {{random_int(-95, -45)}},
  "status": "{{choose(['NORMAL', 'WARNING', 'OPTIMAL'])}}",
  "alert": {{boolean}},
  "sequence": {{sequence}},
  "timestamp": {{timestamp}}
}`
  },
  {
    id: 'finance',
    name: '💳 Financial Transactions',
    topic: 'financial-transactions',
    keyStrategy: { type: 'extract_field', value: 'transaction_id' },
    rate: 50,
    headers: [
      { key: 'clearing-network', value: 'SWIFT-ISO20022' },
      { key: 'risk-flag', value: 'automated-scoring' }
    ],
    template: `{
  "transaction_id": "TXN-{{uuid}}",
  "card_number": "{{credit_card}}",
  "holder_name": "{{name}}",
  "merchant": "{{choose(['Amazon Cloud', 'Uber Mobility', 'Starbucks Coffee', 'Apple Store', 'Netflix Media'])}}",
  "amount": {{random_float(2.50, 4500.00, 2)}},
  "currency": "{{choose(['USD', 'EUR', 'JPY', 'CHF'])}}",
  "risk_score": {{random_float(0.01, 0.99, 3)}},
  "auth_code": "AUTH-{{random_int(100000, 999999)}}",
  "is_flagged_fraud": {{boolean}},
  "origin_country": "{{country_code}}",
  "timestamp": {{timestamp}}
}`
  },
  {
    id: 'clickstream',
    name: '🖱️ User Clickstream',
    topic: 'web-clickstream',
    keyStrategy: { type: 'random_uuid' },
    rate: 500,
    headers: [
      { key: 'source', value: 'web-sdk' }
    ],
    template: `{
  "session_id": "{{uuid}}",
  "user_id": "USR-{{random_int(10000, 99999)}}",
  "page_url": "{{choose(['https://store.io/home', 'https://store.io/cart', 'https://store.io/checkout', 'https://store.io/search', 'https://store.io/products/detail'])}}",
  "action": "{{choose(['page_view', 'click_cta', 'scroll_depth', 'add_to_cart', 'filter_applied'])}}",
  "referrer": "{{choose(['google.com', 'direct', 'twitter.com', 'linkedin.com', 'email_campaign'])}}",
  "user_agent": "{{choose(['Mozilla/5.0 (Windows NT 10.0; Win64)', 'Mozilla/5.0 (Macintosh; Intel Mac OS X)', 'Mozilla/5.0 (iPhone; CPU iPhone OS)'])}}",
  "ip_address": "{{ipv4}}",
  "dwell_time_ms": {{random_int(120, 18000)}},
  "timestamp": {{timestamp}}
}`
  }
];

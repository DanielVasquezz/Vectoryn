// config.js — Configurable URLs for cloud deployment
// Local: GATEWAY_URL = http://localhost:8080
// On Render: GATEWAY_URL = https://vectoryn-gateway.onrender.com
//Fixed

window.VECTORYN_CONFIG = {
  GATEWAY_URL: window.location.hostname === 'localhost' || window.location.hostname === '127.0.0.1'
    ? 'http://localhost:8080'
    : 'https://vectoryn-gateway.onrender.com',
  API_KEY: '1e46805fb3cda729',
};

'use strict';

const express = require('express');
const axios = require('axios');
const AdmZip = require('adm-zip');
const os = require('os');
const fs = require('fs');
const path = require('path');
const { pipeline } = require('stream/promises');
const { spawn } = require('child_process');
const net = require('net');
const { createProxyMiddleware } = require('http-proxy-middleware');

// -----------------------------------------------------------------------------
// Environment
// -----------------------------------------------------------------------------
const FILE_PATH = path.resolve(process.env.FILE_PATH || '.tmp');
const SUB_PATH = (process.env.SUB_PATH || 'sub').replace(/^\/+|\/+$/g, '') || 'sub';
const DOMAIN = process.env.SERVER_DOMAIN || process.env.DOMAIN || 'example.com';
const PORT = Number(process.env.SERVER_PORT || process.env.PORT || 3000);
const UUID = process.env.UUID || '9afd1229-b893-40c1-84dd-51e7ce204913';

const NEZHA_SERVER = process.env.NEZHA_SERVER || '';
const NEZHA_PORT = process.env.NEZHA_PORT || '';
const NEZHA_KEY = process.env.NEZHA_KEY || '';

const ARGO_DOMAIN = process.env.ARGO_DOMAIN || '';
const ARGO_AUTH = process.env.ARGO_AUTH || '';
const ARGO_PORT = Number(process.env.ARGO_PORT || 8001);

const CFIP = process.env.CFIP || 'saas.sin.fan';
const CFPORT = Number(process.env.CFPORT || 443);
const NAME = process.env.NAME || '';

const STARTUP_TIMEOUT_MS = Number(process.env.STARTUP_TIMEOUT_MS || 30000);
const DOWNLOAD_TIMEOUT_MS = Number(process.env.DOWNLOAD_TIMEOUT_MS || 120000);

// -----------------------------------------------------------------------------
// Paths and process state
// -----------------------------------------------------------------------------
fs.mkdirSync(FILE_PATH, { recursive: true });

function randomName(length = 8) {
  const chars = 'abcdefghijklmnopqrstuvwxyz';
  let value = '';
  for (let i = 0; i < length; i += 1) {
    value += chars[Math.floor(Math.random() * chars.length)];
  }
  return value;
}

const webPath = path.join(FILE_PATH, randomName());
const botPath = path.join(FILE_PATH, randomName());
const npmPath = path.join(FILE_PATH, randomName());
const configPath = path.join(FILE_PATH, 'config.json');
const subPath = path.join(FILE_PATH, 'sub.txt');
const bootLogPath = path.join(FILE_PATH, 'boot.log');
const xrayLogPath = path.join(FILE_PATH, 'xray.log');
const nezhaLogPath = path.join(FILE_PATH, 'nezha-agent.log');
const cloudflaredLogPath = path.join(FILE_PATH, 'cloudflared.log');
const tunnelJsonPath = path.join(FILE_PATH, 'tunnel.json');
const tunnelYamlPath = path.join(FILE_PATH, 'tunnel.yml');

const childProcesses = new Set();
let shuttingDown = false;

// -----------------------------------------------------------------------------
// Small utilities
// -----------------------------------------------------------------------------
function log(...args) {
  console.log(new Date().toISOString(), ...args);
}

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function assertConfig() {
  if (!Number.isInteger(PORT) || PORT < 1 || PORT > 65535) {
    throw new Error(`Invalid PORT: ${process.env.PORT || PORT}`);
  }
  if (!Number.isInteger(ARGO_PORT) || ARGO_PORT < 1 || ARGO_PORT > 65535) {
    throw new Error(`Invalid ARGO_PORT: ${process.env.ARGO_PORT || ARGO_PORT}`);
  }
  if (!Number.isInteger(CFPORT) || CFPORT < 1 || CFPORT > 65535) {
    throw new Error(`Invalid CFPORT: ${process.env.CFPORT || CFPORT}`);
  }
  if (PORT === ARGO_PORT) {
    throw new Error('PORT and ARGO_PORT must be different: Express and Xray use separate ports.');
  }
}

function getArchitecture() {
  const arch = os.arch();
  if (arch === 'arm64' || arch === 'aarch64') return 'arm64';
  if (arch === 'arm') return 'arm';
  if (arch === 'x64') return 'amd64';
  throw new Error(`Unsupported CPU architecture: ${arch}`);
}

function getDownloadList() {
  const arch = getArchitecture();
  const list = [
    {
      name: 'xray',
      destination: webPath,
      url: arch === 'arm64'
        ? 'https://github.com/XTLS/Xray-core/releases/latest/download/Xray-linux-arm64-v8a.zip'
        : arch === 'arm'
          ? 'https://github.com/XTLS/Xray-core/releases/latest/download/Xray-linux-arm32-v7a.zip'
          : 'https://github.com/XTLS/Xray-core/releases/latest/download/Xray-linux-64.zip',
      candidates: ['xray', 'Xray']
    },
    {
      name: 'cloudflared',
      destination: botPath,
      url: arch === 'arm64'
        ? 'https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-arm64'
        : arch === 'arm'
          ? 'https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-arm'
          : 'https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-amd64',
      candidates: []
    }
  ];

  if (NEZHA_SERVER && NEZHA_PORT && NEZHA_KEY) {
    list.push({
      name: 'nezha-agent',
      destination: npmPath,
      url: arch === 'arm64'
        ? 'https://github.com/nezhahq/agent/releases/download/v0.20.5/nezha-agent_linux_arm64.zip'
        : arch === 'arm'
          ? 'https://github.com/nezhahq/agent/releases/download/v0.20.5/nezha-agent_linux_arm.zip'
          : 'https://github.com/nezhahq/agent/releases/download/v0.20.5/nezha-agent_linux_amd64.zip',
      candidates: ['nezha-agent']
    });
  }

  return list;
}

// Download to a temporary file. ZIP archives are inspected and only the required
// executable is extracted, so archive directory layouts do not affect the result.
async function downloadBinary(item) {
  const tempPath = `${item.destination}.download`;
  log(`Downloading ${item.name}: ${item.url}`);

  try {
    const response = await axios.get(item.url, {
      responseType: 'stream',
      timeout: DOWNLOAD_TIMEOUT_MS,
      maxRedirects: 10,
      headers: { 'User-Agent': 'nodejs-argo/1.0' },
      validateStatus: status => status >= 200 && status < 300
    });

    await pipeline(response.data, fs.createWriteStream(tempPath));

    if (/\.zip(?:$|\?)/i.test(item.url)) {
      const zip = new AdmZip(tempPath);
      const entries = zip.getEntries();
      const wanted = item.candidates.map(name => name.toLowerCase());

      const entry = entries.find(candidate =>
        !candidate.isDirectory &&
        wanted.includes(path.posix.basename(candidate.entryName).toLowerCase())
      );

      if (!entry) {
        const names = entries
          .filter(candidate => !candidate.isDirectory)
          .map(candidate => candidate.entryName)
          .slice(0, 30);
        throw new Error(
          `ZIP 中找不到 ${item.candidates.join('/')}。压缩包文件列表: ${names.join(', ')}`
        );
      }

      const data = entry.getData();
      if (!data || data.length === 0) {
        throw new Error(`ZIP entry is empty: ${entry.entryName}`);
      }
      fs.writeFileSync(item.destination, data, { mode: 0o755 });
      log(`${item.name} extracted: ${entry.entryName}`);
    } else {
      fs.renameSync(tempPath, item.destination);
      fs.chmodSync(item.destination, 0o755);
    }

    fs.chmodSync(item.destination, 0o755);
    log(`${item.name} ready: ${item.destination}`);
    return item.destination;
  } catch (error) {
    try { fs.rmSync(tempPath, { force: true }); } catch (_) {}
    try { fs.rmSync(item.destination, { force: true }); } catch (_) {}
    throw new Error(`${item.name} download/extract failed: ${error.message}`);
  } finally {
    try { fs.rmSync(tempPath, { force: true }); } catch (_) {}
  }
}

async function downloadAll() {
  const files = getDownloadList();
  // Keep downloads concurrent; each writes to a different destination.
  await Promise.all(files.map(downloadBinary));
}

// -----------------------------------------------------------------------------
// Xray configuration
// -----------------------------------------------------------------------------
function generateConfig() {
  const config = {
    log: {
      access: 'none',
      error: xrayLogPath,
      loglevel: 'warning'
    },
    inbounds: [
      {
        listen: '0.0.0.0',
        port: ARGO_PORT,
        protocol: 'vless',
        settings: {
          clients: [{ id: UUID, flow: 'xtls-rprx-vision' }],
          decryption: 'none',
          fallbacks: [
            { dest: 3001 },
            { path: '/vless-argo', dest: 3002 },
            { path: '/vmess-argo', dest: 3003 },
            { path: '/trojan-argo', dest: 3004 }
          ]
        },
        streamSettings: { network: 'tcp' }
      },
      {
        listen: '127.0.0.1',
        port: 3001,
        protocol: 'vless',
        settings: { clients: [{ id: UUID }], decryption: 'none' },
        streamSettings: { network: 'tcp', security: 'none' }
      },
      {
        listen: '127.0.0.1',
        port: 3002,
        protocol: 'vless',
        settings: {
          clients: [{ id: UUID, level: 0 }],
          decryption: 'none'
        },
        streamSettings: {
          network: 'ws',
          security: 'none',
          wsSettings: { path: '/vless-argo' }
        },
        sniffing: {
          enabled: true,
          destOverride: ['http', 'tls', 'quic'],
          metadataOnly: false
        }
      },
      {
        listen: '127.0.0.1',
        port: 3003,
        protocol: 'vmess',
        settings: { clients: [{ id: UUID, alterId: 0 }] },
        streamSettings: {
          network: 'ws',
          security: 'none',
          wsSettings: { path: '/vmess-argo' }
        },
        sniffing: {
          enabled: true,
          destOverride: ['http', 'tls', 'quic'],
          metadataOnly: false
        }
      },
      {
        listen: '127.0.0.1',
        port: 3004,
        protocol: 'trojan',
        settings: { clients: [{ password: UUID }] },
        streamSettings: {
          network: 'ws',
          security: 'none',
          wsSettings: { path: '/trojan-argo' }
        },
        sniffing: {
          enabled: true,
          destOverride: ['http', 'tls', 'quic'],
          metadataOnly: false
        }
      }
    ],
    dns: { servers: ['https+local://8.8.8.8/dns-query'] },
    outbounds: [
      { protocol: 'freedom', tag: 'direct' },
      { protocol: 'blackhole', tag: 'block' }
    ]
  };

  fs.writeFileSync(configPath, JSON.stringify(config, null, 2));
}

// -----------------------------------------------------------------------------
// Child process management
// -----------------------------------------------------------------------------
function startDetached(command, args, logPath, label) {
  const output = fs.openSync(logPath, 'a');
  let child;

  try {
    child = spawn(command, args, {
      detached: true,
      stdio: ['ignore', output, output],
      env: process.env
    });
  } finally {
    fs.closeSync(output);
  }

  child.once('error', error => {
    log(`${label} spawn error: ${error.message}`);
  });

  child.once('exit', (code, signal) => {
    childProcesses.delete(child.pid);
    if (!shuttingDown) {
      log(`${label} exited: code=${code}, signal=${signal || 'none'}`);
    }
  });

  childProcesses.add(child.pid);
  child.unref();
  log(`${label} process started, pid=${child.pid}`);
  return child.pid;
}

function isPortOpen(port, host = '127.0.0.1') {
  return new Promise(resolve => {
    const socket = net.createConnection({ port, host });
    const timer = setTimeout(() => {
      socket.destroy();
      resolve(false);
    }, 800);

    socket.once('connect', () => {
      clearTimeout(timer);
      socket.destroy();
      resolve(true);
    });
    socket.once('error', () => {
      clearTimeout(timer);
      resolve(false);
    });
  });
}

async function waitForPort(port, timeoutMs) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await isPortOpen(port)) return;
    await sleep(500);
  }
  throw new Error(`Port ${port} did not become ready within ${timeoutMs}ms`);
}

async function startXray() {
  if (!fs.existsSync(webPath)) {
    throw new Error(`Xray executable not found: ${webPath}`);
  }

  // Xray supports this validation command in current releases.
  const { execFile } = require('child_process');
  await new Promise((resolve, reject) => {
    execFile(webPath, ['run', '-test', '-config', configPath], {
      timeout: 15000
    }, (error, stdout, stderr) => {
      if (error) {
        reject(new Error(`Xray config test failed: ${(stderr || error.message).trim()}`));
      } else {
        if (stdout) log(stdout.trim());
        resolve();
      }
    });
  });

  startDetached(webPath, ['run', '-config', configPath], xrayLogPath, 'Xray');
  await waitForPort(ARGO_PORT, STARTUP_TIMEOUT_MS);
  log(`Xray is listening on 127.0.0.1:${ARGO_PORT}`);
}

async function startNezha() {
  if (!(NEZHA_SERVER && NEZHA_PORT && NEZHA_KEY)) {
    log('Nezha settings incomplete; skip Nezha Agent');
    return;
  }
  if (!fs.existsSync(npmPath)) {
    throw new Error(`Nezha executable not found: ${npmPath}`);
  }

  const args = [
    '-s', `${NEZHA_SERVER}:${NEZHA_PORT}`,
    '-p', NEZHA_KEY,
    '--disable-auto-update',
    '--report-delay', '4',
    '--skip-conn',
    '--skip-procs'
  ];

  if (['443', '8443', '2096', '2087', '2083', '2053'].includes(String(NEZHA_PORT))) {
    args.push('--tls');
  }

  startDetached(npmPath, args, nezhaLogPath, 'Nezha Agent');
}

// -----------------------------------------------------------------------------
// Cloudflared tunnel
// -----------------------------------------------------------------------------
function yamlString(value) {
  return JSON.stringify(String(value));
}

function parseTunnelCredentials(auth) {
  try {
    const parsed = JSON.parse(auth);
    if (parsed && parsed.TunnelID && parsed.TunnelSecret) return parsed;
  } catch (_) {
    // Not JSON credentials; token handling is checked below.
  }
  return null;
}

function configureCloudflared() {
  const credentials = parseTunnelCredentials(ARGO_AUTH);

  if (credentials) {
    fs.writeFileSync(tunnelJsonPath, JSON.stringify(credentials, null, 2), { mode: 0o600 });

    const yaml = [
      `tunnel: ${yamlString(credentials.TunnelID)}`,
      `credentials-file: ${yamlString(tunnelJsonPath)}`,
      'protocol: http2',
      'ingress:',
      `  - hostname: ${yamlString(ARGO_DOMAIN)}`,
      `    service: ${yamlString(`http://127.0.0.1:${ARGO_PORT}`)}`,
      '    originRequest:',
      '      noTLSVerify: true',
      '  - service: http_status:404',
      ''
    ].join('\n');

    fs.writeFileSync(tunnelYamlPath, yaml);
    return { type: 'credentials', args: ['tunnel', '--edge-ip-version', 'auto', '--config', tunnelYamlPath, 'run'] };
  }

  if (ARGO_AUTH && ARGO_DOMAIN) {
    return {
      type: 'token',
      args: [
        'tunnel', '--edge-ip-version', 'auto', '--no-autoupdate',
        '--protocol', 'http2', 'run', '--token', ARGO_AUTH
      ]
    };
  }

  return {
    type: 'quick',
    args: [
      'tunnel', '--edge-ip-version', 'auto', '--no-autoupdate',
      '--protocol', 'http2',
      '--logfile', bootLogPath,
      '--loglevel', 'info',
      '--url', `http://127.0.0.1:${ARGO_PORT}`
    ]
  };
}

async function startCloudflared() {
  if (!fs.existsSync(botPath)) {
    throw new Error(`cloudflared executable not found: ${botPath}`);
  }

  const tunnel = configureCloudflared();
  startDetached(botPath, tunnel.args, cloudflaredLogPath, 'Cloudflared');
  await sleep(1500);

  if (tunnel.type === 'quick') {
    return await waitForQuickTunnelDomain();
  }

  if (!ARGO_DOMAIN) {
    throw new Error('ARGO_DOMAIN is required for a named Cloudflare tunnel');
  }
  return ARGO_DOMAIN;
}

async function waitForQuickTunnelDomain() {
  const deadline = Date.now() + 60000;
  const pattern = /https:\/\/([a-z0-9-]+\.trycloudflare\.com)/i;

  while (Date.now() < deadline) {
    for (const logFile of [bootLogPath, cloudflaredLogPath]) {
      try {
        const content = fs.readFileSync(logFile, 'utf8');
        const match = content.match(pattern);
        if (match) {
          log(`Quick Tunnel domain: ${match[1]}`);
          return match[1];
        }
      } catch (_) {
        // Log file may not exist yet.
      }
    }
    await sleep(1000);
  }

  throw new Error(
    `Timed out waiting for trycloudflare.com domain. Check ${cloudflaredLogPath} and ${bootLogPath}`
  );
}

// -----------------------------------------------------------------------------
// Subscription generation
// -----------------------------------------------------------------------------
async function getMetaInfo() {
  try {
    const response = await axios.get('https://api.ip.sb/geoip', {
      timeout: 5000,
      headers: { 'User-Agent': 'Mozilla/5.0' }
    });
    if (response.data && response.data.country_code && response.data.isp) {
      return `${response.data.country_code}-${response.data.isp}`.replace(/\s+/g, '_');
    }
  } catch (error) {
    log(`Primary ISP lookup failed: ${error.message}`);
  }

  try {
    const response = await axios.get('http://ip-api.com/json', {
      timeout: 5000,
      headers: { 'User-Agent': 'Mozilla/5.0' }
    });
    if (response.data && response.data.status === 'success') {
      return `${response.data.countryCode}-${response.data.org || response.data.isp || 'Unknown'}`
        .replace(/\s+/g, '_');
    }
  } catch (error) {
    log(`Backup ISP lookup failed: ${error.message}`);
  }

  return 'Unknown';
}

async function generateLinks(argoDomain) {
  const isp = await getMetaInfo();
  const nodeName = NAME ? `${NAME}-${isp}` : isp;

  const vmess = {
    v: '2',
    ps: `vmess-${nodeName}`,
    add: CFIP,
    port: CFPORT,
    id: UUID,
    aid: '0',
    scy: 'auto',
    net: 'ws',
    type: 'none',
    host: argoDomain,
    path: '/vmess-argo?ed=2560',
    tls: 'tls',
    sni: argoDomain,
    alpn: '',
    fp: 'firefox'
  };

  const lines = [
    `vless://${UUID}@${DOMAIN}:${PORT}?encryption=none&security=none&type=ws&host=${encodeURIComponent(DOMAIN)}&path=%2Fvless-argo%3Fed%3D2560#${encodeURIComponent(`${DOMAIN}-${isp}`)}`,
    `vless://${UUID}@${CFIP}:${CFPORT}?encryption=none&security=tls&sni=${encodeURIComponent(argoDomain)}&fp=firefox&type=ws&host=${encodeURIComponent(argoDomain)}&path=%2Fvless-argo%3Fed%3D2560#${encodeURIComponent(`vless-${nodeName}`)}`,
    `vmess://${Buffer.from(JSON.stringify(vmess)).toString('base64')}`,
    `trojan://${UUID}@${CFIP}:${CFPORT}?security=tls&sni=${encodeURIComponent(argoDomain)}&fp=firefox&type=ws&host=${encodeURIComponent(argoDomain)}&path=%2Ftrojan-argo%3Fed%3D2560#${encodeURIComponent(`trojan-${nodeName}`)}`
  ];

  const subscription = lines.join('\n') + '\n';
  const encoded = Buffer.from(subscription, 'utf8').toString('base64');
  fs.writeFileSync(subPath, encoded);
  log(`Subscription saved: ${subPath}`);
  console.log(encoded);
  return subscription;
}

// -----------------------------------------------------------------------------
// Express and WebSocket reverse proxy
// -----------------------------------------------------------------------------
const app = express();

app.get('/', async (_req, res) => {
  const indexPath = path.join(__dirname, 'index.html');
  try {
    const html = await fs.promises.readFile(indexPath, 'utf8');
    res.type('html').send(html);
  } catch (_) {
    res.type('text').send(`Service is running. Subscription: /${SUB_PATH}`);
  }
});

app.get(`/${SUB_PATH}`, async (_req, res) => {
  try {
    const data = await fs.promises.readFile(subPath, 'utf8');
    res.set('Content-Type', 'text/plain; charset=utf-8');
    res.set('Cache-Control', 'no-store');
    res.send(data);
  } catch (_) {
    res.status(503).type('text').send('Subscription is not ready yet.');
  }
});

const proxy = createProxyMiddleware({
  target: `http://127.0.0.1:${ARGO_PORT}`,
  ws: true,
  changeOrigin: true,
  on: {
    error(error, req, res) {
      log(`Proxy error: ${error.code || ''} ${error.message}`);
      if (res && !res.headersSent) {
        res.writeHead(502, { 'Content-Type': 'text/plain; charset=utf-8' });
        res.end('Proxy target is not ready.');
      }
    }
  }
});

app.use(proxy);

const server = app.listen(PORT, '0.0.0.0', () => {
  log(`HTTP server listening on 0.0.0.0:${PORT}`);
});

// Explicitly attach WebSocket upgrade handling to the HTTP server.
server.on('upgrade', proxy.upgrade);

// -----------------------------------------------------------------------------
// Startup and graceful shutdown
// -----------------------------------------------------------------------------
async function main() {
  assertConfig();
  log(`Runtime directory: ${FILE_PATH}`);
  log(`Architecture: ${getArchitecture()}`);

  await downloadAll();
  generateConfig();

  // Start optional monitoring agent, then Xray. Cloudflared starts only after
  // Xray's inbound port has passed the TCP readiness check.
  await startNezha();
  await startXray();

  const domain = ARGO_DOMAIN && ARGO_AUTH
    ? ARGO_DOMAIN
    : await startCloudflared();

  await generateLinks(domain);
  log('All startup tasks completed.');
}

function shutdown(signal) {
  if (shuttingDown) return;
  shuttingDown = true;
  log(`Received ${signal}; stopping child processes...`);

  for (const pid of childProcesses) {
    try {
      process.kill(-pid, 'SIGTERM');
    } catch (_) {
      try { process.kill(pid, 'SIGTERM'); } catch (_) {}
    }
  }

  server.close(() => process.exit(0));
  setTimeout(() => process.exit(0), 5000).unref();
}

process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT', () => shutdown('SIGINT'));

main().catch(error => {
  log('Startup failed:', error.stack || error.message);
  log(`Check logs in ${FILE_PATH}`);
  // Keep the HTTP process alive so the platform can expose startup diagnostics
  // and the subscription endpoint can return a clear 503 instead of crashing.
});

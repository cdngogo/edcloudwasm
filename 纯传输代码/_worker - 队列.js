import {connect} from 'cloudflare:sockets';
const bufferSize = 512 * 1024;
const startThreshold = 50 * 1024 * 1024;
const maxChunkLen = 64 * 1024;
const flushTime = 20;
const proxyIpAddrs = {EU: 'ProxyIP.DE.CMLiussss.net', AS: 'ProxyIP.SG.CMLiussss.net', JP: 'ProxyIP.JP.CMLiussss.net', US: 'ProxyIP.US.CMLiussss.net'};
const coloRegions = {JP: new Set(['ICN', 'KIX', 'NRT']), EU: new Set(['FRA', 'HAM', 'MRS', 'CDG', 'LHR']), AS: new Set(['HKG', 'SIN', 'TPE'])};
const coloToProxyMap = new Map();
for (const [region, colos] of Object.entries(coloRegions)) {for (const colo of colos) coloToProxyMap.set(colo, proxyIpAddrs[region])}
const textDecoder = new TextDecoder();
const createConnect = (hostname, port, socket = connect({hostname, port})) => socket.opened.then(() => socket);
const concurrentConnect = (hostname, port) => Promise.any(Array(4).fill().map(() => createConnect(hostname, port)));
const manualPipe = async (readable, writable) => {
    const safeBufferSize = bufferSize - maxChunkLen;
    let buffer = new Uint8Array(bufferSize), chunkBuf = new ArrayBuffer(maxChunkLen);
    let offset = 0, totalBytes = 0, timerId = null, resume = null;
    let avgChunkLen = 4096, stats = [], statBytes = 0;
    const flushBuffer = () => {
        offset > 0 && (writable.send(buffer.slice(0, offset)), offset = 0);
        timerId && (clearTimeout(timerId), timerId = null);
        resume?.(), resume = null;
    };
    const reader = readable.getReader({mode: 'byob'});
    try {
        while (true) {
            const {done, value} = await reader.read(new Uint8Array(chunkBuf));
            if (done) break;
            chunkBuf = value.buffer;
            const chunkLen = value.byteLength;
            const now = Date.now();
            stats.push([now, chunkLen]);
            statBytes += chunkLen;
            while (stats.length && now - stats[0][0] > 1000) {
                statBytes -= stats.shift()[1];
            }
            avgChunkLen = statBytes / stats.length || 4096;
            if (chunkLen < 512) {
                flushBuffer();
                writable.send(value.slice());
            } else {
                chunkLen < avgChunkLen * 0.9 && chunkLen < 24576 && (totalBytes = 0);
                buffer.set(value, offset);
                offset += chunkLen;
                totalBytes += chunkLen;
                timerId ||= setTimeout(flushBuffer, flushTime);
                if (totalBytes < startThreshold) {
                    offset > safeBufferSize && flushBuffer();
                } else {
                    offset > safeBufferSize && (await new Promise(r => resume = r));
                }
            }
        }
    } finally {flushBuffer(), reader.releaseLock()}
};
const handleWebSocketConn = async (webSocket, request) => {
    const protocolHeader = request.headers.get('sec-websocket-protocol');
    // @ts-ignore
    const earlyData = protocolHeader ? Uint8Array.fromBase64(protocolHeader, {alphabet: 'base64url'}) : null;
    let tcpWrite, processingChain = Promise.resolve(), tcpSocket;
    const closeSocket = () => {if (!earlyData) {tcpSocket?.close(), webSocket?.close()}};
    const processMessage = async (chunk) => {
        try {
            if (tcpWrite) return tcpWrite(chunk);
            chunk = earlyData ? chunk : new Uint8Array(chunk);
            webSocket.send(new Uint8Array([chunk[0], 0]));
            let offset = 19 + chunk[17];
            const port = (chunk[offset] << 8) | chunk[offset + 1];
            offset += 2;
            const addrType = chunk[offset++];
            let newOffset, hostname;
            if (addrType === 2) {
                const len = chunk[offset++];
                newOffset = offset + len;
                hostname = textDecoder.decode(chunk.subarray(offset, newOffset));
            } else if (addrType === 1) {
                newOffset = offset + 4;
                const bytes = chunk.subarray(offset, newOffset);
                hostname = `${bytes[0]}.${bytes[1]}.${bytes[2]}.${bytes[3]}`;
            } else {
                newOffset = offset + 16;
                let ipv6Str = ((chunk[offset] << 8) | chunk[offset + 1]).toString(16);
                for (let i = 1; i < 8; i++) ipv6Str += ':' + ((chunk[offset + i * 2] << 8) | chunk[offset + i * 2 + 1]).toString(16);
                hostname = `[${ipv6Str}]`;
            }
            tcpSocket = await concurrentConnect(hostname, port).catch(() => {
                const url = new URL(request.url);
                const proxyHost = url.searchParams.get('proxyip') ?? coloToProxyMap.get(request.cf?.colo) ?? proxyIpAddrs.US;
                return concurrentConnect(proxyHost, 443);
            });
            const tcpWriter = tcpSocket.writable.getWriter();
            const payload = chunk.subarray(newOffset);
            if (payload.byteLength) tcpWriter.write(payload);
            tcpWrite = (chunk) => tcpWriter.write(chunk);
            manualPipe(tcpSocket.readable, webSocket);
        } catch {closeSocket()}
    };
    if (earlyData) processingChain = processingChain.then(() => processMessage(earlyData));
    webSocket.addEventListener("message", event => processingChain = processingChain.then(() => processMessage(event.data)));
};
export default {
    async fetch(request) {
        if (request.headers.get('Upgrade') === 'websocket') {
            const {0: clientSocket, 1: webSocket} = new WebSocketPair();
            // @ts-ignore
            webSocket.accept({allowHalfOpen: true}), webSocket.binaryType = "arraybuffer";
            handleWebSocketConn(webSocket, request);
            return new Response(null, {status: 101, webSocket: clientSocket});
        } else {return new Response(null, {status: 400})}
    }
};
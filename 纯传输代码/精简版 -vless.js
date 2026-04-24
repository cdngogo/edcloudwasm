import {connect} from 'cloudflare:sockets';
const uuid = 'd342d11e-d424-4583-b36e-524ab1f0afa4';
const bufferSize = 512 * 1024;
const startThreshold = 50 * 1024 * 1024;
const maxChunkLen = 64 * 1024;
const flushTime = 20;
const proxyIpAddrs = {EU: 'ProxyIP.DE.CMLiussss.net', AS: 'ProxyIP.SG.CMLiussss.net', JP: 'ProxyIP.JP.CMLiussss.net', US: 'ProxyIP.US.CMLiussss.net'};//分区域proxyip
const coloRegions = {
    JP: new Set(['FUK', 'ICN', 'KIX', 'NRT', 'OKA']),
    EU: new Set([
        'ACC', 'ADB', 'ALA', 'ALG', 'AMM', 'AMS', 'ARN', 'ATH', 'BAH', 'BCN', 'BEG', 'BGW', 'BOD', 'BRU', 'BTS', 'BUD', 'CAI',
        'CDG', 'CPH', 'CPT', 'DAR', 'DKR', 'DMM', 'DOH', 'DUB', 'DUR', 'DUS', 'DXB', 'EBB', 'EDI', 'EVN', 'FCO', 'FRA', 'GOT',
        'GVA', 'HAM', 'HEL', 'HRE', 'IST', 'JED', 'JIB', 'JNB', 'KBP', 'KEF', 'KWI', 'LAD', 'LED', 'LHR', 'LIS', 'LOS', 'LUX',
        'LYS', 'MAD', 'MAN', 'MCT', 'MPM', 'MRS', 'MUC', 'MXP', 'NBO', 'OSL', 'OTP', 'PMO', 'PRG', 'RIX', 'RUH', 'RUN', 'SKG',
        'SOF', 'STR', 'TBS', 'TLL', 'TLV', 'TUN', 'VIE', 'VNO', 'WAW', 'ZAG', 'ZRH']),
    AS: new Set([
        'ADL', 'AKL', 'AMD', 'BKK', 'BLR', 'BNE', 'BOM', 'CBR', 'CCU', 'CEB', 'CGK', 'CMB', 'COK', 'DAC', 'DEL', 'HAN', 'HKG',
        'HYD', 'ISB', 'JHB', 'JOG', 'KCH', 'KHH', 'KHI', 'KTM', 'KUL', 'LHE', 'MAA', 'MEL', 'MFM', 'MLE', 'MNL', 'NAG', 'NOU',
        'PAT', 'PBH', 'PER', 'PNH', 'SGN', 'SIN', 'SYD', 'TPE', 'ULN', 'VTE'])
};
const coloToProxyMap = new Map();
for (const [region, colos] of Object.entries(coloRegions)) {for (const colo of colos) coloToProxyMap.set(colo, proxyIpAddrs[region])}
const uuidBytes = new Uint8Array(16), offsets = [0, 0, 0, 0, 1, 1, 2, 2, 3, 3, 4, 4, 4, 4, 4, 4];
for (let i = 0, c; i < 16; i++) uuidBytes[i] = (((c = uuid.charCodeAt(i * 2 + offsets[i])) > 64 ? c + 9 : c) & 0xF) << 4 | (((c = uuid.charCodeAt(i * 2 + offsets[i] + 1)) > 64 ? c + 9 : c) & 0xF);
const textDecoder = new TextDecoder();
const createConnect = (hostname, port, socket = connect({hostname, port})) => socket.opened.then(() => socket);
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
            for (let i = 0; i < 16; i++) if (chunk[i + 1] !== uuidBytes[i]) return null;
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
            tcpSocket = await createConnect(hostname, port).catch(() => {
                const url = new URL(request.url);
                const proxyHost = url.searchParams.get('proxyip') ?? coloToProxyMap.get(request.cf?.colo) ?? proxyIpAddrs.US;
                return createConnect(proxyHost, 443);
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
import asyncio
import json
import os
import mimetypes
import hashlib
import base64
import struct

PORT = int(os.environ.get("PORT", 8080))
CLIENTS = {}  # websocket_writer -> player_data

WS_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

def create_ws_accept(key):
    h = hashlib.sha1((key.strip() + WS_GUID).encode('utf-8')).digest()
    return base64.b64encode(h).decode('utf-8')

def encode_ws_frame(data_str):
    payload = data_str.encode('utf-8')
    length = len(payload)
    if length <= 125:
        header = struct.pack('!BB', 0x81, length)
    elif length <= 65535:
        header = struct.pack('!BBH', 0x81, 126, length)
    else:
        header = struct.pack('!BBQ', 0x81, 127, length)
    return header + payload

async def broadcast(message, exclude=None):
    frame = encode_ws_frame(message)
    dead_clients = []
    for writer in list(CLIENTS.keys()):
        if writer is not exclude:
            try:
                writer.write(frame)
                await writer.drain()
            except Exception:
                dead_clients.append(writer)
    for dc in dead_clients:
        if dc in CLIENTS:
            del CLIENTS[dc]

async def handle_client(reader, writer):
    try:
        # Read HTTP Request
        request_line = await reader.readline()
        if not request_line:
            writer.close()
            return
        
        req_str = request_line.decode('utf-8', errors='ignore')
        parts = req_str.strip().split()
        if len(parts) < 2:
            writer.close()
            return
        
        method, path = parts[0], parts[1]
        
        headers = {}
        while True:
            line = await reader.readline()
            if not line or line == b'\r\n':
                break
            line_str = line.decode('utf-8', errors='ignore').strip()
            if ':' in line_str:
                k, v = line_str.split(':', 1)
                headers[k.strip().lower()] = v.strip()
        
        # Check if WebSocket Upgrade
        if headers.get('upgrade', '').lower() == 'websocket':
            ws_key = headers.get('sec-websocket-key')
            if not ws_key:
                writer.close()
                return
            
            accept_key = create_ws_accept(ws_key)
            response = (
                "HTTP/1.1 101 Switching Protocols\r\n"
                "Upgrade: websocket\r\n"
                "Connection: Upgrade\r\n"
                f"Sec-WebSocket-Accept: {accept_key}\r\n"
                "\r\n"
            )
            writer.write(response.encode('utf-8'))
            await writer.drain()
            
            player_id = f"turtle_{id(writer) % 10000}"
            CLIENTS[writer] = {
                "id": player_id,
                "x": 300 + (len(CLIENTS) * 40) % 300,
                "y": 300,
                "name": f"Kaplumbağa #{len(CLIENTS)+1}",
                "money": 0
            }
            
            # Send current player info & existing players
            init_msg = json.dumps({
                "type": "init",
                "id": player_id,
                "players": [p for p in CLIENTS.values()]
            })
            writer.write(encode_ws_frame(init_msg))
            await writer.drain()
            
            # Broadcast new player
            await broadcast(json.dumps({
                "type": "player_join",
                "player": CLIENTS[writer]
            }), exclude=writer)
            
            # WebSocket message loop
            while True:
                head = await reader.read(2)
                if len(head) < 2:
                    break
                b1, b2 = head[0], head[1]
                opcode = b1 & 0x0F
                if opcode == 0x8:  # Close frame
                    break
                
                masked = (b2 & 0x80) != 0
                payload_len = b2 & 0x7F
                
                if payload_len == 126:
                    ext = await reader.read(2)
                    payload_len = struct.unpack('!H', ext)[0]
                elif payload_len == 127:
                    ext = await reader.read(8)
                    payload_len = struct.unpack('!Q', ext)[0]
                    
                mask = await reader.read(4) if masked else None
                payload = await reader.read(payload_len)
                
                if masked:
                    payload = bytes(b ^ mask[i % 4] for i, b in enumerate(payload))
                
                try:
                    data = json.loads(payload.decode('utf-8'))
                    msg_type = data.get("type")
                    if msg_type == "move":
                        CLIENTS[writer]["x"] = data.get("x", CLIENTS[writer]["x"])
                        CLIENTS[writer]["y"] = data.get("y", CLIENTS[writer]["y"])
                        CLIENTS[writer]["dir"] = data.get("dir", "down")
                        await broadcast(json.dumps({
                            "type": "player_move",
                            "id": player_id,
                            "x": CLIENTS[writer]["x"],
                            "y": CLIENTS[writer]["y"],
                            "dir": CLIENTS[writer]["dir"]
                        }), exclude=writer)
                    elif msg_type == "stats_update":
                        CLIENTS[writer]["money"] = data.get("money", 0)
                        CLIENTS[writer]["level"] = data.get("level", 1)
                        await broadcast(json.dumps({
                            "type": "player_stats",
                            "id": player_id,
                            "money": CLIENTS[writer]["money"],
                            "level": CLIENTS[writer]["level"]
                        }), exclude=writer)
                except Exception:
                    pass
        else:
            # Static HTTP File Serving
            clean_path = path.split('?')[0]
            if clean_path in ('/', ''):
                clean_path = '/index.html'
            
            cur_dir = os.path.dirname(os.path.abspath(__file__))
            file_path = os.path.join(cur_dir, clean_path.lstrip('/'))
            
            if os.path.exists(file_path) and not os.path.isdir(file_path):
                content_type, _ = mimetypes.guess_type(file_path)
                content_type = content_type or 'application/octet-stream'
                with open(file_path, 'rb') as f:
                    content = f.read()
                
                res = (
                    f"HTTP/1.1 200 OK\r\n"
                    f"Content-Type: {content_type}\r\n"
                    f"Content-Length: {len(content)}\r\n"
                    f"Access-Control-Allow-Origin: *\r\n"
                    f"\r\n"
                ).encode('utf-8') + content
                writer.write(res)
                await writer.drain()
            else:
                res = b"HTTP/1.1 404 Not Found\r\nContent-Length: 9\r\n\r\nNot Found"
                writer.write(res)
                await writer.drain()
                
    except Exception as e:
        pass
    finally:
        if writer in CLIENTS:
            pid = CLIENTS[writer]["id"]
            del CLIENTS[writer]
            await broadcast(json.dumps({
                "type": "player_leave",
                "id": pid
            }))
        writer.close()

async def main():
    server = await asyncio.start_server(handle_client, '0.0.0.0', PORT)
    print(f"🐢 Kaplumbağa Maden Sunucusu Başlatıldı: http://localhost:{PORT}")
    async with server:
        await server.serve_forever()

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Sunucu durduruldu.")
